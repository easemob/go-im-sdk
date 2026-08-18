//go:build linux || nativecodecdev

package sdk

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/gorilla/websocket"
)

type lifecycleCodec struct {
	mu         sync.Mutex
	provisions []internalprotocol.ProvisionRequest
}

func (c *lifecycleCodec) EncodeProvision(req internalprotocol.ProvisionRequest) ([]byte, error) {
	c.mu.Lock()
	c.provisions = append(c.provisions, req)
	c.mu.Unlock()
	return []byte("provision"), nil
}
func (*lifecycleCodec) EncodeUnread() ([]byte, error) { return []byte("unread"), nil }
func (*lifecycleCodec) EncodeSync(internalprotocol.SyncRequest) ([]byte, error) {
	return []byte("sync"), nil
}
func (*lifecycleCodec) EncodeLogout(internalprotocol.LogoutRequest) ([]byte, error) {
	return []byte("logout"), nil
}
func (*lifecycleCodec) DecodeFrame(data []byte) (*internalprotocol.Frame, error) {
	switch string(data) {
	case "provision-ok":
		return &internalprotocol.Frame{Command: internalprotocol.CommandProvision, Provision: &internalprotocol.Provision{
			Status: &internalprotocol.Status{Code: internalprotocol.StatusOK}, SessionID: "session",
		}}, nil
	case "logout-ok":
		return &internalprotocol.Frame{Command: internalprotocol.CommandLogout, Logout: &internalprotocol.Logout{
			Status: &internalprotocol.Status{Code: internalprotocol.StatusOK},
		}}, nil
	default:
		return nil, fmt.Errorf("unexpected frame %q", data)
	}
}
func (*lifecycleCodec) EncodeMessageBody(internalprotocol.MessageBody) ([]byte, error) {
	return []byte("message"), nil
}
func (*lifecycleCodec) DecodeMessageBody([]byte) (*internalprotocol.MessageBody, error) {
	return nil, nil
}
func (*lifecycleCodec) DecodeStatistic([]byte) (*internalprotocol.Statistic, error) { return nil, nil }

func TestLogoutAllowsLoginAgainAndReusesFreshDNSCache(t *testing.T) {
	withTestSharedDNSResolver(t, time.Now)
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		_, frame, err := conn.ReadMessage()
		if err != nil || string(frame) != "provision" {
			t.Errorf("provision frame=%q err=%v", frame, err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte("provision-ok")); err != nil {
			t.Errorf("write provision: %v", err)
			return
		}
		_, frame, err = conn.ReadMessage()
		if err != nil || string(frame) != "unread" {
			t.Errorf("unread frame=%q err=%v", frame, err)
			return
		}
		_, frame, err = conn.ReadMessage()
		if err != nil || string(frame) != "logout" {
			t.Errorf("logout frame=%q err=%v", frame, err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte("logout-ok")); err != nil {
			t.Errorf("write logout: %v", err)
		}
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, rawPort, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatal(err)
	}

	var dnsRequests atomic.Int32
	config := validConfig()
	config.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		dnsRequests.Add(1)
		body := fmt.Sprintf(`{"msync-wx":{"hosts":[{"protocol":"https","port":%d,"domain":%q,"priority":1}]},"rest":{"hosts":[{"protocol":"https","port":443,"domain":"rest.example","priority":1}]}}`, port, host)
		return dnsResponse(http.StatusOK, body), nil
	})}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if closer, ok := client.codec.(interface{ Close() }); ok {
		closer.Close()
	}
	codec := &lifecycleCodec{}
	client.codec = codec
	client.wsDialer.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()

	for _, user := range []string{"first-user", "second-user"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Login(ctx, user, "token")
		cancel()
		if err != nil {
			t.Fatalf("Login(%s): %v", user, err)
		}
		if !client.Connected() || client.LoginState() != LoginStateLoggedIn {
			t.Fatalf("connected=%v state=%s", client.Connected(), client.LoginState())
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Logout(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Logout(%s): %v", user, err)
		}
		if client.Connected() || client.LoginState() != LoginStateLogout || client.closed {
			t.Fatalf("after logout connected=%v state=%s closed=%v", client.Connected(), client.LoginState(), client.closed)
		}
	}
	if dnsRequests.Load() != 1 {
		t.Fatalf("DNS requests=%d, want one fresh-cache fill", dnsRequests.Load())
	}
	codec.mu.Lock()
	defer codec.mu.Unlock()
	if len(codec.provisions) != 2 || codec.provisions[0].User.Name != "first-user" ||
		codec.provisions[1].User.Name != "second-user" || codec.provisions[0].SDKVersion != sdkVersion ||
		codec.provisions[0].Resource != resourcePrefix+"service-01" ||
		codec.provisions[0].User.ClientResource != resourcePrefix+"service-01" {
		t.Fatalf("provisions=%#v", codec.provisions)
	}
}

func TestLoginDNSFailureRestoresLoggedOutReusableState(t *testing.T) {
	config := validConfig()
	config.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return dnsResponse(http.StatusOK, `{`), nil
	})}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	err = client.Login(context.Background(), "user", "token")
	if errorCode(err) != ErrDNS || client.LoginState() != LoginStateLogout || client.Connected() {
		t.Fatalf("error=%v state=%s connected=%v", err, client.LoginState(), client.Connected())
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	if client.closed || client.userID != "" || client.token != "" || client.msyncHost != "" || client.restBase != "" {
		t.Fatalf("failed login retained session state: user=%q token=%t wss=%q rest=%q closed=%v",
			client.userID, client.token != "", client.msyncHost, client.restBase, client.closed)
	}
}
