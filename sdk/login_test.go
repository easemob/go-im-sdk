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
	syncQueues []string
}

func (c *lifecycleCodec) EncodeProvision(req internalprotocol.ProvisionRequest) ([]byte, error) {
	c.mu.Lock()
	c.provisions = append(c.provisions, req)
	c.mu.Unlock()
	return []byte("provision"), nil
}
func (*lifecycleCodec) EncodeUnread() ([]byte, error) { return []byte("unread"), nil }
func (c *lifecycleCodec) EncodeSync(req internalprotocol.SyncRequest) ([]byte, error) {
	if req.Queue != nil {
		c.mu.Lock()
		c.syncQueues = append(c.syncQueues, req.Queue.Name)
		c.mu.Unlock()
	}
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
	case "unread-queues":
		return &internalprotocol.Frame{Command: internalprotocol.CommandUnread, Unread: &internalprotocol.Unread{
			Status: &internalprotocol.Status{Code: internalprotocol.StatusOK},
			Queues: []internalprotocol.JID{
				{AppKey: "app", Name: "peer-1", Domain: "easemob.com"},
				{AppKey: "app", Name: "peer-2", Domain: "easemob.com"},
			},
		}}, nil
	case "unread-empty":
		return &internalprotocol.Frame{Command: internalprotocol.CommandUnread, Unread: &internalprotocol.Unread{
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

// newLifecycleClient wires a Client to talk to the given fake WSS server through
// a DNS transport that always resolves to that server, and swaps in the
// string-based lifecycleCodec used by these tests.
func newLifecycleClient(t *testing.T, server *httptest.Server) (*Client, *lifecycleCodec) {
	t.Helper()
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
	config := validConfig()
	config.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := fmt.Sprintf(`{"msync-wx":{"hosts":[{"protocol":"https","port":%d,"domain":%q,"priority":1}]},"rest":{"hosts":[{"protocol":"https","port":443,"domain":"rest.example","priority":1}]}}`, port, host)
		return dnsResponse(http.StatusOK, body), nil
	})}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := client.codec.(interface{ Close() }); ok {
		closer.Close()
	}
	codec := &lifecycleCodec{}
	client.codec = codec
	client.wsDialer.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	return client, codec
}

// TestLoginPullsUnreadOfflineQueues verifies that when the server pushes an
// UNREAD downlink carrying backlog queues, the SDK drives a SYNC pull for each
// queue (mirroring emclient-linux's login-time offline message retrieval).
func TestLoginPullsUnreadOfflineQueues(t *testing.T) {
	withTestSharedDNSResolver(t, time.Now)
	upgrader := websocket.Upgrader{}
	syncFrames := make(chan struct{}, 8)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		if _, frame, err := conn.ReadMessage(); err != nil || string(frame) != "provision" {
			t.Errorf("provision frame=%q err=%v", frame, err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte("provision-ok")); err != nil {
			t.Errorf("write provision: %v", err)
			return
		}
		if _, frame, err := conn.ReadMessage(); err != nil || string(frame) != "unread" {
			t.Errorf("unread frame=%q err=%v", frame, err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte("unread-queues")); err != nil {
			t.Errorf("write unread-queues: %v", err)
			return
		}
		for {
			_, frame, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch string(frame) {
			case "sync":
				syncFrames <- struct{}{}
			case "logout":
				_ = conn.WriteMessage(websocket.BinaryMessage, []byte("logout-ok"))
				return
			}
		}
	}))
	defer server.Close()

	client, codec := newLifecycleClient(t, server)
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Login(ctx, "user", "token"); err != nil {
		t.Fatalf("Login: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-syncFrames:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for offline SYNC pull %d", i+1)
		}
	}

	codec.mu.Lock()
	pulled := append([]string(nil), codec.syncQueues...)
	codec.mu.Unlock()
	seen := map[string]bool{}
	for _, name := range pulled {
		seen[name] = true
	}
	if !seen["peer-1"] || !seen["peer-2"] {
		t.Fatalf("expected SYNC pulls for peer-1 and peer-2, got %v", pulled)
	}

	logoutCtx, logoutCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer logoutCancel()
	if err := client.Logout(logoutCtx); err != nil {
		t.Fatalf("Logout: %v", err)
	}
}

// TestUnreadEmptyQueuesDoesNotPull verifies that a steady-state UNREAD downlink
// with no backlog queues only refreshes keepalive and never triggers a SYNC
// pull, so the heartbeat path stays side-effect free.
func TestUnreadEmptyQueuesDoesNotPull(t *testing.T) {
	withTestSharedDNSResolver(t, time.Now)
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		if _, frame, err := conn.ReadMessage(); err != nil || string(frame) != "provision" {
			t.Errorf("provision frame=%q err=%v", frame, err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte("provision-ok")); err != nil {
			t.Errorf("write provision: %v", err)
			return
		}
		if _, frame, err := conn.ReadMessage(); err != nil || string(frame) != "unread" {
			t.Errorf("unread frame=%q err=%v", frame, err)
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte("unread-empty")); err != nil {
			t.Errorf("write unread-empty: %v", err)
			return
		}
		for {
			_, frame, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch string(frame) {
			case "sync":
				t.Errorf("unexpected SYNC pull for empty unread queues")
			case "logout":
				_ = conn.WriteMessage(websocket.BinaryMessage, []byte("logout-ok"))
				return
			}
		}
	}))
	defer server.Close()

	client, codec := newLifecycleClient(t, server)
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Login(ctx, "user", "token"); err != nil {
		t.Fatalf("Login: %v", err)
	}

	// Allow the empty UNREAD downlink to be processed; assert no pulls resulted.
	time.Sleep(300 * time.Millisecond)
	if !client.Connected() {
		t.Fatalf("client unexpectedly disconnected after empty UNREAD")
	}
	codec.mu.Lock()
	pulled := len(codec.syncQueues)
	codec.mu.Unlock()
	if pulled != 0 {
		t.Fatalf("expected no SYNC pulls for empty unread queues, got %d", pulled)
	}

	logoutCtx, logoutCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer logoutCancel()
	if err := client.Logout(logoutCtx); err != nil {
		t.Fatalf("Logout: %v", err)
	}
}
