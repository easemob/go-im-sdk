//go:build linux || nativecodecdev

package sdk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

func validConfig() Config {
	return Config{AppKey: "org#app", Resource: "service-01", MessageHandler: func(context.Context, *Message) error { return nil }}
}

func TestConfigDefaultsAndSecurity(t *testing.T) {
	c, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if c.cfg.Resource != "go-server-imsdk-service-01" {
		t.Fatalf("resource=%s", c.cfg.Resource)
	}
	if c.cfg.MaxFrameBytes != 4<<20 || c.cfg.HeartbeatInterval != 120*time.Second {
		t.Fatal("defaults not applied")
	}
	bad := validConfig()
	bad.AppKey = "invalid"
	if _, err = New(bad); err == nil {
		t.Fatal("invalid AppKey must be rejected")
	}
}

func TestResourceIsRequiredAndValidated(t *testing.T) {
	missing := validConfig()
	missing.Resource = ""
	if _, err := New(missing); err == nil || !strings.Contains(err.Error(), "Resource") {
		t.Fatalf("missing resource error = %v", err)
	}
	bad := validConfig()
	bad.Resource = "shared resource"
	if _, err := New(bad); err == nil || !strings.Contains(err.Error(), "Resource") {
		t.Fatalf("invalid resource error = %v", err)
	}
	tooLong := validConfig()
	tooLong.Resource = strings.Repeat("x", maxResourceLength-len(resourcePrefix)+1)
	if _, err := New(tooLong); err == nil || !strings.Contains(err.Error(), "prefix") {
		t.Fatalf("too-long resource error = %v", err)
	}
}

func TestListenersAreBoundDuringInitialization(t *testing.T) {
	config := validConfig()
	config.OnConnectionStateChanged = func(ConnState) {}
	config.OnDisconnect = func(error) {}
	config.OnTokenExpired = func() {}
	config.OnTokenWillExpire = func(time.Time) {}
	config.OnTokenRotated = func(string, int64) {}
	config.OnUserForbidden = func() {}
	config.OnUserRemoved = func() {}
	config.OnUserKickedByOtherDevice = func(string, string) {}
	config.OnUserLoginAnotherDevice = func(string, string) {}
	config.OnServerNotice = func(string, []byte) {}
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	callbacks := c.callbackSnapshot()
	if callbacks.connection == nil || callbacks.disconnect == nil || callbacks.tokenExpired == nil ||
		callbacks.tokenWillExpire == nil || callbacks.tokenRotated == nil || callbacks.forbidden == nil ||
		callbacks.removed == nil || callbacks.kicked == nil || callbacks.otherLogin == nil || callbacks.notice == nil {
		t.Fatalf("callbacks were not fully bound: %#v", callbacks)
	}
}

func TestLoginRejectsAlreadyActiveClient(t *testing.T) {
	c := &Client{state: LoginStateLoggedIn}
	if err := c.Login(context.Background(), "user", "token"); errorCode(err) != ErrAlreadyLoggedIn {
		t.Fatalf("error = %v", err)
	}
}

func TestSendBeforeLoginReturnsLoginStateError(t *testing.T) {
	c := &Client{state: LoginStateLogout}
	if _, err := c.Send(context.Background(), SendRequest{}); errorCode(err) != ErrNotLoggedIn {
		t.Fatalf("error = %v", err)
	}
}

func TestLogoutWithoutSessionKeepsClientReusable(t *testing.T) {
	c, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.closed || c.LoginState() != LoginStateLogout {
		t.Fatalf("closed=%v state=%s", c.closed, c.LoginState())
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStaleReconnectAndTerminalEventsCannotReplaceLoggedOutState(t *testing.T) {
	old := &connectionRun{}
	c := &Client{state: LoginStateLogout, connState: ConnStateDisconnected}
	c.reconnect(old)
	c.recordTerminalForRun(old, newError(ErrIO, "old connection", ""))
	if c.state != LoginStateLogout || c.connState != ConnStateDisconnected || c.lastErr != nil {
		t.Fatalf("state=%s connection=%s last_error=%v", c.state, c.connState, c.lastErr)
	}
}

func TestProvisionReasonMapping(t *testing.T) {
	cases := []struct {
		status int32
		reason string
		want   ErrorCode
	}{{1, "Sorry, token expired", ErrTokenExpired}, {1, "Sorry, token or password does not match login info", ErrInvalidToken}, {1, "Sorry, user not found", ErrUserNotFound}, {7, "Sorry, the app online count limit", ErrAppActiveLimit}, {12, "", ErrUserForbidden}, {20, "", ErrResourceChanged}}
	for _, tc := range cases {
		if got := errorCode(protocolError("login", tc.status, tc.reason)); got != tc.want {
			t.Fatalf("status %d got %s want %s", tc.status, got, tc.want)
		}
	}
}

func TestRedirectValidationAndLoop(t *testing.T) {
	u, err := redirectURL("wss://old.example/websocket", []internalprotocol.RedirectInfo{{Host: "new.example", Port: 443}})
	if err != nil || u != "wss://new.example:443/websocket" {
		t.Fatalf("redirect=%q err=%v", u, err)
	}
	if _, err = redirectURL("wss://old.example/websocket", nil); errorCode(err) != ErrProtocol {
		t.Fatalf("empty redirect: %v", err)
	}
}
func TestNetworkErrorPreservesSDKError(t *testing.T) {
	want := newError(ErrTimeout, "heartbeat", "pong timeout")
	if got := classifyNetworkError("connection", want); !errors.Is(got, want) {
		t.Fatal("SDK error was wrapped/reclassified")
	}
}

func TestTokenExpiryTimeFormats(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if got := tokenExpiryTime(1_800_000_000, now); !got.Equal(time.Unix(1_800_000_000, 0)) {
		t.Fatalf("seconds: %v", got)
	}
	if got := tokenExpiryTime(1_800_000_000_123, now); !got.Equal(time.UnixMilli(1_800_000_000_123)) {
		t.Fatalf("milliseconds: %v", got)
	}
	if got := tokenExpiryTime(3600, now); !got.Equal(now.Add(time.Hour)) {
		t.Fatalf("relative: %v", got)
	}
}

func TestProvisionTokenExpiryIsRecordedAndWarned(t *testing.T) {
	config := validConfig()
	warned := make(chan time.Time, 1)
	config.OnTokenWillExpire = func(at time.Time) { warned <- at }
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	expires := time.Now().Add(100 * time.Millisecond).UnixMilli()
	payload := []byte(`{"token":"rotated","expires_in":` + fmt.Sprint(expires) + `}`)
	c.acceptProvision(&internalprotocol.Provision{AuthToken: payload})
	got, ok := c.TokenExpiresAt()
	if !ok || got.UnixMilli() != expires {
		t.Fatalf("expiry=%v ok=%v", got, ok)
	}
	select {
	case at := <-warned:
		if at.UnixMilli() != expires {
			t.Fatalf("warning=%v", at)
		}
	case <-time.After(time.Second):
		t.Fatal("expiry warning not delivered")
	}
}
