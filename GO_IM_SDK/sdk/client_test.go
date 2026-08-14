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
	return Config{MsyncHost: "wss://msync.example.com", RestBase: "https://rest.example.com/org/app", AppKey: "org#app", UserID: "user", Token: "secret", Resource: "go-service-01", MessageHandler: func(context.Context, *Message) error { return nil }}
}

func TestConfigDefaultsAndSecurity(t *testing.T) {
	c, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.MsyncHost != "wss://msync.example.com/websocket" {
		t.Fatalf("path default=%s", c.cfg.MsyncHost)
	}
	if c.cfg.MaxFrameBytes != 4<<20 || c.cfg.HeartbeatInterval != 120*time.Second {
		t.Fatal("defaults not applied")
	}
	bad := validConfig()
	bad.MsyncHost = "ws://insecure"
	if _, err = New(bad); err == nil {
		t.Fatal("ws must be rejected")
	}
	bad = validConfig()
	bad.RestBase = "http://insecure"
	if _, err = New(bad); err == nil {
		t.Fatal("http must be rejected")
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
	c, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	warned := make(chan time.Time, 1)
	c.OnTokenWillExpire(func(at time.Time) { warned <- at })
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
