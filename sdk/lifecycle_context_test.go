package sdk

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/gorilla/websocket"
)

type lifecycleContextCodec struct {
	closeCount atomic.Int32
	closed     chan struct{}
	closeOnce  sync.Once
}

func (*lifecycleContextCodec) EncodeProvision(internalprotocol.ProvisionRequest) ([]byte, error) {
	return []byte("provision"), nil
}
func (*lifecycleContextCodec) EncodeUnread() ([]byte, error) { return []byte("unread"), nil }
func (*lifecycleContextCodec) EncodeSync(internalprotocol.SyncRequest) ([]byte, error) {
	return []byte("sync"), nil
}
func (*lifecycleContextCodec) EncodeLogout(internalprotocol.LogoutRequest) ([]byte, error) {
	return []byte("logout"), nil
}
func (*lifecycleContextCodec) DecodeFrame([]byte) (*internalprotocol.Frame, error) {
	return nil, errors.New("unexpected decode")
}
func (*lifecycleContextCodec) EncodeMessageBody(internalprotocol.MessageBody) ([]byte, error) {
	return nil, errors.New("unexpected message encode")
}
func (*lifecycleContextCodec) DecodeMessageBody([]byte) (*internalprotocol.MessageBody, error) {
	return nil, errors.New("unexpected message decode")
}
func (*lifecycleContextCodec) DecodeStatistic([]byte) (*internalprotocol.Statistic, error) {
	return nil, errors.New("unexpected statistic decode")
}
func (c *lifecycleContextCodec) Close() {
	c.closeCount.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
}

type lifecycleContextRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn lifecycleContextRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func newLifecycleContextClient(transport http.RoundTripper) (*Client, *lifecycleContextCodec) {
	eventCtx, eventCancel := context.WithCancel(context.Background())
	codec := &lifecycleContextCodec{closed: make(chan struct{})}
	return &Client{
		cfg: Config{
			AppKey:     "org#app",
			HTTPClient: &http.Client{Transport: transport},
		},
		logger:      defaultLogger(),
		state:       LoginStateLogout,
		connState:   ConnStateDisconnected,
		eventCtx:    eventCtx,
		eventCancel: eventCancel,
		events:      make(chan func(), writeQueueSize),
		batches:     make(chan batchJob, writeQueueSize),
		codec:       codec,
		closeDone:   make(chan struct{}),
		wsDialer: &websocket.Dialer{
			HandshakeTimeout: connectTimeout,
			Proxy:            http.ProxyFromEnvironment,
		},
	}, codec
}

func TestCloseDeadlineDoesNotDestroyCodecBeforeBlockedLoginExits(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	client, codec := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		startedOnce.Do(func() { close(started) })
		// Deliberately ignore Request.Context to model a supported but broken
		// custom transport that Go cannot forcibly terminate.
		<-release
		return nil, errors.New("transport released")
	}))

	loginResult := make(chan error, 1)
	go func() { loginResult <- client.Login(context.Background(), "blocked-user", "token") }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Login did not enter blocking RoundTripper")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := client.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close error=%v, want context deadline", err)
	}
	client.mu.RLock()
	closed := client.closed
	state := client.state
	connState := client.connState
	client.mu.RUnlock()
	if !closed || state != LoginStateLogout || connState != ConnStateDisconnected {
		t.Fatalf("Close did not detach immediately: closed=%v state=%s conn=%s", closed, state, connState)
	}
	if got := codec.closeCount.Load(); got != 0 {
		t.Fatalf("codec closed before lifecycle operation exited: count=%d", got)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = client.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Close error=%v, want context deadline", err)
	}
	if got := codec.closeCount.Load(); got != 0 {
		t.Fatalf("codec closed while second Close was waiting: count=%d", got)
	}

	close(release)
	select {
	case <-loginResult:
	case <-time.After(time.Second):
		t.Fatal("Login did not exit after transport release")
	}
	select {
	case <-codec.closed:
	case <-time.After(time.Second):
		t.Fatal("asynchronous Close finalizer did not close codec")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close after finalization: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close after finalization: %v", err)
	}
	if got := codec.closeCount.Load(); got != 1 {
		t.Fatalf("codec Close count=%d, want exactly one", got)
	}
}

func TestLogoutDeadlineWhileLoginBlockedPreservesLoginState(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	client, codec := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		startedOnce.Do(func() { close(started) })
		<-release
		return nil, errors.New("transport released")
	}))

	loginResult := make(chan error, 1)
	go func() { loginResult <- client.Login(context.Background(), "blocked-user", "token") }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Login did not enter blocking RoundTripper")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := client.Logout(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Logout error=%v, want context deadline", err)
	}
	client.mu.RLock()
	state := client.state
	connState := client.connState
	userID := client.userID
	token := client.token
	closed := client.closed
	client.mu.RUnlock()
	if state != LoginStateLoggingIn || connState != ConnStateConnecting || userID != "blocked-user" || token != "token" || closed {
		t.Fatalf("timed-out Logout changed state: state=%s conn=%s user=%q token=%q closed=%v", state, connState, userID, token, closed)
	}

	close(release)
	select {
	case <-loginResult:
	case <-time.After(time.Second):
		t.Fatal("Login did not exit after transport release")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := codec.closeCount.Load(); got != 1 {
		t.Fatalf("codec Close count=%d, want exactly one", got)
	}
}

func TestCloseFromEventCallbackPortableDoesNotDeadlock(t *testing.T) {
	client, codec := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	done := make(chan struct{})
	client.callbacks.connection = func(string, ConnState) {
		_ = client.Close(context.Background())
		close(done)
	}
	client.eventWG.Add(1)
	go client.dispatchEvents()
	client.setStates(LoginStateLoggedIn, ConnStateConnected)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked in callback dispatcher")
	}
	if got := codec.closeCount.Load(); got != 1 {
		t.Fatalf("codec Close count=%d, want exactly one", got)
	}
}
