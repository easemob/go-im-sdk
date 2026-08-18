package sdk

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestTerminalCallbacksSurviveFullBestEffortQueue(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	staleState := make(chan struct{}, 1)
	client.events <- func() { staleState <- struct{}{} }
	for i := 1; i < cap(client.events); i++ {
		client.events <- func() {}
	}
	called := make(chan string, 3)
	client.callbacks.tokenExpired = func(string) { called <- "token" }
	client.callbacks.disconnect = func(string, error) { called <- "disconnect" }
	client.callbacks.connection = func(string, ConnState) { called <- "state" }
	client.mu.Lock()
	client.userID = "terminal-user"
	client.mu.Unlock()

	client.recordTerminal(newError(ErrTokenExpired, "test terminal", ""))
	go client.dispatchEvents()
	got := make([]string, 0, 3)
	for len(got) < 3 {
		select {
		case name := <-called:
			got = append(got, name)
		case <-time.After(time.Second):
			t.Fatalf("terminal callbacks=%v, want token/disconnect/state", got)
		}
	}
	if want := []string{"token", "disconnect", "state"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("terminal callback order=%v, want %v", got, want)
	}
	select {
	case <-staleState:
		t.Fatal("queued pre-terminal state callback ran after terminal publication")
	default:
	}
	client.lifetimeCancel()
}

func TestTerminalCallbackPanicDoesNotSkipRemainingCallbacks(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	called := make(chan string, 2)
	client.callbacks.tokenExpired = func(string) { panic("token callback panic") }
	client.callbacks.disconnect = func(string, error) { called <- "disconnect" }
	client.callbacks.connection = func(string, ConnState) { called <- "state" }
	client.recordTerminal(newError(ErrTokenExpired, "test terminal", ""))
	go client.dispatchEvents()
	for _, want := range []string{"disconnect", "state"} {
		select {
		case got := <-called:
			if got != want {
				t.Fatalf("callback=%q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("callback %q was skipped after panic", want)
		}
	}
	client.lifetimeCancel()
}

func TestTerminalCallbackPendingRejectsLoginAndClearsBeforeInvocation(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	pendingDuringCallback := make(chan bool, 1)
	client.callbacks.disconnect = func(string, error) {
		pendingDuringCallback <- client.terminalCallbackPending()
	}
	client.recordTerminal(newError(ErrAuthentication, "test terminal", ""))
	if !client.Health().TerminalCallbackPending {
		t.Fatal("Health did not expose pending terminal callback")
	}

	err := client.Login(context.Background(), "next-user", "token")
	if errorCode(err) != ErrCallbackBacklog {
		t.Fatalf("Login with pending terminal callback error=%v, want ErrCallbackBacklog", err)
	}
	go client.dispatchEvents()
	select {
	case pending := <-pendingDuringCallback:
		if pending {
			t.Fatal("terminal callback remained pending during its own invocation")
		}
	case <-time.After(time.Second):
		t.Fatal("terminal callback was not invoked")
	}
	client.lifetimeCancel()
}

func TestBestEffortCallbackDropIsObservable(t *testing.T) {
	lifetimeCtx, lifetimeCancel := context.WithCancel(context.Background())
	defer lifetimeCancel()
	client := &Client{
		logger:      defaultLogger(),
		lifetimeCtx: lifetimeCtx,
		events:      make(chan func(), 1),
	}
	client.events <- func() {}
	client.emit(func() {})
	if got := client.callbackDrops.Load(); got != 1 {
		t.Fatalf("callback drop counter=%d, want 1", got)
	}
	if got := client.Health().CallbackEventsDropped; got != 1 {
		t.Fatalf("Health callback drop counter=%d, want 1", got)
	}
}

func TestEarlierCallbackLoginFailsFastWhileTerminalIsPending(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	client.callbacks.disconnect = func(string, error) {}
	started := make(chan struct{})
	proceed := make(chan struct{})
	loginResult := make(chan error, 1)
	client.events <- func() {
		close(started)
		<-proceed
		loginResult <- client.Login(context.Background(), "next-user", "token")
	}
	go client.dispatchEvents()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("earlier callback did not start")
	}
	client.recordTerminal(newError(ErrAuthentication, "test terminal", ""))
	close(proceed)
	select {
	case err := <-loginResult:
		if errorCode(err) != ErrCallbackBacklog {
			t.Fatalf("reentrant Login error=%v, want ErrCallbackBacklog", err)
		}
	case <-time.After(time.Second):
		t.Fatal("earlier callback deadlocked on pending terminal callback")
	}
	client.lifetimeCancel()
}

func TestTerminalCallbackCanReenterLoginAfterClaim(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("expected test DNS failure")
	}))
	var attempted atomic.Bool
	loginResult := make(chan error, 1)
	client.callbacks.disconnect = func(string, error) {
		if !attempted.CompareAndSwap(false, true) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		loginResult <- client.Login(ctx, "next-user", "token")
	}
	client.recordTerminal(newError(ErrAuthentication, "test terminal", ""))
	go client.dispatchEvents()
	select {
	case err := <-loginResult:
		if errorCode(err) == ErrCallbackBacklog || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("terminal callback could not reenter Login: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal callback reentrant Login deadlocked")
	}
	client.lifetimeCancel()
}

func TestTerminalCallbackCanCloseWithoutSkippingBundle(t *testing.T) {
	client, codec := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	closed := make(chan error, 1)
	stateCalled := make(chan ConnState, 1)
	client.callbacks.disconnect = func(string, error) {
		closed <- client.Close(context.Background())
	}
	client.callbacks.connection = func(_ string, state ConnState) {
		stateCalled <- state
	}

	client.recordTerminal(newError(ErrAuthentication, "test terminal", ""))
	go client.dispatchEvents()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close from terminal callback: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close from terminal callback deadlocked")
	}
	select {
	case state := <-stateCalled:
		if state != ConnStateDisconnected {
			t.Fatalf("connection callback state=%s, want %s", state, ConnStateDisconnected)
		}
	case <-time.After(time.Second):
		t.Fatal("Close skipped the remaining terminal callback bundle")
	}
	if got := codec.closeCount.Load(); got != 1 {
		t.Fatalf("codec Close count=%d, want exactly one", got)
	}
}

func TestLifetimeCancellationStillDrainsPublishedTerminalCallback(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	called := make(chan struct{}, 1)
	client.callbacks.disconnect = func(string, error) { called <- struct{}{} }
	client.recordTerminal(newError(ErrAuthentication, "test terminal", ""))
	client.lifetimeCancel()
	go client.dispatchEvents()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("published terminal callback was lost during lifetime cancellation")
	}
}

func TestTerminalCallbackIsNotPublishedAfterClose(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	client.callbacks.disconnect = func(string, error) {}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.recordTerminal(newError(ErrAuthentication, "late terminal", ""))
	if client.terminalCallbackPending() {
		t.Fatal("terminal callback was published after Close")
	}
}
