package sdk

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func newDisconnectedReconnectRun(c *Client) *connectionRun {
	runCtx, cancel := context.WithCancel(context.Background())
	r := &connectionRun{
		client:      c,
		ctx:         runCtx,
		cancel:      cancel,
		writes:      make(chan writeRequest, 1),
		provision:   make(chan provisionResult, 1),
		logout:      make(chan error, 1),
		done:        make(chan struct{}),
		pending:     make(map[uint64]chan ackResult),
		queues:      make(map[queueKey]*queueState),
		queueBudget: mustNewByteBudget(connectionQueueBudgetBytes),
	}
	r.fail(newError(ErrIO, "test reconnect", "connection lost"))
	return r
}

func newOpenReconnectRun(c *Client, endpoint string) *connectionRun {
	runCtx, cancel := context.WithCancel(context.Background())
	return &connectionRun{
		client:      c,
		ctx:         runCtx,
		cancel:      cancel,
		endpoint:    endpoint,
		writes:      make(chan writeRequest, 1),
		provision:   make(chan provisionResult, 1),
		logout:      make(chan error, 1),
		done:        make(chan struct{}),
		pending:     make(map[uint64]chan ackResult),
		queues:      make(map[queueKey]*queueState),
		queueBudget: mustNewByteBudget(connectionQueueBudgetBytes),
	}
}

func prepareReconnectSession(c *Client) *connectionRun {
	c.mu.Lock()
	_, _ = c.startSessionLocked()
	c.mu.Unlock()

	old := newDisconnectedReconnectRun(c)
	c.mu.Lock()
	c.run = old
	c.state = LoginStateReconnecting
	c.connState = ConnStateReconnecting
	c.userID = "reconnect-user"
	c.sessionID = "session"
	c.msyncHost = "wss://reconnect.example/websocket"
	c.msyncHosts = []string{c.msyncHost}
	c.msyncCursor = 0
	c.lastErr = newError(ErrIO, "test reconnect", "connection lost")
	c.reconnectDelayFn = func(int) time.Duration { return 0 }
	c.mu.Unlock()
	return old
}

func TestLogoutCancelsReconnectDialBeforeWaitingForLifecycleOwnership(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	old := prepareReconnectSession(client)
	started := make(chan struct{})
	client.connectEndpointFn = func(ctx context.Context, _ string) (*connectionRun, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	reconnectDone := make(chan struct{})
	go func() {
		client.reconnect(old)
		close(reconnectDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not enter dial")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	startedLogout := time.Now()
	err := client.Logout(ctx)
	cancel()
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Logout waited for reconnect timeout: %v", err)
	}
	if elapsed := time.Since(startedLogout); elapsed > 500*time.Millisecond {
		t.Fatalf("Logout took %v after reconnect became cancelable", elapsed)
	}
	select {
	case <-reconnectDone:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not exit after Logout")
	}
	client.mu.RLock()
	state, run := client.state, client.run
	client.mu.RUnlock()
	if state != LoginStateLogout || run != nil {
		t.Fatalf("Logout state=%s run=%p, want logout with no run", state, run)
	}
}

func TestLogoutRejectsReconnectThatSucceedsAfterSessionCancellation(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	old := prepareReconnectSession(client)
	replacement := newOpenReconnectRun(client, "wss://replacement.example/websocket")
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	client.connectEndpointFn = func(ctx context.Context, _ string) (*connectionRun, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return replacement, nil
	}
	reconnectDone := make(chan struct{})
	go func() {
		client.reconnect(old)
		close(reconnectDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not enter dial")
	}

	logoutResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		logoutResult <- client.Logout(ctx)
	}()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Logout did not cancel reconnect dial")
	}
	close(release)
	select {
	case err := <-logoutResult:
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Logout timed out after canceled dial returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Logout did not finish")
	}
	select {
	case <-reconnectDone:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not finish")
	}
	client.mu.RLock()
	state, run := client.state, client.run
	client.mu.RUnlock()
	if state != LoginStateLogout || run != nil {
		t.Fatalf("stale reconnect installed after Logout: state=%s run=%p", state, run)
	}
	select {
	case <-replacement.done:
	default:
		t.Fatal("rejected replacement run was not shut down")
	}
}

func TestLogoutDeadlineDetachesReconnectThatIgnoresCancellation(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	old := prepareReconnectSession(client)
	replacement := newOpenReconnectRun(client, "wss://late.example/websocket")
	replacement.acceptedProvision = provisionState{
		sessionID: "stale-session",
		token:     "stale-token",
		expiresAt: time.Now().Add(time.Hour),
	}
	started := make(chan struct{})
	release := make(chan struct{})
	client.connectEndpointFn = func(context.Context, string) (*connectionRun, error) {
		close(started)
		<-release
		return replacement, nil
	}
	reconnectDone := make(chan struct{})
	go func() {
		client.reconnect(old)
		close(reconnectDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not enter non-cooperative dial")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := client.Logout(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Logout error=%v, want context deadline", err)
	}
	client.mu.RLock()
	state, run := client.state, client.run
	client.mu.RUnlock()
	if state != LoginStateLogout || run != nil {
		t.Fatalf("timed-out Logout left canceled session active: state=%s run=%p", state, run)
	}

	close(release)
	select {
	case <-reconnectDone:
	case <-time.After(time.Second):
		t.Fatal("non-cooperative reconnect did not finish after release")
	}
	client.mu.RLock()
	sessionID, token, run := client.sessionID, client.token, client.run
	client.mu.RUnlock()
	if sessionID != "" || token != "" || run != nil {
		t.Fatalf("late reconnect polluted detached session: session=%q token=%q run=%p", sessionID, token, run)
	}
	select {
	case <-replacement.done:
	default:
		t.Fatal("late replacement run was not shut down")
	}
}

func TestLogoutTimeoutBehindNonReconnectOperationPreservesActiveSession(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	client.mu.Lock()
	sessionCtx, _ := client.startSessionLocked()
	client.state = LoginStateLoggedIn
	client.connState = ConnStateConnected
	owner := make(chan struct{})
	client.connectDone = owner
	client.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := client.Logout(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Logout error=%v, want context deadline", err)
	}
	select {
	case <-sessionCtx.Done():
		t.Fatal("Logout canceled an active session while waiting behind a non-reconnect operation")
	default:
	}
	client.mu.RLock()
	state, connState := client.state, client.connState
	client.mu.RUnlock()
	if state != LoginStateLoggedIn || connState != ConnStateConnected {
		t.Fatalf("Logout changed active state while waiting: state=%s conn=%s", state, connState)
	}

	client.endConnect(owner)
	client.mu.Lock()
	client.cancelSessionLocked()
	client.mu.Unlock()
	client.lifetimeCancel()
}

func TestCanceledSessionCannotCancelReplacementSession(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	client.mu.Lock()
	oldCtx, oldGeneration := client.startSessionLocked()
	oldCancel := client.sessionCancel
	client.cancelSessionLocked()
	newCtx, newGeneration := client.startSessionLocked()
	client.mu.Unlock()

	if oldGeneration == newGeneration {
		t.Fatalf("session generation was reused: %d", oldGeneration)
	}
	select {
	case <-oldCtx.Done():
	default:
		t.Fatal("old session was not canceled")
	}
	oldCancel()
	select {
	case <-newCtx.Done():
		t.Fatal("old session cancel affected replacement session")
	default:
	}
}

func TestTokenWarningDoesNotCrossSession(t *testing.T) {
	client, _ := newLifecycleContextClient(lifecycleContextRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	}))
	warnings := make(chan struct{}, 2)
	client.callbacks.tokenWillExpire = func(string, time.Time) { warnings <- struct{}{} }
	client.userID = "token-user"

	client.mu.Lock()
	oldCtx, oldGeneration := client.startSessionLocked()
	oldExpiry := time.Now().Add(tokenExpiryWarningBefore)
	client.tokenExpiresAt = oldExpiry
	client.mu.Unlock()
	client.scheduleTokenExpiryWarning(oldCtx, oldGeneration, oldExpiry)
	var oldCallback func()
	select {
	case oldCallback = <-client.events:
	case <-time.After(time.Second):
		t.Fatal("old session warning was not queued")
	}

	client.mu.Lock()
	client.cancelSessionLocked()
	newCtx, newGeneration := client.startSessionLocked()
	newExpiry := time.Now().Add(tokenExpiryWarningBefore)
	client.tokenExpiresAt = newExpiry
	client.mu.Unlock()
	oldCallback()
	select {
	case <-warnings:
		t.Fatal("queued token warning crossed into the replacement session")
	default:
	}

	client.scheduleTokenExpiryWarning(newCtx, newGeneration, newExpiry)
	var newCallback func()
	select {
	case newCallback = <-client.events:
	case <-time.After(time.Second):
		t.Fatal("replacement session warning was not queued")
	}
	newCallback()
	select {
	case <-warnings:
	case <-time.After(time.Second):
		t.Fatal("replacement session warning was not delivered")
	}
	client.lifetimeCancel()
}
