package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/gorilla/websocket"
)

type writeRequest struct {
	frame []byte
	done  chan error
}
type batchJob struct {
	queue       queueKey
	d           *internalprotocol.Sync
	r           *connectionRun
	reservation *batchReservation
}
type ackResult struct {
	result *SendResult
	err    error
}
type provisionResult struct {
	provision *internalprotocol.Provision
	err       error
}

type connectionRun struct {
	client             *Client
	ctx                context.Context
	cancel             context.CancelFunc
	conn               *websocket.Conn
	endpoint           string
	generation         uint64
	writes             chan writeRequest
	provision          chan provisionResult
	logout             chan error
	done               chan struct{}
	closeOnce          sync.Once
	finalizeOnce       sync.Once
	wg                 sync.WaitGroup
	pendingMu          sync.Mutex
	pending            map[uint64]chan ackResult
	queueMu            sync.Mutex
	queues             map[queueKey]*queueState
	deferred           deferredQueueRing
	pullRequests       chan *queueState
	queueBudget        *byteBudget
	queueTimeout       time.Duration
	processBatchBudget *byteBudget
	decodeBudget       *byteBudget
	decodeWait         time.Duration
	activeQueues       atomic.Int32
	capacityHits       atomic.Uint64
	lastPong           atomic.Int64
}

func (c *Client) isClosed() bool { c.mu.RLock(); defer c.mu.RUnlock(); return c.closed }

// beginConnect serializes lifecycle operations without forcing callers to wait
// on an uninterruptible mutex. The returned channel is an ownership token and
// must be passed to endConnect on every path after successful acquisition.
func (c *Client) beginConnect(ctx context.Context, operation string) (chan struct{}, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, newError(ErrClientClosed, operation, "")
		}
		if c.connectDone == nil {
			done := make(chan struct{})
			c.connectDone = done
			c.mu.Unlock()
			return done, nil
		}
		done := c.connectDone
		c.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *Client) endConnect(done chan struct{}) {
	c.mu.Lock()
	if c.connectDone != done {
		c.mu.Unlock()
		panic("sdk: ending a lifecycle operation not owned by caller")
	}
	c.connectDone = nil
	close(done)
	c.mu.Unlock()
}

// Login 使用 userID/token 建立长连接会话。userID 应尽量使用全小写（大小写混杂
// 也可以），但务必保证该账号不会在移动端 SDK 上登录：移动端会把 userID 统一转为
// 全小写，若服务端账号存在大写字符，同一账号在移动端与服务端会对应到不同的 userID。
func (c *Client) Login(ctx context.Context, userID, token string) error {
	if strings.TrimSpace(userID) == "" {
		return newError(ErrNotLoggedIn, "login", "user ID is empty")
	}
	if strings.TrimSpace(token) == "" {
		return newError(ErrNotLoggedIn, "login", "token is empty")
	}
	connectDone, err := c.beginConnect(ctx, "login")
	if err != nil {
		return err
	}
	defer c.endConnect(connectDone)
	c.mu.Lock()
	// Close does not wait before marking the Client closed, so it may win the
	// race immediately after beginConnect grants ownership. Recheck while
	// installing login state to preserve Close's immediate Logout invariant.
	if c.closed {
		c.mu.Unlock()
		return newError(ErrClientClosed, "login", "")
	}
	if c.state != LoginStateLogout {
		c.mu.Unlock()
		return newError(ErrAlreadyLoggedIn, "login", "client is already active")
	}
	c.state = LoginStateLoggingIn
	c.connState = ConnStateConnecting
	c.userID = userID
	c.token = token
	c.lastErr = nil
	c.tokenExpiresAt = time.Time{}
	c.backoffAttempt.Store(0)
	cb := c.callbacks.connection
	c.mu.Unlock()
	if cb != nil {
		c.emit(func() { cb(userID, ConnStateConnecting) })
	}
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	stopLifecycleCancel := context.AfterFunc(c.eventCtx, cancel)
	defer stopLifecycleCancel()
	resolved, err := c.resolveCachedEndpointCandidates(connectCtx, 0)
	if err != nil {
		c.recordTerminal(err)
		return err
	}
	r, usedIndex, err := c.connectLoginCandidates(connectCtx, resolved.Endpoints.WSS)
	if err != nil {
		c.recordTerminal(err)
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		r.shutdown(newError(ErrClientClosed, "login", "closed while connecting"))
		return newError(ErrClientClosed, "login", "")
	}
	c.run = r
	c.state = LoginStateLoggedIn
	c.connState = ConnStateConnected
	c.msyncHost = r.endpoint
	c.msyncHosts = append([]string(nil), resolved.Endpoints.WSS...)
	c.msyncCursor = nextCandidateCursor(usedIndex, len(c.msyncHosts))
	c.dnsGeneration = resolved.Generation
	c.restBase = resolved.Endpoints.REST
	c.lastStableConnect.Store(time.Now().UnixNano())
	c.mu.Unlock()
	if cb != nil {
		c.emit(func() { cb(userID, ConnStateConnected) })
	}
	go c.monitor(r)
	return nil
}

const endpointConnectTimeout = 5 * time.Second

func (c *Client) connectLoginCandidates(ctx context.Context, candidates []string) (*connectionRun, int, error) {
	round := newEndpointRound(candidates, 0)
	var lastErr error
	for !round.exhausted() {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, -1, lastErr
			}
			return nil, -1, classifyNetworkError("login endpoints", err)
		}
		endpoint, index, _ := round.nextEndpoint()
		attemptCtx, cancel := context.WithTimeout(ctx, endpointConnectTimeout)
		r, err := c.connectEndpoint(attemptCtx, endpoint)
		cancel()
		if err == nil {
			return r, index, nil
		}
		lastErr = err
		if c.debug {
			c.logger.Debug("msync.endpoint_failed", "phase", "login", "candidate_index", index,
				"candidate_count", len(candidates), "error_code", errorCode(err))
		}
		if !shouldRotateEndpoint(err) {
			return nil, index, err
		}
	}
	return nil, -1, lastErr
}

func (c *Client) connectEndpoint(ctx context.Context, endpoint string) (*connectionRun, error) {
	if c.connectEndpointFn != nil {
		return c.connectEndpointFn(ctx, endpoint)
	}
	return c.connectWithRedirects(ctx, endpoint)
}

func nextCandidateCursor(usedIndex, candidateCount int) int {
	if candidateCount == 0 || usedIndex < 0 {
		return 0
	}
	return (usedIndex + 1) % candidateCount
}

func (c *Client) connectWithRedirects(ctx context.Context, endpoint string) (*connectionRun, error) {
	seen := map[string]struct{}{}
	for hop := 0; ; hop++ {
		if hop > maxRedirectHops {
			return nil, newError(ErrRedirectLimit, "provision", "maximum redirect hops exceeded")
		}
		if _, ok := seen[endpoint]; ok {
			return nil, newError(ErrRedirectLoop, "provision", endpoint)
		}
		seen[endpoint] = struct{}{}
		r, err := c.dial(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		p, err := r.login(ctx)
		if err == nil {
			c.acceptProvision(p)
			if err := r.sendUnread(); err != nil {
				r.shutdown(err)
				return nil, err
			}
			r.wg.Add(1)
			go r.heartbeat()
			return r, nil
		}
		var redir *redirectError
		if !errors.As(err, &redir) {
			r.shutdown(err)
			return nil, err
		}
		r.shutdown(err)
		next, e := redirectURL(endpoint, redir.info)
		if e != nil {
			return nil, e
		}
		endpoint = next
	}
}

func (c *Client) dial(ctx context.Context, endpoint string) (*connectionRun, error) {
	d := *c.wsDialer
	conn, resp, err := d.DialContext(ctx, endpoint, nil)
	if err != nil {
		if resp != nil {
			return nil, &SDKError{Code: ErrHandshake, Operation: "websocket handshake", HTTPStatus: resp.StatusCode, Cause: err}
		}
		return nil, classifyNetworkError("websocket dial", err)
	}
	conn.SetReadLimit(maxFrameBytes)
	rctx, cancel := context.WithCancel(context.Background())
	r := &connectionRun{
		client: c, ctx: rctx, cancel: cancel, conn: conn, endpoint: endpoint,
		generation: c.generation.Add(1), writes: make(chan writeRequest, writeQueueSize),
		provision: make(chan provisionResult, 1), logout: make(chan error, 1),
		done: make(chan struct{}), pending: map[uint64]chan ackResult{},
		queues: make(map[queueKey]*queueState), deferred: newDeferredQueueRing(maxTrackedQueues),
		pullRequests: make(chan *queueState, pullRequestCapacity),
		queueBudget:  mustNewByteBudget(connectionQueueBudgetBytes), queueTimeout: queueResponseTimeout,
	}
	r.lastPong.Store(time.Now().UnixNano())
	r.wg.Add(2)
	go r.writePump()
	go r.readPump()
	r.startPullScheduler()
	return r, nil
}

func (r *connectionRun) login(ctx context.Context) (*internalprotocol.Provision, error) {
	token := r.client.tokenValue()
	auth, _ := json.Marshal(map[string]string{"token": token})
	frame, err := r.client.codec.EncodeProvision(internalprotocol.ProvisionRequest{User: r.client.jid(r.client.currentUserID()), SDKVersion: sdkVersion, Resource: r.client.cfg.Resource, AuthToken: auth})
	if err != nil {
		return nil, err
	}
	if err = r.sendFrame(ctx, frame); err != nil {
		return nil, err
	}
	select {
	case out := <-r.provision:
		return out.provision, out.err
	case <-ctx.Done():
		return nil, classifyNetworkError("provision", ctx.Err())
	case <-r.done:
		return nil, newError(ErrStreamClosed, "provision", "connection closed")
	}
}

func (c *Client) jid(name string) internalprotocol.JID {
	return internalprotocol.JID{AppKey: c.cfg.AppKey, Name: name, Domain: c.cfg.Domain, ClientResource: c.cfg.Resource}
}

func (c *Client) currentUserID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userID
}

func (r *connectionRun) sendFrame(ctx context.Context, frame []byte) error {
	req := writeRequest{frame: frame, done: make(chan error, 1)}
	select {
	case r.writes <- req:
		if r.client.debug {
			r.client.logger.Debug("wss.outbound", "generation", r.generation, "bytes", len(frame))
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return newError(ErrStreamClosed, "write", "")
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		// The peer may close immediately after accepting the frame. Preserve a
		// completed write result instead of randomly selecting the close signal.
		select {
		case err := <-req.done:
			return err
		default:
			return newError(ErrStreamClosed, "write", "")
		}
	}
}

func (r *connectionRun) writePump() {
	defer r.wg.Done()
	writeTimeout := sendTimeout
	for {
		select {
		case req := <-r.writes:
			_ = r.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := r.conn.WriteMessage(websocket.BinaryMessage, req.frame)
			req.done <- classifyNetworkError("websocket write", err)
			if err != nil {
				r.fail(err)
				return
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *connectionRun) readPump() {
	defer r.wg.Done()
	readTimeout := heartbeatTimeout
	for {
		_ = r.conn.SetReadDeadline(time.Now().Add(readTimeout))
		typ, data, err := r.conn.ReadMessage()
		if err != nil {
			if r.client.debug {
				r.client.logger.Debug("wss.read_error", "generation", r.generation, "error", err)
			}
			r.fail(err)
			return
		}
		if typ != websocket.BinaryMessage {
			continue
		}
		r.client.lastInbound.Store(time.Now().UnixNano())
		if decodeErr := r.decodeAndDispatch(data); decodeErr != nil {
			if r.ctx.Err() != nil {
				return
			}
			if r.client.debug {
				r.client.logger.Debug("wss.decode_error", "generation", r.generation, "bytes", len(data), "error", decodeErr)
			}
			r.fail(decodeErr)
			return
		}
	}
}

func (r *connectionRun) decodeAndDispatch(data []byte) error {
	budget := r.decodeBudget
	if budget == nil {
		budget = processDecodeBudget
	}
	wait := r.decodeWait
	if wait <= 0 {
		wait = decodeAdmissionWait
	}
	weight, err := decodeAdmissionWeight(r.client.codec, data)
	if err != nil {
		if errors.Is(err, internalprotocol.ErrLimitExceeded) {
			return wrapError(ErrProtocolLimit, "decode frame", err)
		}
		return wrapError(ErrProtocol, "decode frame", err)
	}
	waitCtx, cancel := context.WithTimeout(r.ctx, wait)
	err = budget.Acquire(waitCtx, weight)
	cancel()
	if err != nil {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			r.client.decodeTimeouts.Add(1)
		}
		return wrapError(ErrHandlerBacklog, "decode admission", err)
	}
	defer func() {
		if releaseErr := budget.Release(weight); releaseErr != nil {
			r.client.logger.Error("decode budget release invariant failed", "bytes", weight, "error", releaseErr)
		}
	}()
	frame, err := r.client.codec.DecodeFrame(data)
	if err != nil {
		code := ErrProtocol
		if errors.Is(err, internalprotocol.ErrLimitExceeded) {
			code = ErrProtocolLimit
		}
		return wrapError(code, "decode frame", err)
	}
	if r.client.debug {
		r.client.logger.Debug("wss.inbound", "generation", r.generation, "command", frame.Command, "trace_id", frame.TraceID, "bytes", len(data))
	}
	r.dispatch(frame)
	return nil
}

func (r *connectionRun) dispatch(frame *internalprotocol.Frame) {
	switch frame.Command {
	case internalprotocol.CommandProvision:
		p := frame.Provision
		if p == nil {
			r.tryProvision(nil, newError(ErrProtocol, "provision", "missing payload"))
			return
		}
		st := p.Status
		if st == nil {
			r.tryProvision(nil, newError(ErrProtocol, "provision", "missing status"))
			return
		}
		if st.Code == internalprotocol.StatusOK {
			if r.client.debug {
				r.client.logger.Debug("wss.provision_ok", "configured_user", r.client.currentUserID(),
					"configured_resource", r.client.cfg.Resource, "session_id", p.SessionID,
					"auth_token_bytes", len(p.AuthToken))
			}
			r.tryProvision(p, nil)
			return
		}
		if st.Code == internalprotocol.StatusRedirect {
			r.tryProvision(nil, &redirectError{info: st.Redirects})
			return
		}
		r.tryProvision(nil, protocolError("provision", int32(st.Code), st.Reason))
	case internalprotocol.CommandUnread:
		if frame.Unread == nil {
			r.fail(newError(ErrProtocol, "unread", "missing payload"))
			return
		}
		st := frame.Unread.Status
		if st != nil && st.Code == internalprotocol.StatusRedirect {
			r.fail(&redirectError{info: st.Redirects})
			return
		}
		if st != nil && st.Code != internalprotocol.StatusOK {
			r.fail(protocolError("unread", int32(st.Code), st.Reason))
			return
		}
		// UNREAD is intentionally acknowledged but not consumed. This SDK's
		// scope is realtime long-link delivery; the service owns roaming/offline
		// history retrieval through its REST-facing workflow.
		if r.client.debug && len(frame.Unread.Queues) > 0 {
			r.client.logger.Debug("wss.unread_ignored", "queue_count", len(frame.Unread.Queues))
		}
		r.lastPong.Store(time.Now().UnixNano())
	case internalprotocol.CommandNotice:
		if frame.Notice != nil {
			if r.client.debug {
				r.client.logger.Debug("wss.notice", "queue", frame.Notice.ID())
			}
			r.startQueue(*frame.Notice)
		}
	case internalprotocol.CommandSync:
		if frame.Sync == nil {
			r.fail(newError(ErrProtocol, "sync", "missing payload"))
			return
		}
		if frame.Sync.MetaID > 0 {
			if r.client.debug {
				r.client.logger.Debug("wss.ack", "meta_id", frame.Sync.MetaID, "server_id", frame.Sync.ServerID, "server_timestamp", frame.Sync.Timestamp, "status", statusCode(frame.Sync.Status))
			}
			r.completeACK(frame.Sync)
		} else {
			if r.client.debug {
				r.client.logger.Debug("wss.sync_batch", "meta_count", len(frame.Sync.Metas), "queue", jidID(frame.Sync.Queue), "status", statusCode(frame.Sync.Status))
			}
			r.handleBatch(frame.Sync)
		}
	case internalprotocol.CommandLogout:
		var err error
		if frame.Logout != nil && frame.Logout.Status != nil && frame.Logout.Status.Code != internalprotocol.StatusOK {
			err = protocolError("logout", int32(frame.Logout.Status.Code), frame.Logout.Status.Reason)
		}
		select {
		case r.logout <- err:
		default:
		}
	}
}

func statusCode(s *internalprotocol.Status) int32 {
	if s == nil {
		return -1
	}
	return int32(s.Code)
}
func jidID(j *internalprotocol.JID) string {
	if j == nil {
		return ""
	}
	return j.ID()
}

func (r *connectionRun) tryProvision(p *internalprotocol.Provision, err error) {
	select {
	case r.provision <- provisionResult{p, err}:
	default:
	}
}

func (c *Client) acceptProvision(p *internalprotocol.Provision) {
	c.mu.Lock()
	c.sessionID = p.SessionID
	c.mu.Unlock()
	if len(p.AuthToken) > 0 {
		var v struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
		}
		if json.Unmarshal(p.AuthToken, &v) == nil && v.Token != "" {
			expiresAt := tokenExpiryTime(v.ExpiresIn, time.Now())
			c.updateProvisionToken(v.Token, expiresAt)
			c.scheduleTokenExpiryWarning(expiresAt)
		}
	}
}

// expires_in is historically named but C++ treats it as an expiry timestamp.
// Accept seconds, milliseconds, and (for private deployments) a relative TTL.
func tokenExpiryTime(value int64, now time.Time) time.Time {
	switch {
	case value <= 0:
		return time.Time{}
	case value >= 1_000_000_000_000:
		return time.UnixMilli(value)
	case value >= 1_000_000_000:
		return time.Unix(value, 0)
	default:
		return now.Add(time.Duration(value) * time.Second)
	}
}

func (c *Client) updateProvisionToken(token string, expiresAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
		c.tokenWarningCancel = nil
	}
	c.token = token
	c.tokenExpiresAt = expiresAt
}

func (c *Client) scheduleTokenExpiryWarning(expiresAt time.Time) {
	cb := c.callbackSnapshot().tokenWillExpire
	if expiresAt.IsZero() || cb == nil {
		return
	}
	userID := c.currentUserID()
	ctx, cancel := context.WithCancel(c.eventCtx)
	c.mu.Lock()
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
	}
	c.tokenWarningCancel = cancel
	c.mu.Unlock()
	delay := time.Until(expiresAt.Add(-tokenExpiryWarningBefore))
	go func() {
		timer := time.NewTimer(maxDuration(delay, 0))
		defer timer.Stop()
		select {
		case <-timer.C:
			c.emit(func() { cb(userID, expiresAt) })
		case <-ctx.Done():
		}
	}()
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (r *connectionRun) heartbeat() {
	defer r.wg.Done()
	tick := time.NewTicker(heartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if time.Since(time.Unix(0, r.lastPong.Load())) > heartbeatTimeout {
				r.fail(newError(ErrTimeout, "heartbeat", "pong timeout"))
				return
			}
			if err := r.sendUnread(); err != nil {
				r.fail(err)
				return
			}
		case <-r.ctx.Done():
			return
		}
	}
}
func (r *connectionRun) sendUnread() error {
	frame, err := r.client.codec.EncodeUnread()
	if err != nil {
		return err
	}
	// 心跳发送使用带 deadline 的 context，避免在写背压时永久阻塞；
	// 否则看门狗会被它本该监管的写队列卡死，无法触发 pong 超时。
	ctx, cancel := context.WithTimeout(r.ctx, sendTimeout)
	defer cancel()
	return r.sendFrame(ctx, frame)
}

func (r *connectionRun) fail(err error) {
	r.closeOnce.Do(func() {
		r.client.mu.Lock()
		r.client.lastErr = classifyNetworkError("connection", err)
		r.client.mu.Unlock()
		close(r.done)
		r.cancel()
		if r.conn != nil {
			_ = r.conn.Close()
		}
		r.cancelPending(err)
	})
}
func (r *connectionRun) shutdown(err error) {
	r.fail(err)
	r.wg.Wait()
	r.finalizeQueues()
}

func (c *Client) monitor(r *connectionRun) {
	<-r.done
	r.wg.Wait()
	r.finalizeQueues()
	c.mu.RLock()
	current := c.run == r
	closed := c.closed
	err := c.lastErr
	c.mu.RUnlock()
	if !current || closed {
		return
	}
	if isTerminal(err) || c.cfg.DisableReconnect {
		c.recordTerminalForRun(r, err)
		return
	}
	go c.reconnect(r)
}

func (c *Client) reconnect(old *connectionRun) {
	c.mu.Lock()
	if c.closed || c.run != old || c.state == LoginStateLogout {
		c.mu.Unlock()
		return
	}
	changed := c.connState != ConnStateReconnecting
	c.state = LoginStateReconnecting
	c.connState = ConnStateReconnecting
	stateCallback := c.callbacks.connection
	userID := c.userID
	effectiveEndpoint := c.msyncHost
	hosts := append([]string(nil), c.msyncHosts...)
	cursor := c.msyncCursor
	dnsGeneration := c.dnsGeneration
	lastErr := c.lastErr
	delayFn := c.reconnectDelayFn
	c.mu.Unlock()
	if delayFn == nil {
		delayFn = reconnectDelay
	}
	if changed && stateCallback != nil {
		c.emit(func() { stateCallback(userID, ConnStateReconnecting) })
	}

	round := newEndpointRound(hosts, cursor)
	endpoint := effectiveEndpoint
	endpointIndex := -1
	needRefresh := false
	var redirect *redirectError
	if errors.As(lastErr, &redirect) {
		if next, err := redirectURL(old.endpoint, redirect.info); err == nil {
			endpoint = next
		} else {
			c.recordTerminalForRun(old, err)
			return
		}
	} else if shouldRotateEndpoint(lastErr) {
		var ok bool
		endpoint, endpointIndex, ok = round.nextEndpoint()
		needRefresh = !ok
	}
	// 跨代际退避：连接只有在稳定运行超过 reconnectStableWindow 后才重置退避档位；
	// 否则毒消息/慢消费者导致的频繁重建会让退避跨代际累积到最高档，避免自伤式重连风暴。
	if last := c.lastStableConnect.Load(); last == 0 || time.Since(time.Unix(0, last)) > reconnectStableWindow {
		c.backoffAttempt.Store(0)
	}
	for attempt := int(c.backoffAttempt.Load()) + 1; ; attempt++ {
		c.backoffAttempt.Store(uint32(attempt))
		c.mu.RLock()
		closed := c.closed
		current := c.run == old && c.state == LoginStateReconnecting
		c.mu.RUnlock()
		if closed || !current {
			return
		}
		timer := time.NewTimer(delayFn(attempt))
		select {
		case <-timer.C:
		case <-c.eventCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
		c.mu.RLock()
		current = c.run == old && c.state == LoginStateReconnecting
		c.mu.RUnlock()
		if c.isClosed() || !current {
			return
		}
		stop := func() bool {
			connectDone, err := c.beginConnect(c.eventCtx, "reconnect")
			if err != nil {
				return true
			}
			defer c.endConnect(connectDone)
			// State may have changed while this attempt waited behind Login or
			// Logout. Revalidate after acquiring lifecycle ownership.
			c.mu.RLock()
			closed = c.closed
			current = c.run == old && c.state == LoginStateReconnecting
			c.mu.RUnlock()
			if closed || !current {
				return true
			}
			ctx, cancel := context.WithTimeout(c.eventCtx, connectTimeout)
			defer cancel()
			if needRefresh {
				resolved, err := c.resolveCachedEndpointCandidates(ctx, dnsGeneration)
				if err != nil {
					c.mu.Lock()
					c.lastErr = err
					c.mu.Unlock()
					if isTerminal(err) {
						c.recordTerminalForRun(old, err)
						return true
					}
					return false
				}
				hosts = append(hosts[:0], resolved.Endpoints.WSS...)
				dnsGeneration = resolved.Generation
				cursor = 0
				round = newEndpointRound(hosts, cursor)
				endpoint, endpointIndex, _ = round.nextEndpoint()
				needRefresh = false
				c.mu.Lock()
				if c.run == old && c.state == LoginStateReconnecting {
					c.msyncHosts = append([]string(nil), hosts...)
					c.msyncCursor = cursor
					c.dnsGeneration = dnsGeneration
					c.restBase = resolved.Endpoints.REST
				}
				c.mu.Unlock()
				if c.debug {
					c.logger.Debug("dns.reconnect_refresh", "generation", dnsGeneration,
						"stale", resolved.Stale, "candidate_count", len(hosts))
				}
			}

			r, err := c.connectEndpoint(ctx, endpoint)
			if err != nil {
				if c.isClosed() {
					return true
				}
				c.mu.Lock()
				c.lastErr = err
				c.mu.Unlock()
				if isTerminal(err) {
					c.recordTerminalForRun(old, err)
					return true
				}
				if c.debug {
					c.logger.Debug("msync.endpoint_failed", "phase", "reconnect", "candidate_index", endpointIndex,
						"candidate_count", len(hosts), "dns_generation", dnsGeneration, "error_code", errorCode(err))
				}
				if shouldRotateEndpoint(err) {
					var ok bool
					endpoint, endpointIndex, ok = round.nextEndpoint()
					needRefresh = !ok
				} else {
					endpoint = effectiveEndpoint
					endpointIndex = -1
				}
				return false
			}
			c.mu.Lock()
			if c.closed || c.run != old {
				c.mu.Unlock()
				r.shutdown(newError(ErrClientClosed, "reconnect", "superseded"))
				return true
			}
			c.run = r
			c.state = LoginStateLoggedIn
			c.connState = ConnStateConnected
			c.msyncHost = r.endpoint
			c.msyncHosts = append([]string(nil), hosts...)
			if endpointIndex >= 0 {
				cursor = nextCandidateCursor(endpointIndex, len(hosts))
			}
			c.msyncCursor = cursor
			c.dnsGeneration = dnsGeneration
			c.lastStableConnect.Store(time.Now().UnixNano())
			c.mu.Unlock()
			if cb := c.callbackSnapshot().connection; cb != nil {
				c.emit(func() { cb(userID, ConnStateConnected) })
			}
			go c.monitor(r)
			return true
		}()
		if stop {
			return
		}
	}
}

// reconnectStableWindow 是连接稳定运行超过该时长后重置退避档位的窗口。
const reconnectStableWindow = 5 * time.Minute

func reconnectDelay(attempt int) time.Duration {
	var lo, hi int
	if attempt <= 3 {
		lo, hi = 5, 10
	} else if attempt < 9 {
		lo, hi = 20, 40
	} else {
		lo, hi = 60, 120
	}
	return time.Duration(lo+rand.Intn(hi-lo+1)) * time.Second
}

func isTerminal(err error) bool {
	switch errorCode(err) {
	case ErrTokenExpired, ErrInvalidToken, ErrUserForbidden, ErrBindAnotherDevice, ErrTooManyDevices, ErrResourceChanged, ErrAuthentication, ErrPermissionDenied, ErrAppActiveLimit, ErrUserNotFound, ErrKickedChangePass, ErrProtocolLimit:
		return true
	}
	return false
}

func (c *Client) recordTerminal(err error) {
	c.recordTerminalForRun(nil, err)
}

func (c *Client) recordTerminalForRun(expected *connectionRun, err error) {
	c.mu.Lock()
	if expected != nil && c.run != expected {
		c.mu.Unlock()
		return
	}
	c.lastErr = err
	c.state = LoginStateLogout
	c.connState = ConnStateDisconnected
	c.run = nil
	c.sessionID = ""
	userID := c.userID
	c.userID = ""
	c.token = ""
	c.msyncHost = ""
	c.msyncHosts = nil
	c.msyncCursor = 0
	c.dnsGeneration = 0
	c.restBase = ""
	c.tokenExpiresAt = time.Time{}
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
		c.tokenWarningCancel = nil
	}
	cb := c.callbacks.disconnect
	c.mu.Unlock()
	c.fireErrorCallback(userID, err)
	if cb != nil {
		c.emit(func() { cb(userID, err) })
	}
	if state := c.callbackSnapshot().connection; state != nil {
		c.emit(func() { state(userID, ConnStateDisconnected) })
	}
}
func (c *Client) fireErrorCallback(userID string, err error) {
	callbacks := c.callbackSnapshot()
	switch errorCode(err) {
	case ErrTokenExpired:
		if callbacks.tokenExpired != nil {
			c.emit(func() { callbacks.tokenExpired(userID) })
		}
	}
}

// Close permanently closes the Client. The first call marks it closed and
// cancels its active connection immediately, then a shared finalizer waits for
// any in-flight lifecycle operation before destroying the native codec.
//
// A nil return means that shared finalizer has completed. If ctx expires first,
// Close returns ctx.Err while finalization continues asynchronously; later calls
// wait for the same finalizer. Callers that need bounded shutdown latency must
// pass a context with a deadline. In particular, Close(context.Background()) can
// wait forever when a custom HTTP transport ignores Request.Context and never
// returns.
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.eventCancel()
		if c.tokenWarningCancel != nil {
			c.tokenWarningCancel()
			c.tokenWarningCancel = nil
		}
		r := c.run
		c.run = nil
		c.state = LoginStateLogout
		c.connState = ConnStateDisconnected
		c.sessionID = ""
		c.userID = ""
		c.token = ""
		c.msyncHost = ""
		c.msyncHosts = nil
		c.msyncCursor = 0
		c.dnsGeneration = 0
		c.restBase = ""
		c.tokenExpiresAt = time.Time{}
		c.mu.Unlock()
		go c.finalizeClose(r)
	})
	select {
	case <-c.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) finalizeClose(r *connectionRun) {
	if r != nil {
		r.shutdown(newError(ErrClientClosed, "close", ""))
	}
	// closed prevents a new lifecycle operation from registering. The one
	// observed here (if any) owns cleanup of every temporary run it created.
	c.mu.RLock()
	connectDone := c.connectDone
	c.mu.RUnlock()
	if connectDone != nil {
		<-connectDone
	}
	c.waitPendingDNSFlights()
	c.drainBatchJobs()
	if closer, ok := c.codec.(interface{ Close() }); ok {
		closer.Close()
	}
	// Do not wait for eventWG here: a callback is allowed to call Close, and
	// waiting from the dispatcher goroutine would deadlock on itself.
	close(c.closeDone)
}

func (c *Client) drainBatchJobs() {
	for {
		select {
		case job := <-c.batches:
			if job.reservation != nil {
				if err := job.reservation.Release(); err != nil && c.logger != nil {
					c.logger.Error("batch budget drain invariant failed", "error", err)
				}
			}
		default:
			return
		}
	}
}

// Logout ends the current session while keeping the Client reusable. Waiting
// for another lifecycle operation is governed by ctx; if that wait times out,
// Logout returns ctx.Err without clearing the current login state.
func (c *Client) Logout(ctx context.Context) error {
	connectDone, err := c.beginConnect(ctx, "logout")
	if err != nil {
		return err
	}
	defer c.endConnect(connectDone)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return newError(ErrClientClosed, "logout", "")
	}
	r := c.run
	session := c.sessionID
	wasConnected := c.connState != ConnStateDisconnected
	userID := c.userID
	c.run = nil
	c.state = LoginStateLogout
	c.connState = ConnStateDisconnected
	c.sessionID = ""
	c.userID = ""
	c.msyncHost = ""
	c.msyncHosts = nil
	c.msyncCursor = 0
	c.dnsGeneration = 0
	c.restBase = ""
	c.tokenExpiresAt = time.Time{}
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
		c.tokenWarningCancel = nil
	}
	c.token = ""
	stateCallback := c.callbacks.connection
	c.mu.Unlock()
	if r == nil {
		if wasConnected && stateCallback != nil {
			c.emit(func() { stateCallback(userID, ConnStateDisconnected) })
		}
		return nil
	}
	frame, err := c.codec.EncodeLogout(internalprotocol.LogoutRequest{SessionID: session, Reason: "client logout"})
	if err == nil {
		err = r.sendFrame(ctx, frame)
	}
	if err == nil {
		select {
		case err = <-r.logout:
		case <-ctx.Done():
			err = ctx.Err()
		case <-time.After(logoutTimeout):
			err = newError(ErrTimeout, "logout", "")
		}
	}
	// A successful logout frame can race the peer's immediate websocket close.
	// If the protocol response was already delivered, it is authoritative.
	if errorCode(err) == ErrStreamClosed {
		select {
		case logoutErr := <-r.logout:
			err = logoutErr
		default:
		}
	}
	r.shutdown(newError(ErrStreamClosed, "logout", "session ended"))
	if wasConnected && stateCallback != nil {
		c.emit(func() { stateCallback(userID, ConnStateDisconnected) })
	}
	return err
}

type redirectError struct {
	info []internalprotocol.RedirectInfo
}

func (e *redirectError) Error() string { return "server redirect" }
func redirectURL(current string, infos []internalprotocol.RedirectInfo) (string, error) {
	if len(infos) == 0 {
		return "", newError(ErrProtocol, "redirect", "empty endpoint")
	}
	u, err := url.Parse(current)
	if err != nil {
		return "", err
	}
	i := infos[0]
	if i.Host == "" || i.Port == 0 || i.Port > 65535 {
		return "", newError(ErrProtocol, "redirect", "invalid endpoint")
	}
	u.Scheme = "wss"
	u.Host = i.Host + ":" + strconv.Itoa(int(i.Port))
	if u.Path == "" {
		u.Path = "/websocket"
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
