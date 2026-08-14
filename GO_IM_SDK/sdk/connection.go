package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
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
	id string
	d  *internalprotocol.Sync
	r  *connectionRun
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
	client       *Client
	ctx          context.Context
	cancel       context.CancelFunc
	conn         *websocket.Conn
	endpoint     string
	generation   uint64
	writes       chan writeRequest
	provision    chan provisionResult
	logout       chan error
	done         chan struct{}
	closeOnce    sync.Once
	wg           sync.WaitGroup
	pendingMu    sync.Mutex
	pending      map[uint64]chan ackResult
	queueMu      sync.Mutex
	queues       map[string]*queueState
	activeQueues atomic.Int32
	lastPong     atomic.Int64
}

func (c *Client) isClosed() bool { c.mu.RLock(); defer c.mu.RUnlock(); return c.closed }

func (c *Client) Connect(ctx context.Context) error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return newError(ErrClientClosed, "connect", "")
	}
	if c.state != LoginStateLogout {
		c.mu.Unlock()
		return fmt.Errorf("client is already active")
	}
	c.state = LoginStateLoggingIn
	c.connState = ConnStateConnecting
	cb := c.callbacks.connection
	c.mu.Unlock()
	if cb != nil {
		c.emit(func() { cb(ConnStateConnecting) })
	}
	connectCtx, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
	defer cancel()
	stopLifecycleCancel := context.AfterFunc(c.eventCtx, cancel)
	defer stopLifecycleCancel()
	r, err := c.connectWithRedirects(connectCtx, c.cfg.MsyncHost)
	if err != nil {
		c.recordTerminal(err)
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		r.shutdown(newError(ErrClientClosed, "connect", "closed while connecting"))
		return newError(ErrClientClosed, "connect", "")
	}
	c.run = r
	c.state = LoginStateLoggedIn
	c.connState = ConnStateConnected
	c.lastStableConnect.Store(time.Now().UnixNano())
	c.mu.Unlock()
	if cb != nil {
		c.emit(func() { cb(ConnStateConnected) })
	}
	go c.monitor(r)
	return nil
}

func (c *Client) connectWithRedirects(ctx context.Context, endpoint string) (*connectionRun, error) {
	seen := map[string]struct{}{}
	for hop := 0; ; hop++ {
		if hop > c.cfg.MaxRedirectHops {
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
	conn.SetReadLimit(c.cfg.MaxFrameBytes)
	rctx, cancel := context.WithCancel(context.Background())
	r := &connectionRun{client: c, ctx: rctx, cancel: cancel, conn: conn, endpoint: endpoint, generation: c.generation.Add(1), writes: make(chan writeRequest, c.cfg.WriteQueueSize), provision: make(chan provisionResult, 1), logout: make(chan error, 1), done: make(chan struct{}), pending: map[uint64]chan ackResult{}, queues: map[string]*queueState{}}
	r.lastPong.Store(time.Now().UnixNano())
	r.wg.Add(2)
	go r.writePump()
	go r.readPump()
	return r, nil
}

func (r *connectionRun) login(ctx context.Context) (*internalprotocol.Provision, error) {
	token := r.client.tokenValue()
	auth, _ := json.Marshal(map[string]string{"token": token})
	frame, err := r.client.codec.EncodeProvision(internalprotocol.ProvisionRequest{User: r.client.jid(r.client.cfg.UserID), SDKVersion: r.client.cfg.SDKVersion, Resource: r.client.cfg.Resource, AuthToken: auth})
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
	writeTimeout := r.client.cfg.SendTimeout
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
	readTimeout := r.client.cfg.HeartbeatTimeout
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
		frame, decodeErr := r.client.codec.DecodeFrame(data)
		if decodeErr != nil {
			if r.client.debug {
				r.client.logger.Debug("wss.decode_error", "generation", r.generation, "bytes", len(data), "error", decodeErr)
			}
			r.fail(wrapError(ErrProtocol, "decode frame", decodeErr))
			return
		}
		if r.client.debug {
			r.client.logger.Debug("wss.inbound", "generation", r.generation, "command", frame.Command, "trace_id", frame.TraceID, "bytes", len(data))
		}
		r.dispatch(frame)
	}
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
				r.client.logger.Debug("wss.provision_ok", "configured_user", r.client.cfg.UserID,
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
			if cb := c.callbackSnapshot().tokenRotated; cb != nil {
				c.emit(func() { cb(v.Token, v.ExpiresIn) })
			}
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
	ctx, cancel := context.WithCancel(c.eventCtx)
	c.mu.Lock()
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
	}
	c.tokenWarningCancel = cancel
	c.mu.Unlock()
	delay := time.Until(expiresAt.Add(-c.cfg.TokenExpiryWarningBefore))
	go func() {
		timer := time.NewTimer(maxDuration(delay, 0))
		defer timer.Stop()
		select {
		case <-timer.C:
			c.emit(func() { cb(expiresAt) })
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
	tick := time.NewTicker(r.client.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if time.Since(time.Unix(0, r.lastPong.Load())) > r.client.cfg.HeartbeatTimeout {
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
	ctx, cancel := context.WithTimeout(r.ctx, r.client.cfg.SendTimeout)
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
		_ = r.conn.Close()
		r.cancelPending(err)
	})
}
func (r *connectionRun) shutdown(err error) { r.fail(err); r.wg.Wait() }

func (c *Client) monitor(r *connectionRun) {
	<-r.done
	r.wg.Wait()
	c.mu.RLock()
	current := c.run == r
	closed := c.closed
	err := c.lastErr
	c.mu.RUnlock()
	if !current || closed {
		return
	}
	if isTerminal(err) || c.cfg.DisableReconnect {
		c.recordTerminal(err)
		return
	}
	go c.reconnect(r)
}

func (c *Client) reconnect(old *connectionRun) {
	c.setStates(LoginStateReconnecting, ConnStateReconnecting)
	endpoint := c.cfg.MsyncHost
	c.mu.RLock()
	lastErr := c.lastErr
	c.mu.RUnlock()
	var redirect *redirectError
	if errors.As(lastErr, &redirect) {
		if next, err := redirectURL(old.endpoint, redirect.info); err == nil {
			endpoint = next
		} else {
			c.recordTerminal(err)
			return
		}
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
		c.mu.RUnlock()
		if closed {
			return
		}
		timer := time.NewTimer(reconnectDelay(attempt))
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
		if c.isClosed() {
			return
		}
		c.connectMu.Lock()
		ctx, cancel := context.WithTimeout(c.eventCtx, c.cfg.ConnectTimeout)
		r, err := c.connectWithRedirects(ctx, endpoint)
		cancel()
		c.connectMu.Unlock()
		if err != nil {
			if c.isClosed() {
				return
			}
			c.mu.Lock()
			c.lastErr = err
			c.mu.Unlock()
			if isTerminal(err) {
				c.recordTerminal(err)
				return
			}
			continue
		}
		c.mu.Lock()
		if c.closed || c.run != old {
			c.mu.Unlock()
			r.shutdown(newError(ErrClientClosed, "reconnect", "superseded"))
			return
		}
		c.run = r
		c.state = LoginStateLoggedIn
		c.connState = ConnStateConnected
		c.lastStableConnect.Store(time.Now().UnixNano())
		c.mu.Unlock()
		if cb := c.callbackSnapshot().connection; cb != nil {
			c.emit(func() { cb(ConnStateConnected) })
		}
		go c.monitor(r)
		return
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
	case ErrTokenExpired, ErrInvalidToken, ErrUserForbidden, ErrBindAnotherDevice, ErrTooManyDevices, ErrResourceChanged, ErrAuthentication, ErrPermissionDenied, ErrAppActiveLimit, ErrUserNotFound, ErrKickedChangePass:
		return true
	}
	return false
}

func (c *Client) recordTerminal(err error) {
	c.mu.Lock()
	c.lastErr = err
	c.state = LoginStateLogout
	c.connState = ConnStateDisconnected
	cb := c.callbacks.disconnect
	c.mu.Unlock()
	c.fireErrorCallback(err)
	if cb != nil {
		c.emit(func() { cb(err) })
	}
	if state := c.callbackSnapshot().connection; state != nil {
		c.emit(func() { state(ConnStateDisconnected) })
	}
}
func (c *Client) fireErrorCallback(err error) {
	callbacks := c.callbackSnapshot()
	switch errorCode(err) {
	case ErrTokenExpired:
		if callbacks.tokenExpired != nil {
			c.emit(callbacks.tokenExpired)
		}
	case ErrUserForbidden:
		if callbacks.forbidden != nil {
			c.emit(callbacks.forbidden)
		}
	}
}

func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.eventCancel()
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
		c.tokenWarningCancel = nil
	}
	r := c.run
	c.mu.Unlock()
	if r != nil {
		r.shutdown(newError(ErrClientClosed, "close", ""))
	}
	c.setStates(LoginStateLogout, ConnStateDisconnected)
	// Wait until an in-flight initial/reconnect handshake has stopped before
	// destroying the shared native codec handle.
	c.connectMu.Lock()
	c.connectMu.Unlock()
	if closer, ok := c.codec.(interface{ Close() }); ok {
		closer.Close()
	}
	c.mu.Lock()
	if c.run == r {
		c.run = nil
	}
	c.sessionID = ""
	c.mu.Unlock()
	// Do not wait for eventWG here: a callback is allowed to call Close, and
	// waiting from the dispatcher goroutine would deadlock on itself.
	return nil
}

func (c *Client) Logout(ctx context.Context) error {
	c.mu.RLock()
	r := c.run
	session := c.sessionID
	c.mu.RUnlock()
	if r == nil {
		return c.Close(ctx)
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
		case <-time.After(c.cfg.LogoutTimeout):
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
	_ = c.Close(context.Background())
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
