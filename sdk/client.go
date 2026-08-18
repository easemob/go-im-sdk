package sdk

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/gorilla/websocket"
)

// MessageHandler 处理一条入站消息。
//
// 契约：handler 必须遵守传入的 context 并及时返回。context 会在
// handlerTimeout 后取消，但取消是协作式的——SDK 无法强制终止一个忽略
// context 且永久阻塞的 handler。这样的 handler 会永久占用一个并发槽位，
// 并使 Health().StuckHandlers 持续增长。业务方必须在 handler 内部检查
// ctx、限制阻塞时长并自行管理外部资源。
//
// 投递语义为 at-least-once：连接重连时服务端可能重投同一批次，handler
// 必须按 Message.MetaID（或稳定的业务消息 ID）幂等处理，不能依赖"绝不重复"。
type MessageHandler func(context.Context, *Message) error

type Telemetry interface {
	Record(context.Context, TelemetryEvent)
}
type TelemetryEvent struct {
	Operation   string
	Attempt     int
	Duration    time.Duration
	StatusCode  int
	ServiceCode string
	Error       string
	Generation  uint64
	Queue       string
	Backlog     int
}

type Config struct {
	AppKey string
	Domain string
	// Resource is a UUID-like stable identity for this logical service
	// instance. The application must generate it once, persist the raw value,
	// and reuse exactly the same value after a crash, restart, or failover.
	// Changing it is treated by the server as logging in from another device.
	// Existing deployments must retain their persisted value even if it is not
	// UUID-shaped.
	// The SDK adds the go-server-imsdk- prefix before using it.
	// Each IM user may be used by only one live service instance; logging in from
	// another service or client kicks this connection and is reported through
	// OnDisconnect, without a dedicated kick callback.
	Resource                 string
	DisableReconnect         bool
	HTTPClient               *http.Client
	Logger                   *slog.Logger
	Telemetry                Telemetry
	Debug                    bool
	MessageHandler           MessageHandler
	OnConnectionStateChanged func(userID string, state ConnState)
	OnDisconnect             func(userID string, err error)
	// OnTokenExpired reports that authentication failed because the current
	// token is already expired. The connection is terminal at this point.
	OnTokenExpired func(userID string)
	// OnTokenWillExpire reports the expiry time learned from Provision before
	// it is reached, using tokenExpiryWarningBefore as the lead time.
	OnTokenWillExpire func(userID string, expiresAt time.Time)
}

type LoginState int

const (
	LoginStateLogout LoginState = iota
	LoginStateLoggingIn
	LoginStateLoggedIn
	LoginStateReconnecting
)

func (s LoginState) String() string {
	switch s {
	case LoginStateLogout:
		return "logout"
	case LoginStateLoggingIn:
		return "logging_in"
	case LoginStateLoggedIn:
		return "logged_in"
	case LoginStateReconnecting:
		return "reconnecting"
	default:
		return fmt.Sprintf("login_state(%d)", int(s))
	}
}

type ConnState int

const (
	ConnStateDisconnected ConnState = iota
	ConnStateConnecting
	ConnStateConnected
	ConnStateReconnecting
)

func (s ConnState) String() string {
	switch s {
	case ConnStateDisconnected:
		return "disconnected"
	case ConnStateConnecting:
		return "connecting"
	case ConnStateConnected:
		return "connected"
	case ConnStateReconnecting:
		return "reconnecting"
	default:
		return fmt.Sprintf("conn_state(%d)", int(s))
	}
}

type HealthStatus struct {
	Connected                  bool       `json:"connected"`
	SessionID                  string     `json:"session_id,omitempty"`
	LoginState                 LoginState `json:"login_state"`
	ConnectionGeneration       uint64     `json:"connection_generation"`
	LastInbound                time.Time  `json:"last_inbound"`
	WriteBacklog               int        `json:"write_backlog"`
	QueueBacklog               int        `json:"queue_backlog"`
	KnownQueues                int        `json:"known_queues"`
	DeferredQueues             int        `json:"deferred_queues"`
	OutstandingPulls           int        `json:"outstanding_pulls"`
	QueueTrackedBytes          int64      `json:"queue_tracked_bytes"`
	QueueCapacityRejects       uint64     `json:"queue_capacity_rejects"`
	ClientBatchBacklogBytes    int64      `json:"client_batch_backlog_bytes"`
	ProcessBatchBacklogBytes   int64      `json:"process_batch_backlog_bytes"`
	ProcessDecodeInFlightBytes int64      `json:"process_decode_in_flight_bytes"`
	ClientBatchBudgetRejects   uint64     `json:"client_batch_budget_rejects"`
	ProcessBatchBudgetRejects  uint64     `json:"process_batch_budget_rejects"`
	DecodeAdmissionTimeouts    uint64     `json:"decode_admission_timeouts"`
	DNSGeneration              uint64     `json:"dns_generation"`
	MSyncCandidateCount        int        `json:"msync_candidate_count"`
	MSyncNextCandidate         int        `json:"msync_next_candidate"`
	LastError                  string     `json:"last_error,omitempty"`
	TokenExpiresAt             time.Time  `json:"token_expires_at,omitempty"`
	// StuckHandlers 是当前超过 handlerTimeout 仍未返回的 handler 数量。
	// 持续大于 0 说明存在忽略 context 的 handler，应告警并定位修复。
	StuckHandlers int `json:"stuck_handlers"`
}

type callbacks struct {
	connection      func(string, ConnState)
	disconnect      func(string, error)
	tokenExpired    func(string)
	tokenWillExpire func(string, time.Time)
}

type Client struct {
	cfg                 Config
	logger              *slog.Logger
	mu                  sync.RWMutex
	connectDone         chan struct{}
	pendingDNSFlights   map[<-chan struct{}]struct{}
	closeOnce           sync.Once
	closeDone           chan struct{}
	state               LoginState
	connState           ConnState
	run                 *connectionRun
	userID              string
	token               string
	msyncHost           string
	msyncHosts          []string
	msyncCursor         int
	dnsGeneration       uint64
	restBase            string
	sessionID           string
	generation          atomic.Uint64
	lastInbound         atomic.Int64
	backoffAttempt      atomic.Uint32
	lastStableConnect   atomic.Int64
	stuckHandlers       atomic.Int32
	lastErr             error
	tokenExpiresAt      time.Time
	tokenWarningCancel  context.CancelFunc
	closed              bool
	idCounter           atomic.Uint64
	callbacks           callbacks
	wsDialer            *websocket.Dialer
	connectEndpointFn   func(context.Context, string) (*connectionRun, error)
	eventCtx            context.Context
	eventCancel         context.CancelFunc
	events              chan func()
	batches             chan batchJob
	batchBudget         *byteBudget
	clientBatchRejects  atomic.Uint64
	processBatchRejects atomic.Uint64
	decodeTimeouts      atomic.Uint64
	reconnectDelayFn    func(int) time.Duration
	eventWG             sync.WaitGroup
	codec               internalprotocol.Codec
	debug               bool
}

func New(cfg Config) (*Client, error) {
	if err := applyDefaultsAndValidate(&cfg); err != nil {
		return nil, err
	}
	var seed [4]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("generate message id prefix: %w", err)
	}
	eventCtx, eventCancel := context.WithCancel(context.Background())
	codec, err := newProtocolCodec()
	if err != nil {
		eventCancel()
		return nil, fmt.Errorf("initialize protocol codec: %w", err)
	}
	c := &Client{cfg: cfg, logger: cfg.Logger, state: LoginStateLogout, connState: ConnStateDisconnected,
		eventCtx: eventCtx, eventCancel: eventCancel, events: make(chan func(), writeQueueSize),
		batches:     make(chan batchJob, writeQueueSize),
		batchBudget: mustNewByteBudget(clientBatchBudgetBytes),
		codec:       codec, closeDone: make(chan struct{}), debug: cfg.Debug,
		reconnectDelayFn: reconnectDelay,
		callbacks: callbacks{
			connection: cfg.OnConnectionStateChanged, disconnect: cfg.OnDisconnect,
			tokenExpired: cfg.OnTokenExpired, tokenWillExpire: cfg.OnTokenWillExpire,
		},
		wsDialer: &websocket.Dialer{HandshakeTimeout: connectTimeout, Proxy: http.ProxyFromEnvironment, EnableCompression: false}}
	c.connectEndpointFn = c.connectWithRedirects
	// Preserve 32 bits of per-Client randomness while reserving the top bit and
	// the low 31 bits for a non-wrapping monotonic sequence. This leaves at
	// least 2^63 automatically allocated IDs in every Client lifetime.
	c.idCounter.Store(initialMessageIDCounter(seed))
	c.eventWG.Add(1)
	go c.dispatchEvents()
	// Client 级固定 handler worker 池：所有连接代际共享，重连不再创建新 worker。
	// 这样即使 handler 忽略 context 永久阻塞，goroutine 数量也有固定上限，
	// 不会随重连次数跨代际累积。
	for i := 0; i < handlerConcurrency; i++ {
		go c.batchWorker()
	}
	return c, nil
}

func applyDefaultsAndValidate(c *Config) error {
	if c.Domain == "" {
		c.Domain = "easemob.com"
	}
	if c.Logger == nil {
		c.Logger = defaultLogger()
	}
	if c.MessageHandler == nil {
		return fmt.Errorf("MessageHandler is required")
	}
	if c.AppKey == "" || c.Resource == "" {
		return fmt.Errorf("AppKey and Resource are required")
	}
	c.Resource = resourcePrefix + c.Resource
	if len(c.Resource) > maxResourceLength || strings.ContainsAny(c.Resource, " \t\r\n/@") {
		return fmt.Errorf("Resource with SDK prefix must be 1-%d characters without whitespace, '/', '@'", maxResourceLength)
	}
	org, app, ok := strings.Cut(c.AppKey, "#")
	if !ok || org == "" || app == "" || strings.Contains(app, "#") || strings.ContainsAny(org+app, " \t\r\n/") {
		return fmt.Errorf("AppKey must have org#app form")
	}
	return nil
}

const (
	sdkVersion        = "4.0.0-go"
	resourcePrefix    = "go-server-imsdk-"
	maxResourceLength = 128
)

// 以下参数是 SDK 的固定值（不再暴露为 Config 配置项）。代码开源，如需调整请
// fork 后自行修改这些常量。
const (
	heartbeatInterval        = 120 * time.Second
	heartbeatTimeout         = 240 * time.Second
	connectTimeout           = 15 * time.Second
	sendTimeout              = 15 * time.Second
	logoutTimeout            = 5 * time.Second
	maxRedirectHops          = 5
	maxFrameBytes            = 4 << 20
	writeQueueSize           = 256
	handlerTimeout           = 30 * time.Second
	handlerMaxAttempts       = 3
	handlerConcurrency       = 4
	tokenExpiryWarningBefore = 5 * time.Minute
)

func (c *Client) LoginState() LoginState { c.mu.RLock(); defer c.mu.RUnlock(); return c.state }
func (c *Client) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connState == ConnStateConnected
}
func (c *Client) Health() HealthStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	h := HealthStatus{Connected: c.connState == ConnStateConnected, SessionID: c.sessionID, LoginState: c.state, ConnectionGeneration: c.generation.Load()}
	if n := c.lastInbound.Load(); n > 0 {
		h.LastInbound = time.Unix(0, n)
	}
	if c.run != nil {
		h.WriteBacklog = len(c.run.writes)
		h.QueueBacklog, h.KnownQueues, h.DeferredQueues, h.OutstandingPulls,
			h.QueueTrackedBytes, h.QueueCapacityRejects = c.run.queueHealth()
	}
	if c.lastErr != nil {
		h.LastError = c.lastErr.Error()
	}
	h.TokenExpiresAt = c.tokenExpiresAt
	h.StuckHandlers = int(c.stuckHandlers.Load())
	if c.batchBudget != nil {
		h.ClientBatchBacklogBytes = c.batchBudget.Used()
	}
	h.ProcessBatchBacklogBytes = processBatchBudget.Used()
	h.ProcessDecodeInFlightBytes = processDecodeBudget.Used()
	h.ClientBatchBudgetRejects = c.clientBatchRejects.Load()
	h.ProcessBatchBudgetRejects = c.processBatchRejects.Load()
	h.DecodeAdmissionTimeouts = c.decodeTimeouts.Load()
	h.DNSGeneration = c.dnsGeneration
	h.MSyncCandidateCount = len(c.msyncHosts)
	h.MSyncNextCandidate = c.msyncCursor
	return h
}

func (c *Client) UpdateToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("token is empty")
	}
	c.mu.Lock()
	c.token = token
	c.tokenExpiresAt = time.Time{}
	if c.tokenWarningCancel != nil {
		c.tokenWarningCancel()
		c.tokenWarningCancel = nil
	}
	c.mu.Unlock()
	return nil
}
func (c *Client) tokenValue() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.token }

// TokenExpiresAt returns the absolute expiry reported by PROVISION auth_token.
func (c *Client) TokenExpiresAt() (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tokenExpiresAt, !c.tokenExpiresAt.IsZero()
}

func (c *Client) callbackSnapshot() callbacks {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.callbacks
}

func initialMessageIDCounter(seed [4]byte) uint64 {
	return uint64(binary.BigEndian.Uint32(seed[:])) << 31
}

func (c *Client) nextMessageID() (uint64, error) {
	for {
		current := c.idCounter.Load()
		if current == math.MaxUint64 {
			return 0, newError(ErrMessageIDExhausted, "send", "automatic ClientMessageID space exhausted; provide a non-zero ClientMessageID")
		}
		next := current + 1
		if c.idCounter.CompareAndSwap(current, next) {
			return next, nil
		}
	}
}

func (c *Client) setStates(login LoginState, conn ConnState) {
	c.mu.Lock()
	changed := c.connState != conn
	c.state = login
	c.connState = conn
	cb := c.callbacks.connection
	userID := c.userID
	c.mu.Unlock()
	if changed && cb != nil {
		c.emit(func() { cb(userID, conn) })
	}
}
func safeCall(fn func()) { defer func() { _ = recover() }(); fn() }

func (c *Client) emit(fn func()) {
	select {
	case <-c.eventCtx.Done():
		return
	default:
	}
	select {
	case c.events <- fn:
	default:
		// Never let a slow application callback stall websocket I/O. The queue
		// is deliberately bounded; state can always be queried through Health.
		c.logger.Warn("callback queue full; dropping event")
	case <-c.eventCtx.Done():
	}
}

func (c *Client) dispatchEvents() {
	defer c.eventWG.Done()
	for {
		select {
		case fn := <-c.events:
			safeCall(fn)
		case <-c.eventCtx.Done():
			for {
				select {
				case fn := <-c.events:
					safeCall(fn)
				default:
					return
				}
			}
		}
	}
}
