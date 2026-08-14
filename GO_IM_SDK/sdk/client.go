package sdk

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
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
// HandlerTimeout 后取消，但取消是协作式的——SDK 无法强制终止一个忽略
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
	AppKey                    string
	Domain                    string
	Resource                  string
	HeartbeatInterval         time.Duration
	HeartbeatTimeout          time.Duration
	ConnectTimeout            time.Duration
	SendTimeout               time.Duration
	LogoutTimeout             time.Duration
	DisableReconnect          bool
	MaxRedirectHops           int
	MaxFrameBytes             int64
	WriteQueueSize            int
	HandlerTimeout            time.Duration
	HandlerMaxAttempts        int
	HandlerConcurrency        int
	TokenExpiryWarningBefore  time.Duration
	HTTPClient                *http.Client
	Logger                    *slog.Logger
	Telemetry                 Telemetry
	Debug                     bool
	MessageHandler            MessageHandler
	OnConnectionStateChanged  func(ConnState)
	OnDisconnect              func(error)
	OnTokenExpired            func()
	OnTokenWillExpire         func(time.Time)
	OnTokenRotated            func(string, int64)
	OnUserForbidden           func()
	OnUserRemoved             func()
	OnUserKickedByOtherDevice func(string, string)
	OnUserLoginAnotherDevice  func(string, string)
	OnServerNotice            func(string, []byte)
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
	Connected            bool       `json:"connected"`
	SessionID            string     `json:"session_id,omitempty"`
	LoginState           LoginState `json:"login_state"`
	ConnectionGeneration uint64     `json:"connection_generation"`
	LastInbound          time.Time  `json:"last_inbound"`
	WriteBacklog         int        `json:"write_backlog"`
	QueueBacklog         int        `json:"queue_backlog"`
	LastError            string     `json:"last_error,omitempty"`
	TokenExpiresAt       time.Time  `json:"token_expires_at,omitempty"`
	// StuckHandlers 是当前超过 HandlerTimeout 仍未返回的 handler 数量。
	// 持续大于 0 说明存在忽略 context 的 handler，应告警并定位修复。
	StuckHandlers int `json:"stuck_handlers"`
}

type callbacks struct {
	connection      func(ConnState)
	disconnect      func(error)
	tokenExpired    func()
	forbidden       func()
	removed         func()
	kicked          func(string, string)
	otherLogin      func(string, string)
	tokenRotated    func(string, int64)
	tokenWillExpire func(time.Time)
	notice          func(string, []byte)
}

type Client struct {
	cfg                Config
	logger             *slog.Logger
	mu                 sync.RWMutex
	connectMu          sync.Mutex
	state              LoginState
	connState          ConnState
	run                *connectionRun
	userID             string
	token              string
	msyncHost          string
	restBase           string
	sessionID          string
	generation         atomic.Uint64
	lastInbound        atomic.Int64
	backoffAttempt     atomic.Uint32
	lastStableConnect  atomic.Int64
	stuckHandlers      atomic.Int32
	lastErr            error
	tokenExpiresAt     time.Time
	tokenWarningCancel context.CancelFunc
	closed             bool
	idPrefix           uint64
	idCounter          atomic.Uint64
	callbacks          callbacks
	wsDialer           *websocket.Dialer
	eventCtx           context.Context
	eventCancel        context.CancelFunc
	events             chan func()
	batches            chan batchJob
	eventWG            sync.WaitGroup
	codec              internalprotocol.Codec
	debug              bool
}

func New(cfg Config) (*Client, error) {
	if err := applyDefaultsAndValidate(&cfg); err != nil {
		return nil, err
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("generate message id prefix: %w", err)
	}
	eventCtx, eventCancel := context.WithCancel(context.Background())
	codec, err := newProtocolCodec()
	if err != nil {
		eventCancel()
		return nil, fmt.Errorf("initialize protocol codec: %w", err)
	}
	c := &Client{cfg: cfg, logger: cfg.Logger, state: LoginStateLogout, connState: ConnStateDisconnected, idPrefix: binary.BigEndian.Uint64(seed[:]) & 0xffffffff00000000,
		eventCtx: eventCtx, eventCancel: eventCancel, events: make(chan func(), cfg.WriteQueueSize),
		batches: make(chan batchJob, cfg.WriteQueueSize),
		codec:   codec, debug: cfg.Debug,
		callbacks: callbacks{
			connection: cfg.OnConnectionStateChanged, disconnect: cfg.OnDisconnect,
			tokenExpired: cfg.OnTokenExpired, tokenWillExpire: cfg.OnTokenWillExpire,
			tokenRotated: cfg.OnTokenRotated, forbidden: cfg.OnUserForbidden,
			removed: cfg.OnUserRemoved, kicked: cfg.OnUserKickedByOtherDevice,
			otherLogin: cfg.OnUserLoginAnotherDevice, notice: cfg.OnServerNotice,
		},
		wsDialer: &websocket.Dialer{HandshakeTimeout: cfg.ConnectTimeout, Proxy: http.ProxyFromEnvironment, EnableCompression: false}}
	c.eventWG.Add(1)
	go c.dispatchEvents()
	// Client 级固定 handler worker 池：所有连接代际共享，重连不再创建新 worker。
	// 这样即使 handler 忽略 context 永久阻塞，goroutine 数量也有固定上限，
	// 不会随重连次数跨代际累积。
	for i := 0; i < cfg.HandlerConcurrency; i++ {
		go c.batchWorker()
	}
	return c, nil
}

func applyDefaultsAndValidate(c *Config) error {
	if c.Domain == "" {
		c.Domain = "easemob.com"
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 120 * time.Second
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 240 * time.Second
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 15 * time.Second
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = 15 * time.Second
	}
	if c.LogoutTimeout <= 0 {
		c.LogoutTimeout = 5 * time.Second
	}
	if c.MaxRedirectHops <= 0 {
		c.MaxRedirectHops = 5
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = 4 << 20
	}
	if c.WriteQueueSize <= 0 {
		c.WriteQueueSize = 256
	}
	if c.HandlerTimeout <= 0 {
		c.HandlerTimeout = 30 * time.Second
	}
	if c.HandlerMaxAttempts <= 0 {
		c.HandlerMaxAttempts = 3
	}
	if c.HandlerConcurrency <= 0 {
		c.HandlerConcurrency = 4
	}
	if c.TokenExpiryWarningBefore <= 0 {
		c.TokenExpiryWarningBefore = 5 * time.Minute
	}
	if c.Logger == nil {
		c.Logger = defaultLogger()
	}
	// 上限校验：防止误配极大值造成内存/goroutine 压力。
	if c.HandlerConcurrency > 64 {
		return fmt.Errorf("HandlerConcurrency must be between 1 and 64")
	}
	if c.WriteQueueSize > 1<<16 {
		return fmt.Errorf("WriteQueueSize must be between 1 and %d", 1<<16)
	}
	if c.MaxFrameBytes > 16<<20 {
		return fmt.Errorf("MaxFrameBytes must be between 1 and %d", 16<<20)
	}
	if c.HandlerTimeout < time.Second {
		return fmt.Errorf("HandlerTimeout must be at least 1s")
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
		h.QueueBacklog = c.run.queueBacklog()
	}
	if c.lastErr != nil {
		h.LastError = c.lastErr.Error()
	}
	h.TokenExpiresAt = c.tokenExpiresAt
	h.StuckHandlers = int(c.stuckHandlers.Load())
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

func (c *Client) nextMessageID() uint64 { return c.idPrefix | (c.idCounter.Add(1) & 0xffffffff) }

func (c *Client) setStates(login LoginState, conn ConnState) {
	c.mu.Lock()
	changed := c.connState != conn
	c.state = login
	c.connState = conn
	cb := c.callbacks.connection
	c.mu.Unlock()
	if changed && cb != nil {
		c.emit(func() { cb(conn) })
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
