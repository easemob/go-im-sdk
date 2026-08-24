# GO IM SDK — 项目计划

> 服务端场景的最小环信（msync 协议）Go 消息 SDK。
> 面向“客户服务端以单个 IM 用户身份长期在线”的无 UI 长连接场景，仅提供鉴权、可靠消息收发、
> 显式用户属性 REST 和少量群操作，不承担客户业务存储、会话模型或业务逻辑。
>
> 版本 v3：在 v2 协议核对基础上，补齐服务端使用所需的身份、发送 ACK、消费确认、并发、
> TLS、错误契约、可观测性和测试门禁。

---

## 1. 需求范围（已确认）

### 1.1 做

| 能力 | 说明 |
|---|---|
| 传输 | 仅安全 WebSocket（**wss**），path 默认 `/websocket`；不支持 ws/TCP |
| 鉴权 | `UserID + token` 由用户传入，wss 建联后 PROVISION 鉴权（`guid=JID(UserID, AppKey, Domain, Resource)`，`auth_token = {"token":"..."}`） |
| 消息类型 | 文本 / CMD / 自定义消息 |
| 消息场景 | 单聊、群聊、**群定向消息**（`directed_users` + `ROUTE_DIRECT`） |
| 群操作 | 仅**创建公开群 + 加入公开群 + 退出群**（REST）；公开群固定为无需审批、可直接加入 |
| 心跳 | UNREAD 保活（间隔/超时可配置；默认对齐 C++ 120s，另加超时判定） |
| 重连 | 自动重连（三段随机退避）+ REDIRECT 换地址重连（可配置开关） |
| 发送确认 | 发送后按 `Meta.id` 等待服务端 SYNC ACK，返回服务端消息 ID、时间戳或业务错误；支持调用方提供幂等消息 ID |
| 接收确认 | 客户消息处理器返回成功后才推进 queue 的 `next_key`；失败不确认，并按策略重试/重连 |
| 错误映射 | Go 网络错误按类型/错误链映射；协议层错误按 Status 枚举和兼容 reason 规则映射，保留原始 cause/status |
| 可观测性 | 可注入 slog + Telemetry/health/readiness hook；示例程序默认输出控制台，由运维收集 |
| 消息序列化 | Message 对象 → JSON（用户自行存储；SDK 只维护连接和内存状态，不持久化消息） |
| 会话生命周期事件 | token 过期 / 被服务端禁用 / 被强制踢出 / 被移除 / 其他设备登录 / 改密被踢；登录状态管理；连接状态变更回调 |
| 用户属性 | 仅显式调用：更新自己的属性、批量查询属性；不在消息中携带属性时间戳，不自动拉取，不向用户暴露属性变更 NOTIFY |

### 1.2 不做

- ws/TCP 传输、压缩（zlib/LZ4）、业务层加密（传输安全仅依赖 TLS）
- 会话、未读数、已读回执、撤回/编辑、消息缓存/存储（登录后的**离线/断线积压消息**已由 SDK 通过 UNREAD 拉取并经 `MessageHandler` 投递，见 1.1）
- 好友、聊天室、presence、thread、reaction、翻译、推送
- 群成员管理（加人/拉人/申请/邀请/审批/踢人/改群设置）—— 由**用户后台 REST** 完成
- 登录相关 REST（token 由用户传入）
- 群名片（namecard）、群成员属性（不涉及）

### 1.3 外部依赖（运维侧，SDK 无代码）

- 多 client 共用出口 IP → 服务端加白
- DNS config 不预埋 → 用户初始化传入 msync host+port 与 rest 地址
- 客户保证同一个 UserID 同一时刻只由一个服务实例登录；SDK 不负责多实例选主、租约或互踢规避
- 不设置/持久化 DeviceUUID；当前服务端单活场景不依赖设备身份
- 服务端不需要会话/未读业务模型；**登录后的离线/断线积压消息由 SDK 通过 UNREAD 拉取**并经 `MessageHandler` 投递，与在线消息同一条链路，无需客户后台额外拉取

---

## 2. 协议与 PB

### 2.1 保留的 proto（5 个，字段编号不可改动）

| 文件 | 用途 |
|---|---|
| `msync.proto` | 信封 MSync + Meta + Provision + Status + CommSyncUL/DL + UnreadUL/DL + CommNotice |
| `messagebody.proto` | 消息体（Content.Type: TEXT / COMMAND / CUSTOM） |
| `jid.proto` | JID（app_key / name / domain / client_resource） |
| `keyvalue.proto` | ext / CMD params / 自定义字段（KeyValue 8 种值类型） |
| `statisticsbody.proto` | 会话生命周期事件（ns=STATISTIC 下行：被移除/其他设备登录/改密被踢/被其他设备踢） |

不使用：`rosterbody` / `mucbody` / `conferencebody` / `gateway` / `argus*`。

**生成注意：** 5 个 proto 均无 `go_package` 选项，除 msync.proto 外无 `syntax` 行（隐式 proto2，Go 侧为指针语义）。
生成时必须用 `--go_opt=M<file>.proto=<path>` 显式指定输出路径（详见 M1 实施）。

### 2.2 核心流程（重点修订：收消息链路）

#### 2.2.1 传输帧格式（P0-3，最容易踩的坑）

```
WebSocket 上一个二进制帧 = 一个裸 MSync protobuf（无任何长度前缀），帧边界即消息边界。
只有 TCP 直连才需要 4 字节大端长度前缀——本 SDK 仅 WS，不涉及长度头。

依据：C++ 共享 sendBuffer 统一加 4 字节长度前缀，但 WS transport 发送时剥掉
（src/emwebsocketchattransport.cpp:69-73），接收时人为补回前缀再喂共享解析器（同文件 96-113）。
Go 实现：conn.WriteMessage(Binary, msyncBytes) / conn.ReadMessage() 得到裸 MSync，直接 marshal/unmarshal。
```

#### 2.2.2 登录与收消息完整时序（P0-1 / P0-2 修订）

```
1. wss 建联
2. MSync{command: PROVISION, guid: JID, payload: Provision{os_type, version, auth_token:{"token":"..."}}}
   → 服务端回 Provision{status, resource, compress_type, session_id, auth_token(可能刷新)}
   → status==OK → 登录成功；status==REDIRECT → 在 hop/环/总时限约束内选择 redirect_info 重连重登
   → auth_token 非空时解析 token/expires_in，原子替换 Client 当前 token，再触发 OnTokenRotated 供上层持久化
3. 【登录后首个保活】登录成功后发一次 MSync{command: UNREAD}（空 UnreadUL，兼作首个心跳）
   → 服务端回 UnreadDL{unread[]: MetaQueue{queue, n}}
   → 【离线/积压消息：拉取】该队列列表是积压消息入口，对每个 queue 调 syncQueue(queue)（与第 4 步同路径），
     把离线消息经 MessageHandler 投递；仅当 status==REDIRECT 时按重定向处理，其余错误按协议错误处理
4. 【NOTICE 触发路径，P0-1】运行中收到 MSync{command: NOTICE, payload: CommNotice{queue}}
   → 该队列有新内容，调 syncQueue(queue) 主动拉取（与第 3 步同路径）
5. 心跳保活：定时发 MSync{command: UNREAD}（空 UnreadUL），收到 UNREAD 下行视为 pong（更新保活时间）
   → 【注意】心跳 UNREAD 下行可能带 unread 队列列表（积压入口），同样对每个 queue 调 syncQueue 拉取；
     基于队列 cursor(key) 去重，稳态下无重投，兼作断线/心跳期间的持续兜底
   → 【在线投递通道】在线新消息通过 NOTICE 触发拉取（见第 4 步）；UNREAD 队列列表则用于登录及兜底的离线拉取
6. 发送：MSync{command: SYNC, payload: CommSyncUL{meta}}
   → 按 Meta.id 等待服务端下行 ACK：CommSyncDL{meta_id=上行id, server_id, timestamp, status}
   → 【发送成功定义】收到对应 meta_id 的 ACK 即完成：server_id 即服务端消息 ID，
     随 SendResult.ServerMessageID 返回；ACK status 非 OK 时返回对应业务错误
   → 超时/断线返回可判定错误，调用方决定是否用同一幂等 ID 重试
7. 登出：MSync{command: LOGOUT, payload: Logout{session_id, reason}} → 等服务端 LOGOUT 回复后关闭
```

读循环分发总表：

| MSync.Command | 下行处理 |
|---|---|
| PROVISION | 登录结果（等待中的 login channel） |
| SYNC | `meta_id>0`：发送 ACK；否则按 metas[] 分发 CHAT/STATISTIC；MUC 和不需要的 NOTIFY 丢弃；批次处理完成后推进 next_key |
| UNREAD | 先检查 Status；OK → 更新保活（pong），并对 **unread 队列列表逐个 syncQueue 拉取离线消息**（基于 cursor 去重、幂等）；REDIRECT → 受限切址；其他错误按协议错误处理 |
| NOTICE | CommNotice{queue} → syncQueue(queue) |
| LOGOUT | 登出结果 |

`syncQueue(queue)` 定义：`MSync{command: SYNC, payload: CommSyncUL{queue: queue, key: 0 或 next_key}}`。
同一 queue 只允许一条拉取链：NOTICE 的重复 key=0 触发必须合并（single-flight）；next_key 严格串行；
旧 connection generation 的下行不得推进新连接的 queue 状态。

#### 2.2.3 会话生命周期事件（ns=STATISTIC 下行，对齐 C++ handleStatistic）

```
StatisticsBody.operation:
  INFORMATION(0)              → 忽略（离线群已读回执数量，本 SDK 不消费）
  USER_REMOVED(1)             → OnUserRemoved           （用户被服务端移除）
  USER_LOGIN_ANOTHER_DEVICE(2)   → OnUserLoginAnotherDevice（session_id 只用于诊断，收到事件一律断开）
  USER_KICKED_BY_CHANGE_PASSWORD(3) → 断连不重连，OnDisconnect(改密被踢)（P1-5：不能忽略，服务端可能发）
  USER_KICKED_BY_OTHER_DEVICE(4) → OnUserKickedByOtherDevice（被其他设备强制踢出）
以上事件触发后进入不可重连的断开状态（业务性断开，不自动重连）
```

#### 2.2.4 NOTIFY 分发

```

#### 2.2.5 MUC 事件

`ns=MUC` 的下行事件全部丢弃，不解析 `mucbody.proto`，不提供群事件回调，也不据此维护本地群状态。
MUC meta 被丢弃后继续处理同一 CommSyncDL 批次中的其他 meta；不得阻断 queue，也不得单独触发业务回调。
群创建、加入和退出结果仅以对应 REST 响应为准。
群聊消息本身属于 `ns=CHAT + MessageBody.type=GROUPCHAT`，仍按普通消息进入 MessageHandler；丢弃 MUC 不影响群消息收发。
ns=NOTIFY payload 为 JSON，按 type 字段做最小分发：
  type ∈ {"user_metadata_updated", "subscribe_metadata_updated", "contact_metadata_updated"}
      → 明确忽略；服务端 Go SDK 用户不需要感知用户属性/联系人属性变化
  其他 → OnServerNotice(ns, payload)，供诊断或未来兼容；未注册 handler 时忽略
```

### 2.3 消息构建要点

- 单聊：`MessageBody.type=CHAT`，`to.name=对方用户名`
- 群聊：`MessageBody.type=GROUPCHAT`，`to.name=群id`
- 群定向：`Meta.directed_users=[用户列表]` + `Meta.routetype=ROUTE_DIRECT`
- JID：`app_key="org#app"`，`domain=easemob.com`（可配置）
- 发送时不设置 `MessageBody.userinfo_update_time`
- **Meta.id：** 允许调用方传入非零 `ClientMessageID` 作为幂等键；未传时由 SDK 使用进程级 atomic
  计数器 + 安全随机启动前缀生成。SDK 默认不自动重发结果不确定的消息；调用方重试必须复用原 ID

### 2.4 用户属性（userinfo）机制

```
更新自己属性: PUT {rest}/metadata/user/{UserID}（form-urlencoded）→ Response/APIError
批量查询:     POST {rest}/metadata/user/get（{"targets":[],"properties":[]}）→ Response/APIError

SDK 不缓存属性和 updateTime，不设置/比较 userinfo_update_time，不自动发起 FetchUserInfo，
也不向调用方分发用户属性变更 NOTIFY。属性数据是否存储、何时刷新完全由客户业务决定。
```

---

## 3. 架构设计

```
┌─ SDK 纯库（初始化传参，无持久化状态）─────────────┐
│  New(Config) → Connect(ctx) → 收发消息 → Close   │
└──────────────────────────────────────────────────┘
                    ▲ 组装 Config
┌─ 可执行程序 cmd/server（部署层）──────────────────┐
│  main() 读 config 文件 → 校验 → 起 SDK → 示例回调  │
└──────────────────────────────────────────────────┘
                    ▲
┌─ 部署层 ─────────────────────────────────────────┐
│  config.yaml + start.sh -c x.yaml [-d] + stop.sh │
└──────────────────────────────────────────────────┘
```

**职责划分：**
- SDK 层：只做协议/连接/消息，**不读配置文件**（调用方掌控一切）
- 部署层：配置文件承载 token/地址/参数，运维改配置不碰代码
- SDK 维护必要的内存状态：登录/session、连接 generation、发送等待表、queue 拉取状态和计时器
- 单个 Client 绑定一个 UserID；客户保证同一 UserID 单实例登录

---

## 4. 公开 API 设计

### 4.1 Config（初始化传参）

```go
type Config struct {
    MsyncHost         string        // 必须为 wss://host:port/websocket（path 可省略）
    RestBase          string        // 必须为 https://host:port/org/app（无尾斜杠）
    AppKey            string        // "org#app"，必填
    UserID            string        // 当前 IM 用户，必填
    Token             string        // 与 UserID/AppKey 绑定的用户 token，必填
    Domain            string        // 默认 "easemob.com"
    Resource          string        // 必填；用户持久化，同一实例重启复用，并行实例必须不同
    SDKVersion        string        // 默认 "4.0.0-go"
    HeartbeatInterval time.Duration // 默认 120s
    HeartbeatTimeout  time.Duration // 默认 240s
    ConnectTimeout    time.Duration // 默认 15s
    SendTimeout       time.Duration // 等待服务端 ACK，默认 15s
    LogoutTimeout     time.Duration // 默认 5s
    DisableReconnect  bool          // 零值表示开启自动重连
    MaxRedirectHops   int           // 单次连接默认最多 5 次，并检测环
    MaxFrameBytes     int64         // 默认值由实现确定并写入 README
    WriteQueueSize    int           // 有界写队列容量
    HandlerTimeout    time.Duration // 单批消息处理超时
    HandlerMaxAttempts int          // handler 最大尝试次数，耗尽后断开重拉
    HandlerConcurrency int          // 不同 queue 的最大处理并发；同 queue 始终串行
    HTTPClient        *http.Client  // REST 用；nil 时创建带完整超时的安全默认 client
    Logger            *slog.Logger  // 可选；nil 使用安全默认 logger
    Telemetry         Telemetry     // 可选的指标/请求上报 hook
    MessageHandler    MessageHandler// 必填；成功返回后才确认 next_key
}
```

> 心跳说明（P1-6 修订）：C++ 心跳间隔为 120s（src/emclient.cpp:15），且是 fire-and-forget，
> 没有"收不到 pong 就断开"的逻辑。本 SDK 的 HeartbeatTimeout（默认 240s = 2 个心跳周期）
> 是**有意新增的保活策略**，用于服务端长连接场景更快发现死链，非对齐 C++。

### 4.2 Client

```go
type MessageHandler func(ctx context.Context, msg *Message) error
type Telemetry interface {
    Record(ctx context.Context, event TelemetryEvent)
}

func New(cfg Config) (*Client, error)
func (c *Client) Connect(ctx context.Context) error  // ctx 控制本次登录；阻塞到 PROVISION 成功/失败
func (c *Client) Close(ctx context.Context) error    // 幂等：停止接单、取消后台任务、等待 goroutine 收敛
func (c *Client) Logout(ctx context.Context) error   // 发 LOGOUT（带 session_id）→ 服务端确认或超时后 Close
func (c *Client) UpdateToken(token string) error     // 刷新内存中 token，供后续重连/重登录使用；不持久化、不主动刷新

// 登录/连接状态查询
func (c *Client) LoginState() LoginState              // Logout / LoggingIn / LoggedIn / Reconnecting
func (c *Client) Connected() bool
func (c *Client) Health() HealthStatus // connection generation、last inbound、queue/write backlog、last error

// 事件回调在独立的有界 dispatcher 中调用；不得阻塞 readPump
func (c *Client) OnServerNotice(fn func(ns string, payload []byte)) // 服务端 NOTIFY 通知（非 userinfo 类），可选

// 会话生命周期回调
func (c *Client) OnConnectionStateChanged(fn func(state ConnState))  // 连接状态变更
func (c *Client) OnDisconnect(fn func(err error))                    // 断开原因（含改密被踢等）
func (c *Client) OnTokenExpired(fn func())                           // token 过期（登录期 FAIL+reason 或运行中通知）
func (c *Client) OnUserForbidden(fn func())                          // 用户被服务端禁用（IM_FORBIDDEN）
func (c *Client) OnUserRemoved(fn func())                            // 用户被服务端移除
func (c *Client) OnUserKickedByOtherDevice(fn func(device, reason string)) // 被其他设备强制踢出
func (c *Client) OnUserLoginAnotherDevice(fn func(device, reason string))  // 其他设备登录
func (c *Client) OnTokenRotated(fn func(token string, expiresIn int64)) // PROVISION 返回刷新 token 时通知上层持久化

// 用户属性：只在用户显式调用时请求，不缓存、不自动拉取、无属性变更回调
func (c *Client) UpdateOwnUserInfo(ctx context.Context, attrs map[string]string) (*Response, error)
func (c *Client) FetchUserInfo(ctx context.Context, users, properties []string) (*Response, error)

type SendRequest struct {
    ClientMessageID uint64       // 可选；业务重试必须复用同一 ID
    To              string
    IsGroup         bool
    DirectedUsers   []string     // 非空时要求 IsGroup=true，并使用 ROUTE_DIRECT
    Body            MessageBody  // text / command / custom
}
type SendResult struct {
    ClientMessageID uint64 // 上行携带的幂等 ID
    ServerMessageID uint64 // 服务端消息 ID（ACK 中的 server_id）
    ServerTimestamp uint64 // 服务端时间戳
}
func (c *Client) Send(ctx context.Context, req SendRequest) (*SendResult, error)

type Response struct {
    StatusCode int
    Header     http.Header
    Body       []byte
}
type APIError struct {
    Response    *Response
    ServiceCode string
    RequestID   string
    RetryAfter  time.Duration
    Cause       error
}

// 仅创建公开、无需审批、可直接加入的群；public=true、membersonly=false 固定写入请求
type CreatePublicGroupOptions struct {
    AllowInvites       *bool  // nil 默认 true
    InviteNeedConfirm  *bool  // nil 默认 false
    MaxUsers           int    // 透传给服务端；SDK 不校验上限（成员上限一般 500，由服务端决定）
    Description        string
    Welcome            string
    Members            []string
}
func (c *Client) CreatePublicGroup(ctx context.Context, name string, opt CreatePublicGroupOptions) (*Response, error)
func (c *Client) JoinPublicGroup(ctx context.Context, groupID string) (*Response, error) // 直接 apply，不做预检；结果以服务端响应为准
func (c *Client) LeaveGroup(ctx context.Context, groupID string) (*Response, error)
```

**登录/连接状态机：**

```
LoginState: Logout → LoggingIn → LoggedIn（可 Reconnecting）→ Logout
ConnState:  disconnected / connecting / connected / reconnecting
```

**重连策略（P1-7 修订，对齐 C++ emsessionmanager.cpp:932-950，三段随机非指数退避）：**
- 网络性断开（DNS/超时/IoError/StreamClosed）→ 自动重连（可配置开关）：
  - attempts ≤ 3 → 随机 5~10s
  - 3 < attempts < 9 → 随机 20~40s
  - attempts ≥ 9 → 随机 60~120s
- 业务性断开（被踢/被移除/被禁用/token 过期/登录失败）→ **不自动重连**，触发对应回调后置为 Logout，由用户上层决定是否用新 token 重新 Connect
- REDIRECT endpoint 只接受合法 host + port；继承原 wss scheme 和 `/websocket` path，不接受服务端下发任意 URL 或 TLS 降级

**并发与背压模型：**

- 一个 `readPump` 独占 WebSocket 读取；设置 `MaxFrameBytes`，畸形/超大 protobuf 不得进入业务回调
- 一个 `writePump` 独占 WebSocket 写入；Send、UNREAD、SYNC next_key、LOGOUT 全部进入有界写队列
- 同一 queue 的 handler 严格串行；不同 queue 可在受控并发度内并行
- 一个 CommSyncDL 批次中的 CHAT metas 全部处理成功后才提交 next_key；任一 handler 返回错误或 panic 时整批不确认，
  并按有上限策略重试，耗尽后断开以便重拉。客户 handler 必须按 MetaID 做持久化幂等
- 发送等待表按 ClientMessageID 关联 ACK；断线时取消当前等待，由调用方基于错误类型决定是否复用 ID 重试
- `Close`/`Logout` 幂等；关闭顺序为停止接受新请求 → 取消后台任务 → 有界 drain → 关闭连接 → 等待 goroutine 退出
- 回调、日志和 telemetry 不得运行在 readPump/writePump 上；队列满时采用背压，不静默丢消息
- MessageHandler 和事件回调在 `Connect` 前注册；一个事件最多一个 handler，运行中替换/注销不在 MVP 支持范围

### 4.3 消息模型（→ JSON，用户自行存储）

```go
type Message struct {
    From      string                 `json:"from"`
    To        string                 `json:"to"`
    IsGroup   bool                   `json:"is_group"`
    MetaID    uint64                 `json:"meta_id"`
    Timestamp uint64                 `json:"timestamp"` // 服务端毫秒时间戳
    Bodies    []*MessageBody         `json:"bodies"`
    Ext       map[string]KeyValue    `json:"ext,omitempty"` // 消息扩展（P2 修订：类型化，见下）
}
type MessageBody struct {
    Type       MessageBodyType       `json:"type"`               // text / command / custom
    Text       string                `json:"text,omitempty"`
    Action     string                `json:"action,omitempty"`   // command
    Params     map[string]KeyValue   `json:"params,omitempty"`   // command
    Event      string                `json:"event,omitempty"`    // custom
    CustomExts map[string]KeyValue   `json:"custom_exts,omitempty"` // custom
    RawType    int32                 `json:"raw_type,omitempty"` // 未支持/未来类型
    RawPayload []byte                `json:"raw_payload,omitempty"`
}

// P2 修订：接收侧 KeyValue 类型化（8 种协议值类型）
// iOS/Android 发来的带类型属性（BOOL/INT/UINT/LLINT/FLOAT/DOUBLE/STRING/JSON_STRING）
// 不再被降级为 string；发送侧仍只接受 string（业务 JSON 用户自解析）
type KeyValue struct {
    Type  KeyValueType `json:"type"`
    Value any          `json:"value"` // string / int64 / uint64 / float64 / double / bool
}
```

- 全部带 json tag，用户 `json.Marshal(msg)` 直接落库/转发
- `Message.Ext` 明确来自 `MessageBody.ext`；`Meta.ext` 作为协议路由扩展单独处理，不混入业务消息扩展
- KeyValue 自定义 JSON 编码：LLINT/UINT 使用十进制字符串并保留 Type，避免超过 JavaScript `2^53` 后丢精度
- 未支持或未来的消息 body 透出 RawType/RawPayload，不得因单条未知消息阻断整个 queue
- SDK **不缓存消息、不落库、不维护客户业务会话模型**；仅维护协议运行所需的内存状态

---

## 5. 错误处理与映射

### 5.1 wss 连接错误

| Go 错误类型/场景 | 映射错误 |
|---|---|
| `net.DNSError` / DNS 解析失败 | DnsError |
| WebSocket 正常/异常关闭、EOF | StreamClosed |
| `tls` / `x509` 验证或握手失败 | TlsFailed |
| `context.DeadlineExceeded` / `net.Error.Timeout()` | Timeout |
| WebSocket HTTP 握手非 101 | HandshakeError（保留 HTTP 状态） |
| 其他连接失败 | IoError |

实现必须使用 `errors.Is/errors.As` 和 WebSocket close code 分类，保留原始 cause；不得复制
libwebsockets 的英文错误字符串做全等比较。

【兜底策略】Go 错误类型分类为主；对分类不确定/无法匹配的错误场景，保留 C++ 侧 reason 文案
子串匹配作为辅助分类，两条路径的映射结果取更具体者。

### 5.2 登录期错误映射（P1-4 重写，对齐 C++ handleProvision）

**关键：TOKEN_EXPIRED(6) 不是按状态码判的！** C++ 的 handleProvision switch 没有 case TOKEN_EXPIRED，
6 会落进 default → 认证失败。真实的 token 判定挂在 FAIL(1) 分支下的 **reason 子串匹配**（逐字复刻）：

```
FAIL(1) + reason 子串匹配（contains，顺序敏感）：
  "Sorry, token expired"                                 → OnTokenExpired
  "Sorry, token or password does not match login info"   → OnDisconnect(INVALID_TOKEN)
  "Sorry, user not found"                                → OnDisconnect(USER_NOT_FOUND)
  "Sorry, user register limit" / "Sorry, user register rate limit" → OnDisconnect(注册数超限)

状态码分支：
  OK(0)              → 登录成功（随后发首个 UNREAD 保活）
  FAIL(1)            → 见上（reason 不命中任何子串 → 认证失败）
  UNAUTHORIZED(2)    → OnDisconnect(认证失败)
  MISSING_PARAMETER(3) → OnDisconnect(缺少参数)
  WRONG_PARAMETER(4) → OnDisconnect(参数错误)
  REDIRECT(5)        → 从 redirect_info 选择 endpoint 重连；受 MaxRedirectHops、环检测和 Connect ctx 总时限约束
  IM_FORBIDDEN(12)   → OnUserForbidden（用户被禁用），不重连
  BIND_ANOTHER_DEVICE(11) → OnDisconnect(USER_BIND_ANOTHER_DEVICE)，不重连
  TOO_MANY_DEVICES(13)    → OnDisconnect(USER_LOGIN_TOO_MANY_DEVICES)，不重连
  RESOURCE_CHANGED(20)    → OnDisconnect(USER_DEVICE_CHANGED)，不重连
  PERMISSION_DENIED(7)    → reason 命中三个 live-count 文案时分类为 APP_ACTIVE_LIMIT；
                            其他情况返回通用 PERMISSION_DENIED。两者均断开并回调，禁止静默挂起
  ENCRYPT_DISABLE/ENABLE/DECRYPT_FAILURE → 本 SDK 无加密，理论上不会出现；若出现按认证失败处理并告警日志
  default              → OnDisconnect(认证失败)
```

### 5.3 运行中会话事件（ns=STATISTIC 下行，对齐 C++ handleStatistic）

| StatisticsBody.operation | 事件 | 是否重连 |
|---|---|---|
| USER_REMOVED(1) | OnUserRemoved | 否 |
| USER_LOGIN_ANOTHER_DEVICE(2) | OnUserLoginAnotherDevice（session_id 仅用于诊断；无论匹配与否均断开） | 否 |
| USER_KICKED_BY_CHANGE_PASSWORD(3) | **处理**：断连不重连，OnDisconnect(改密被踢)（P1-5：不可忽略） | 否 |
| USER_KICKED_BY_OTHER_DEVICE(4) | OnUserKickedByOtherDevice | 否 |

### 5.4 用户属性 REST

| 接口 | 路径/方法 | body | 错误码（对齐 C++） |
|---|---|---|---|
| 更新自己属性 | `PUT {rest}/metadata/user/{username}` | form-urlencoded（key=value&…） | 404→USER_NOT_FOUND、401→AUTH_FAILED、403→DATALENGTH_EXCEED、429→EXCEED_SERVICE_LIMIT |
| 批量查询 | `POST {rest}/metadata/user/get` | `{"targets":[],"properties":[]}` | 404→USER_NOT_FOUND、401→AUTH_FAILED、400→USERCOUNT_EXCEED、429→EXCEED_SERVICE_LIMIT |

所有 REST API 返回 `Response`；非 2xx 返回带 `StatusCode/ServiceCode/RequestID/RetryAfter` 的 `APIError`。
网络、TLS、context cancel、请求序列化、响应读取和响应体超限同样返回 error。调用方仍可从
`APIError.Response` 访问受大小限制的原始 body。

### 5.5 公开群 REST

| 接口 | 路径/方法 | 说明 |
|---|---|---|
| 创建公开群 | `POST {rest}/chatgroups?version=v3&resource={deviceResource}` | body 中固定 `owner=Config.UserID`、`public=true`、`membersonly=false`；其余为 name/description/maxusers/members/allowinvites/invite_need_confirm |
| 加入公开群 | `POST {rest}/chatgroups/{groupId}/apply?version=v3&resource={deviceResource}` | 直接 apply（空 body）；公开性/审批/容量/群数量均不做本地判断，由服务端决定；对齐 C++ `mucApply` |
| 离开群 | `DELETE {rest}/chatgroups/{groupId}/quit?version=v3&resource={deviceResource}` | |

- 鉴权头：`Authorization: Bearer <token>`
- `resource` 参数 = Config.Resource（deviceResource）
- 所有路径参数使用 `url.PathEscape`；请求/响应 body 有大小上限并正确关闭响应体
- 加入接口不维护本地群缓存，不预检群详情；公开性/审批/容量/群数量上限均由服务端决定，SDK 不做任何本地判断
- 群详情/成员列表等查询**不封装**——客户直接使用 REST API 自行拉取
- 创建群时 maxusers 透传不校验（成员上限一般 500，由服务端决定）；群容量/群数量上限同理由服务端决定
- 加入操作记录 request telemetry，整体 JoinPublicGroup 再记录一次 operation telemetry
- 429 解析 `Retry-After`，但 SDK 不自动重试创建群、加入群等结果不确定的写请求
- 每次 REST 请求通过 Telemetry 上报 operation、attempt、耗时、HTTP/服务错误码；token 和 body 默认脱敏

---

## 6. 文件结构

```
.
├── go.mod / go.sum          module: 发布前确定的正式可引用路径（禁止使用裸 `emgosdk`）
├── README.md                使用/部署文档
├── PLAN.md                  本计划
├── sdk/
│   ├── log.go               控制台日志（slog 薄封装）
│   ├── errors.go            错误定义与映射（含 FAIL+reason 子串表）
│   ├── client.go            Config / Client / 主 API
│   ├── connection.go        wss + PROVISION + 首个 UNREAD 保活 + readPump/writePump + 心跳 + 有界重连
│   ├── queue.go             per-queue single-flight + handler 消费确认 + next_key 推进
│   ├── message.go           消息构建/解析（含群定向、发送 ACK、KeyValue 类型化、JSON tag）
│   └── rest.go              公开群创建/加入/退出、显式用户属性 REST + typed APIError + reporting
├── cmd/server/main.go       读配置 → 起 SDK → 示例消息处理（用户可自行替换）
├── config.example.yaml      配置模板（token/host/rest/appkey/心跳参数）
├── start.sh                 一键启动：start.sh -c prod.yaml [-d 守护]（写 pidfile）
├── stop.sh                  停止：stop.sh -c prod.yaml（SIGTERM → 优雅 Logout+Close）
└── test/                    离线测试（mock ws server，不依赖真实服务端）
```

---

## 7. 配置与部署

### 7.1 config.example.yaml（模板）

```yaml
# msync websocket 地址（用户初始化时设置，DNS config 不预埋）
msync_host: "wss://msync.example.com:443/websocket"
# REST 基础地址（含 org/app），公开群创建/加入/退出及用户属性用
rest_base: "https://rest.example.com:443/org/app"
# 环信 appkey，形如 org#app
app_key: "org#app"
# 当前登录的 IM 用户；客户保证同一 user_id 只有一个服务实例在线
user_id: ""
# 登录 token（用户传入）
token: ""
# 可选参数
domain: "easemob.com"
resource: "go-service-instance-01"
sdk_version: "4.0.0-go"
heartbeat_interval_seconds: 120   # 对齐 C++ 默认
heartbeat_timeout_seconds: 240    # 有意新增的保活超时（2 个心跳周期）
disable_reconnect: false
connect_timeout_seconds: 15
send_timeout_seconds: 15
logout_timeout_seconds: 5
max_redirect_hops: 5
max_frame_bytes: 4194304
write_queue_size: 256
handler_timeout_seconds: 30
handler_max_attempts: 3
handler_concurrency: 4
```

生产配置只接受 `wss://` 和 `https://`。由于公开 API 使用 `slog`，最低 Go 版本为 1.21。
示例程序优先从环境变量或 secret file 读取 token；
若使用 YAML，文件权限必须为 `0600`。日志、错误和 telemetry 禁止输出 token、Authorization 或完整消息 payload。

### 7.2 启动/停止

```
./start.sh -c prod.yaml        # 前台启动
./start.sh -c prod.yaml -d     # 后台守护（写 pidfile）
./stop.sh -c prod.yaml         # SIGTERM → 优雅 Logout + Close
```

---

## 8. 实施顺序

> 实施状态（2026-08-12）：M0–M5 的开发版本均已完成，并已通过 Linux amd64/arm64 Go 1.21、race、vet、native smoke、ASan/UBSan 与净化发布包验证。客户目标基线已确认：glibc 2.28、GCC 8.5.0，Clang 18.1.8 可作为兼容编译器。正式发布前必须在不高于 glibc 2.28 的环境重建制品、检查 GLIBC/GLIBCXX 符号，并完成 GA 级 fuzz/soak/benchmark 门禁。代码尚未上传 GitHub。

| 里程碑 | 内容 | 依赖 |
|---|---|---|
| M0 契约与夹具 | 固化单 UserID 单活、发送 ACK、消费确认、TLS、错误模型；pb 生成和版本/hash 校验；搭建 mock wss server | 无 |
| M1 身份与连接 | UserID/JID、wss、PROVISION、token 更新、首个 UNREAD 保活、readPump/单 writePump、登录状态机、幂等 Close/Logout | M0 |
| M2 可靠消息垂直切片 | 发送 ACK/超时/幂等 ID；NOTICE/UNREAD；per-queue single-flight；handler 成功后 next_key；消息解析、群定向、KeyValue/JSON | M1 |
| M3 韧性与运维 | 心跳/死链、三段随机重连、REDIRECT 上限/环检测、连接 generation、背压、telemetry、health/readiness hook | M2 |
| M4 REST 层 | 创建公开群/加入公开群/退出群、显式用户属性更新/查询、typed APIError、限流信息、request/operation reporting | M1 |
| M5 部署与发布 | cmd/server 示例、secret 注入、config、启停脚本、README、Go 版本/平台矩阵、semver、许可证/SBOM | M3+M4 |

---

## 9. 测试策略

- **基础门禁**：`go test ./...`、`go test -race ./...`、`go vet ./...` 和选定 lint；每个里程碑同步补测试，不集中到 M5
- **单元**：消息构建/解析、全部 Status 枚举、FAIL+reason 兼容表、KeyValue 边界 JSON、幂等 ID、URL/TLS 配置校验
- **集成**：本地 mock wss server 模拟 PROVISION、UNREAD、NOTICE、CommSyncDL、发送 ACK、心跳、REDIRECT、LOGOUT
- **可靠性**：handler 成功/失败/panic/超时；确认前后断进程；重复 NOTICE；同 queue 顺序；不同 queue 并发；旧 connection response
- **并发**：并发 Send/Close/Logout、写队列满、慢 handler、断线时 pending ACK、goroutine 泄漏，必须通过 race detector
- **网络与安全**：TLS/证书失败、网络半开、超大/截断/畸形 PB、未知 enum/body、redirect 环/空地址/降级尝试
- **REST**：公开群创建固定 public=true/membersonly=false；加入公开群/重复加入/非公开群/审批群/群满；退出群/重复退出；以及 context cancel、超时、401/403/404/429+Retry-After、大响应、路径转义、敏感字段脱敏和 telemetry；容量/群数量不本地判断，以服务端返回为准
- **回归重点**：登录成功后必须发首个 UNREAD 保活；UNREAD.status=REDIRECT 必须切址；handler 未成功不得推进 next_key；UNREAD 下行队列列表须对每个 queue 触发 syncQueue 拉取离线消息（幂等、基于 cursor 去重）
- 全部测试离线运行，不依赖真实服务端；解析器增加 fuzz test

---

## 10. 决策记录（多轮确认 + C++ 对照修订）

1. 产品定位为“客户服务端以单个 UserID 长期在线”的 Headless/UserSession SDK，不是 app credential 管理 SDK
2. 客户保证同一 UserID 单实例登录；SDK 不设计选主/租约/互踢规避，也不设置 DeviceUUID
3. 客户公网部署：仅支持 wss/https，不支持 ws/TCP；**WS 帧 = 裸 MSync，无长度前缀**
4. 无压缩、无业务层加密，TLS 证书校验不可关闭
5. UserID + token 由用户传入，不内置登录 REST；wss 建联后通过 JID + PROVISION 鉴权
6. Send 等待服务端按 Meta.id 下行的 ACK（含 server_id 服务端消息 ID）即成功；ACK status 非 OK 返回业务错误；
   调用方重试必须复用 ClientMessageID
7. 消息 handler 成功后才推进 queue 的 next_key；失败/panic 不确认，不允许静默丢消息
8. 收消息链路：在线实时投递走 NOTICE → queue single-flight → CommSyncDL → handler → next_key；
   登录后发首个 UNREAD 兼作保活，其下行队列列表（离线/积压入口）对每个 queue 走同一 syncQueue 拉取，
   把离线消息经 handler 投递（基于 cursor 去重、幂等，心跳期间兜底）
9. 一个 readPump + 一个 writePump；所有 WS 写串行，回调/telemetry 与 I/O pump 隔离并使用有界队列
10. wss 错误按 Go 错误类型和错误链分类（不复制 libwebsockets 错误字符串）；分类不确定时保留 C++ reason 文案匹配兜底
11. REDIRECT 同时处理 PROVISION/UNREAD Status，限制 hop、检测环并保持 wss/path/TLS
12. 群操作仅创建公开免审批群 + 加入公开免审批群 + 退出群；其他群成员管理由客户后台 REST 完成；
   容量/群数量/成员上限（一般 500）均由服务端决定，SDK 不本地判断、不预检群详情；群详情/成员列表不封装，客户自拉
13. REST 使用 Response + typed APIError，保留受限原始 body，并包含 request/operation reporting
14. 用户属性仅支持显式更新自己和批量查询；不携带 userinfo_update_time，不自动拉取，不分发相关 NOTIFY
15. 所有 ns=MUC 下行事件直接丢弃，不解析 mucbody、不提供群事件回调、不维护本地群状态
16. 接收侧 Ext/Params 保留 KeyValue 类型；64 位整数 JSON 编码为字符串；发送侧 MVP 仅接受 string
17. 消息不持久化、不维护业务会话；未知 body 透出 RawType/RawPayload，不阻断 queue
18. 会话生命周期事件透出；被禁用/被踢/被移除/token 失效等业务断开不自动重连
19. session_id 由 Provision 回复管理并用于 LOGOUT；STATISTIC 中的 session_id 仅用于诊断，不改变断开结论
20. 登录错误优先按 Status 枚举；FAIL reason 子串仅作为兼容分类，不手抄错误枚举数值
21. 心跳默认 120s，HeartbeatTimeout 默认 240s；网络重连使用三段随机退避，所有等待受 ctx/timeout 控制
22. SDK 只接受初始化参数；config、secret 注入、启停脚本属于示例部署层
23. 日志可注入；提供 telemetry/health/readiness hook，默认脱敏 token、Authorization、metadata 和消息 payload
