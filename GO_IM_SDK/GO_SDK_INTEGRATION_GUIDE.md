# Go IM SDK 代码集成指南

本文面向把 SDK 作为 Go Module 导入业务服务的开发者，覆盖安装、初始化、DNS 登录、消息接收、文本/CMD/Custom 发送、消息级 Ext、群组定向发送、REST API、错误处理和优雅退出。

只想在真实环境跑命令验证接口，请看 [INTEGRATION_DEMO_README.md](INTEGRATION_DEMO_README.md)。

## 一、支持范围与构建要求

- 最低 Go 版本：1.21。
- 一个 `Client` 同一时刻只登录一个 IM 用户；`Logout` 后可以使用同一个 Client 再次 `Login`。
- 登录使用 SDK 内置的主 DNS 引导地址，只接受 DNS 返回的安全 WSS/REST 地址。
- Linux 客户发布构建默认使用 Module 内置 native codec，需要 `CGO_ENABLED=1` 和可用的 C/C++ 编译链接工具链。
- 客户构建只使用 native codec；Go generated protobuf 和协议源码不属于客户发布包。
- Module 通过 cgo 链接按 `GOOS/GOARCH` 提供的静态 `.a`；不要求业务工程额外集成 Apple framework 或部署 `.so`。
- SDK 处理实时消息，不存储会话/未读，也不消费断线期间的积压消息。

安装：

```bash
go get github.com/easemob/go-im-sdk/sdk@latest
```

导入：

```go
import imsdk "github.com/easemob/go-im-sdk/sdk"
```

### 新建 Go 工程和 GoLand 导包排查

业务工程必须使用 Go 1.21 或更高版本。若 GoLand 的 `External Libraries` 仍显示
`Go SDK 1.18.x`，Go 工具链无法按本 SDK 的 `go.mod` 加载模块，通常会连锁出现
`Cannot resolve symbol 'github.com'`、`Client`、`SendRequest` 等错误。切换 GoLand 的
GOROOT 到 Go 1.21+ 后，在包含 `go.mod` 的工程根目录执行：

```bash
go version                         # 应为 go1.21 或更高
go get github.com/easemob/go-im-sdk/sdk@latest
go mod tidy
go test ./...
```

如果测试的是尚未发布到远程仓库的本地 SDK 源码，`go get @latest` 不会看到本地改动，
可以临时使用 `replace` 指向源码目录：

```bash
cd /path/to/your-business-project
go mod edit -replace=github.com/easemob/go-im-sdk=/Users/zhujichao_1/Documents/Desktop/go-im-sdk/GO_IM_SDK
go mod edit -require=github.com/easemob/go-im-sdk@v0.0.0
go mod tidy
go test ./...
```

发布版本进入远程仓库后，应删除这条 `replace`，再使用带版本号的 `go get`。在 GoLand
中执行一次 “Sync Project with Go Modules” 或重新打开包含 `go.mod` 的工程即可刷新索引。
如果命令行 `go test` 已通过而编辑器仍报红，再执行 GoLand 的 `File → Invalidate Caches`
并重启。截图中的 `var client *imsdk.Client` 只适合编译检查，不能直接运行；它没有初始化
Client，运行会触发 nil pointer。实际连接请先调用 `imsdk.New(imsdk.Config{...})`。

Linux 生产构建：

```bash
CGO_ENABLED=1 go build ./cmd/your-service
```

`.so`/framework 不是当前发布形态：动态库还需要运行时搜索路径、ABI 和部署环境管理；Go SDK 的客户制品使用静态 `.a` 加公开 C ABI header 自包含链接。

Linux 本地回归：

```bash
CGO_ENABLED=1 go test ./...
```

macOS 仅支持使用开发 native archive 做本地验证：

```bash
CGO_ENABLED=1 GOARCH=arm64 go test -tags nativecodecdev ./...
```

## 二、初始化参数与 listener

初始化只创建 Client、固定 resource 并注册消息处理器和 listener，不执行登录或网络请求。最少需要提供：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `AppKey` | 是 | `org#app` 格式；DNS 查询和 REST 根路径都由它派生 |
| `Resource` | 是 | 业务持久化的原始设备/实例标识；SDK 自动增加固定前缀 |
| `MessageHandler` | 是 | 实时消息处理函数，必须在 `New` 时传入 |
| `Domain` | 否 | JID domain，默认 `easemob.com` |
| `Logger` | 否 | 自定义 `*slog.Logger` |
| `Debug` | 否 | 输出 DNS、协议命令、队列和 ACK 元数据，不输出 token |
| `HTTPClient` | 否 | 自定义 DNS/REST HTTP Client，例如统一代理、证书和超时策略 |
| `Telemetry` | 否 | REST/队列等操作的观测实现 |

以下回调也只能在 `Config` 中注册：

```go
OnConnectionStateChanged  func(imsdk.ConnState)
OnDisconnect              func(error)
OnTokenExpired            func()
OnTokenWillExpire         func(time.Time)
OnTokenRotated            func(string, int64)
OnUserForbidden           func()
OnUserRemoved             func()
OnUserKickedByOtherDevice func(string, string)
OnUserLoginAnotherDevice  func(string, string)
OnServerNotice            func(string, []byte)
```

listener 在 `New` 时绑定，因此 DNS/登录失败、Provision 和首批同步消息发生前已经就绪。SDK 不补发注册之前的历史事件，也不支持登录后动态添加 listener。回调由有界队列调度，必须快速返回；需要数据库、RPC 或 secret 持久化时，应立即投递到业务自己的有界工作队列。

### Resource 前缀和持久化

业务配置保存原始值：

```yaml
resource: "service-instance-01"
```

SDK 在 Provision、JID 和 REST `resource` query 中统一使用：

```text
go-server-imsdk-service-instance-01
```

固定前缀是 `go-server-imsdk-`，并计入最终 128 字符限制。最终值不能包含空白、`/`、`@`。业务必须持久化原始值并在同一逻辑实例重启后复用；同一用户的并行在线实例必须使用不同原始值。不要把已经带前缀的值再次传给 SDK，否则前缀会再次添加。SDK 不负责生成、保存或跨机器查重 resource。

`Config` 不再包含或需要 `UserID`、`Token`、`MsyncHost`、`RestBase`、`SDKVersion`：

- `userID` 和 token 在每次 `Login` 时传入；
- WSS/REST 地址只能来自本次登录前的 DNS 响应；
- SDK 版本由 SDK 内部固定，不接受业务覆盖。

## 三、DNS 登录与完整生命周期

调用顺序固定为：

```text
New(Config + MessageHandler + listeners)
→ Login(ctx, userID, token)
→ DNS
→ WSS
→ Provision
→ LoggedIn/Connected
```

### DNS 是强制登录前置

每次 `Login` 都请求 SDK 内置的主引导地址：

```text
https://rs.easemob.com/easemob/server.json
```

查询参数由 SDK 生成：

```text
sdk_version=<SDK 内部版本>
app_key=<Config.AppKey>
file_version=1
```

DNS 返回值具有权威性：

- WSS 从 `msync-wx.hosts` 选择，优先 `priority=1`，否则取第一个有效 host；
- WSS 接受 `wss`，也会把 DNS 返回的 `https` 转换成 `wss`，路径统一为 `/websocket`；
- REST 从 `rest.hosts` 选择，优先 `priority=1`，并且只接受 `https`；
- REST 根路径由 `AppKey` 自动拼为 `/org/app`；
- SDK 不使用旧 `MsyncHost`/`RestBase`、备用引导地址或本地缓存继续登录；
- 网络错误和可重试 HTTP 状态只做有限次重试；非 2xx、响应超过 1 MiB、非法 JSON、缺少 WSS 或 REST host 都会使登录失败；
- DNS 阶段错误以 `SDKError.Code == ErrDNS` 和 `Operation == "dns bootstrap"` 标识。

传给 `Login` 的 context 与 `Config.ConnectTimeout` 共同限制 DNS、WSS 和 Provision 整个流程。业务网络策略必须允许访问固定 DNS 引导域名及 DNS 返回的 WSS/REST 域名。

### 可编译的最小示例

下面的示例从环境变量读取身份，所有 listener 在 `New` 时注册，然后调用 `Login`。生产环境应把 handler 替换为可靠存储或消息队列。

```go
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	imsdk "github.com/easemob/go-im-sdk/sdk"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	appKey, err := requiredEnv("GO_IM_SDK_APP_KEY")
	if err != nil {
		return err
	}
	userID, err := requiredEnv("GO_IM_SDK_USER_ID")
	if err != nil {
		return err
	}
	resource, err := requiredEnv("GO_IM_SDK_RESOURCE")
	if err != nil {
		return err
	}
	token, err := requiredEnv("GO_IM_SDK_TOKEN")
	if err != nil {
		return err
	}

	var client *imsdk.Client
	client, err = imsdk.New(imsdk.Config{
		AppKey:   appKey,
		Resource: resource,
		Logger:   logger,
		MessageHandler: func(ctx context.Context, msg *imsdk.Message) error {
			// 先按 MetaID 幂等持久化或可靠投递，成功后才返回 nil。
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				logger.Info("message.received",
					"meta_id", msg.MetaID,
					"from", msg.From,
					"to", msg.To,
					"is_group", msg.IsGroup,
					"body_count", len(msg.Bodies),
					"ext_count", len(msg.Ext))
				return nil
			}
		},
		OnConnectionStateChanged: func(state imsdk.ConnState) {
			logger.Info("connection.state", "state", state.String())
		},
		OnDisconnect: func(err error) {
			logger.Warn("connection.disconnected", "error", err)
		},
		OnTokenRotated: func(newToken string, expiresIn int64) {
			// 不要记录 newToken；把它快速投递到业务 secret 存储流程。
			_ = newToken
			logger.Info("token.rotated", "expires_in", expiresIn)
		},
		OnTokenWillExpire: func(expiresAt time.Time) {
			logger.Warn("token.will_expire", "expires_at", expiresAt)
		},
		OnTokenExpired:  func() { logger.Error("token.expired") },
		OnUserForbidden: func() { logger.Error("user.forbidden") },
		OnUserRemoved:   func() { logger.Error("user.removed") },
		OnUserKickedByOtherDevice: func(device, reason string) {
			logger.Warn("user.kicked", "device", device, "reason", reason)
		},
		OnUserLoginAnotherDevice: func(device, reason string) {
			logger.Warn("user.other_login", "device", device, "reason", reason)
		},
		OnServerNotice: func(kind string, payload []byte) {
			logger.Info("server.notice", "kind", kind, "payload_bytes", len(payload))
		},
	})
	if err != nil {
		return fmt.Errorf("create IM client: %w", err)
	}
	defer client.Close(context.Background())

	loginCtx, cancelLogin := context.WithTimeout(context.Background(), 15*time.Second)
	err = client.Login(loginCtx, userID, token)
	cancelLogin()
	if err != nil {
		return fmt.Errorf("login IM client: %w", err)
	}

	processCtx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)
	defer stop()
	<-processCtx.Done()

	logoutCtx, cancelLogout := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLogout()
	if err := client.Logout(logoutCtx); err != nil {
		return fmt.Errorf("logout IM client: %w", err)
	}
	return nil
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("environment variable %s is required", name)
	}
	return value, nil
}
```

运行前只需要设置 AppKey、登录身份和原始 resource，不配置 WSS/REST：

```bash
export GO_IM_SDK_APP_KEY='org#app'
export GO_IM_SDK_USER_ID='server-bot'
export GO_IM_SDK_RESOURCE='server-bot-instance-01'
export GO_IM_SDK_TOKEN='...'

go run .
```

生产环境建议通过权限受控的 secret file/secret manager 注入 token。`OnTokenRotated` 收到的新 token 必须由业务持久化；SDK 只更新当前 Client 的内存 token。

### Login、Logout 和 Close 的边界

- `Login(ctx, userID, token)` 只能在 `LoginStateLogout` 调用；重复登录返回 `ErrAlreadyLoggedIn`；
- 登录成功后，当前 `userID` 自动成为所有发送消息的 `from`，业务不传 `from`；
- DNS 或连接失败后 Client 回到 logout 状态，可以修复外部问题后再次调用 `Login`；
- `Logout` 结束当前会话、清空当前身份/token/DNS 地址，但保留 Client、listener、worker 和 codec；
- `Logout` 后可以用相同或不同的 userID/token 再次 `Login`，每次都会重新请求 DNS；
- `Close` 是 Client 最终释放操作；调用后不能再 `Login`，但可以重复调用 `Close`。

## 四、接收消息

`MessageHandler` 收到稳定、可 JSON 序列化的 `Message`：

```go
type Message struct {
	From      string
	To        string
	IsGroup   bool
	MetaID    uint64
	Timestamp uint64
	Bodies    []*MessageBody
	Ext       map[string]KeyValue
}
```

按消息类型处理：

```go
func handleMessage(ctx context.Context, msg *imsdk.Message) error {
	if trace, ok := msg.Ext["trace_id"]; ok {
		log.Printf("trace type=%s value=%v", trace.Type, trace.Value)
	}

	for _, body := range msg.Bodies {
		switch body.Type {
		case imsdk.MessageBodyText:
			log.Printf("text=%q", body.Text)
		case imsdk.MessageBodyCommand:
			log.Printf("action=%q params=%v", body.Action, body.Params)
		case imsdk.MessageBodyCustom:
			log.Printf("event=%q exts=%v", body.Event, body.CustomExts)
		case imsdk.MessageBodyUnknown:
			log.Printf("unknown raw_type=%d bytes=%d", body.RawType, len(body.RawPayload))
		}
	}
	return persistIdempotently(ctx, msg.MetaID, msg)
}
```

上例中的 `persistIdempotently` 是业务实现，不是 SDK API。handler 契约：

- 投递语义为 at-least-once，必须用 `MetaID` 或稳定业务 ID 去重；
- 只有可靠持久化/投递成功后才返回 `nil`；
- 必须遵守传入的 `ctx` 并及时返回；context 取消是协作式的；
- 不要依赖 handler “绝不重复”，也不要在日志中输出敏感消息正文；
- 首批同步消息也由 `New` 时提供的同一个 `MessageHandler` 接收。

消息级 Ext 会出现在 `Message.Ext`。例如发送端的 `trace_id` 会被 JSON 序列化为：

```json
{
  "ext": {
    "trace_id": {
      "type": "string",
      "value": "request-123"
    }
  }
}
```

## 五、状态、token 与错误处理

常用状态/API：

```go
switch client.LoginState() {
case imsdk.LoginStateLoggedIn:
	log.Print("logged in")
case imsdk.LoginStateReconnecting:
	log.Print("reconnecting")
}

if client.Connected() {
	health := client.Health()
	log.Printf("generation=%d backlog=%d", health.ConnectionGeneration, health.QueueBacklog)
}

if expiresAt, ok := client.TokenExpiresAt(); ok {
	log.Printf("token expires at %s", expiresAt)
}

// 业务主动取得新 token 后更新当前会话内存中的凭据。
if err := client.UpdateToken(newToken); err != nil {
	return err
}
```

发送前必须已经成功 Login 且当前连接为 Connected：

- 未登录或仍在登录时，`Send` 返回 `ErrNotLoggedIn`；
- 已登录但处于重连/断开窗口时，`Send` 返回 `ErrNotConnected`；
- 已经有活跃登录时再次 `Login` 返回 `ErrAlreadyLoggedIn`；
- `Close` 后调用生命周期 API 返回 `ErrClientClosed`；
- DNS 引导失败返回 `ErrDNS`。

统一检查 SDK 错误：

```go
var sdkErr *imsdk.SDKError
if errors.As(err, &sdkErr) {
	log.Printf("operation=%s code=%s http_status=%d reason=%s",
		sdkErr.Operation, sdkErr.Code, sdkErr.HTTPStatus, sdkErr.Reason)
}
```

生产程序至少应处理 Config 中的断线、token 轮换/过期、用户禁用/移除、其他设备踢出/登录等 listener。

## 六、发送消息

所有发送继续使用 `Client.Send(ctx, SendRequest)`，不需要也没有 `CreateTextMessage`、`CreateCMDMessage` 或 `CreateCustomMessage` 工厂。返回的 `SendResult` 表示服务端 ACK：

```go
result, err := client.Send(ctx, request)
if err != nil {
	return err
}
log.Printf("message_id=%d client_id=%d timestamp=%d",
	result.MessageID,
	result.ClientMessageID,
	result.ServerTimestamp)
```

`result.MessageID` 是 ACK 中 `server_id` 给出的最终服务器消息 ID；接收端同一条消息的 `Message.MetaID` 与它一致。`ClientMessageID` 只用于发送前本地关联、ACK 匹配和结果不确定时的幂等重试，不能当作服务器消息 ID。`ServerMessageID` 暂时保留为 `MessageID` 的兼容别名，新代码应使用 `MessageID`。

ACK 成功只说明服务端已经受理，不证明接收端已收到或完成业务处理。需要业务级已送达/已读语义时，应设计回执协议。

### 单聊文本和普通群聊

```go
_, err := client.Send(ctx, imsdk.SendRequest{
	To: "xu",
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "hello xu",
	},
})

_, err = client.Send(ctx, imsdk.SendRequest{
	To:      groupID,
	IsGroup: true,
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "group normal message",
	},
})
```

### 消息级 Ext

`SendRequest.Ext` 映射到协议 `MessageBody.ext`，接收端从 `Message.Ext` 读取。它支持全部八种 KeyValue 类型：

```go
result, err := client.Send(ctx, imsdk.SendRequest{
	To: "xu",
	Ext: map[string]imsdk.KeyValue{
		"enabled":  {Type: imsdk.KeyValueBool, Value: true},
		"attempt":  {Type: imsdk.KeyValueInt, Value: int32(3)},
		"sequence": {Type: imsdk.KeyValueUint, Value: uint64(42)},
		"offset":   {Type: imsdk.KeyValueLong, Value: int64(-9)},
		"ratio":    {Type: imsdk.KeyValueFloat, Value: float32(1.25)},
		"score":    {Type: imsdk.KeyValueDouble, Value: float64(9.5)},
		"trace_id": {Type: imsdk.KeyValueString, Value: "request-123"},
		"payload": {
			Type:  imsdk.KeyValueJSONString,
			Value: `{"order_id":"123","items":[1,2]}`,
		},
	},
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "hello with ext",
	},
})
```

`KeyValueLong` 的 JSON `type` 名为 `llint`。`uint` 和 `llint` 在 `KeyValue` 的 JSON 表示中使用十进制字符串，避免 JavaScript 对 64 位整数静默舍入。

Ext 编码规则：

- key 不能为空；
- key 按字典序稳定编码，便于复现和测试；
- `nil` 或空 map 不发送 ext，也不会分配 native Ext 数组，保持原有 wire 行为；
- `KeyValueJSONString.Value` 是承载 JSON 文本的 Go `string`；需要业务自行保证内容语义；
- 类型和值不匹配会在发送前返回参数错误。

三个扩展位置不能混用：

| 字段 | 协议位置 | 用途 | 发送类型 |
| --- | --- | --- | --- |
| `SendRequest.Ext` | 整条消息的 `MessageBody.ext` | trace、业务上下文、跨消息类型元数据 | bool/int/uint/long/float/double/string/json_string |
| `MessageBody.Params` | CMD content 参数 | 命令 action 的参数 | string/json_string |
| `MessageBody.CustomExts` | Custom content 扩展 | Custom event 自身的数据 | string/json_string |

### 群组定向 Text、CMD 和 Custom

`DirectedUsers` 只有在 `IsGroup: true` 时有效；否则 SDK 返回参数错误。列表元素必须是该群真实成员的环信 IM 用户 ID，不是昵称、resource 或完整 JID。

定向文本并携带消息级 Ext：

```go
_, err := client.Send(ctx, imsdk.SendRequest{
	To:            groupID,
	IsGroup:       true,
	DirectedUsers: []string{"xu"},
	Ext: map[string]imsdk.KeyValue{
		"trace_id": {Type: imsdk.KeyValueString, Value: "directed-demo"},
	},
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "directed to xu",
	},
})
```

定向 CMD：

```go
_, err := client.Send(ctx, imsdk.SendRequest{
	To:            groupID,
	IsGroup:       true,
	DirectedUsers: []string{"xu"},
	Body: imsdk.MessageBody{
		Type:   imsdk.MessageBodyCommand,
		Action: "refresh-cache",
		Params: map[string]imsdk.KeyValue{
			"job_id":  {Type: imsdk.KeyValueString, Value: "123"},
			"payload": {Type: imsdk.KeyValueJSONString, Value: `{"scope":"user"}`},
		},
	},
})
```

定向 Custom：

```go
_, err := client.Send(ctx, imsdk.SendRequest{
	To:            groupID,
	IsGroup:       true,
	DirectedUsers: []string{"xu"},
	Body: imsdk.MessageBody{
		Type:  imsdk.MessageBodyCustom,
		Event: "order-status",
		CustomExts: map[string]imsdk.KeyValue{
			"status":  {Type: imsdk.KeyValueString, Value: "paid"},
			"payload": {Type: imsdk.KeyValueJSONString, Value: `{"order_id":"123"}`},
		},
	},
})
```

### ClientMessageID 与结果不确定

不设置 `ClientMessageID` 时，SDK 会为当前 Client 生成 ID。需要跨进程重试或严格幂等时，建议由业务生成并持久化稳定 ID：

```go
request := imsdk.SendRequest{
	ClientMessageID: businessStableMessageID,
	To:              "xu",
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "hello",
	},
}

result, err := client.Send(ctx, request)
var sdkErr *imsdk.SDKError
if errors.As(err, &sdkErr) && sdkErr.Code == imsdk.ErrSendOutcomeUnknown {
	// ACK 超时或连接在 ACK 前断开：消息可能已经到达服务端。
	// 业务决定重试时，必须复用同一个 request.ClientMessageID。
}
```

不要在结果不确定时生成一个新 ID 立即重发，否则可能产生业务重复消息。

## 七、调用 REST API

REST 地址不再从 Config/YAML 读取。成功 `Login` 后，SDK 使用同一次 DNS 响应中 `rest.hosts` 的 HTTPS domain，并根据 `AppKey=org#app` 自动形成：

```text
https://<dns-rest-domain>/org/app
```

REST 方法复用当前登录的 userID、当前 token 和带 `go-server-imsdk-` 前缀的 resource。未成功登录时调用 REST API 返回 `ErrNotLoggedIn`。DNS 响应缺少有效 REST host 时，`Login` 本身就会失败，不会出现“WSS 已登录但 REST 仍使用旧地址”的混合状态。

### 设置和获取用户属性

```go
response, err := client.UpdateOwnUserInfo(ctx, map[string]string{
	"nickname":   "Go Bot",
	"department": "IM",
})
if err != nil {
	return err
}
log.Printf("status=%d body=%s", response.StatusCode, response.Body)

response, err = client.FetchUserInfo(
	ctx,
	[]string{"lxm", "xu"},
	[]string{"nickname", "department"},
)
```

`FetchUserInfo` 的 properties 传 `nil` 时，由服务端决定默认返回属性。

### 创建公开群并解析群 ID

```go
response, err := client.CreatePublicGroup(
	ctx,
	"go-sdk-test-group",
	imsdk.CreatePublicGroupOptions{
		Description: "Go SDK integration test",
		MaxUsers:    200,
		Members:     []string{"lxm2", "xu"},
	},
)
if err != nil {
	return err
}

var created struct {
	Data struct {
		ID string `json:"id"`
	} `json:"data"`
}
if err := json.Unmarshal(response.Body, &created); err != nil {
	return fmt.Errorf("decode create group response: %w", err)
}
if created.Data.ID == "" {
	return fmt.Errorf("create group response has no data.id")
}
groupID := created.Data.ID
```

后续发送必须使用响应中的 `data.id`，不要复用其他环境或旧测试里的群 ID。

### 加入和退出公开群

```go
response, err := client.JoinPublicGroup(ctx, groupID)
if err != nil {
	return err
}
log.Printf("join status=%d", response.StatusCode)

response, err = client.LeaveGroup(ctx, groupID)
if err != nil {
	return err
}
log.Printf("leave status=%d", response.StatusCode)
```

调用 `JoinPublicGroup`/`LeaveGroup` 的身份就是当前 `Login` 的 IM 用户。

### REST 错误处理

```go
response, err := client.FetchUserInfo(ctx, []string{"xu"}, nil)
if err != nil {
	var apiErr *imsdk.APIError
	if errors.As(err, &apiErr) {
		status := 0
		if apiErr.Response != nil {
			status = apiErr.Response.StatusCode
		}
		log.Printf("REST failed: status=%d service_code=%s request_id=%s retry_after=%s",
			status,
			apiErr.ServiceCode,
			apiErr.RequestID,
			apiErr.RetryAfter)
	}
	return err
}
_ = response
```

SDK 不会自动重试创建群、入群等有副作用的 REST 写操作。调用超时或网络断开且结果不确定时，应先查询服务端状态，再决定是否重试。

## 八、使用现有账号做真实环境矩阵验收

仓库中的三个配置分别用于：

| 配置 | 用户 | 角色 |
| --- | --- | --- |
| `prod.yaml` | `lxm` | 发送端/建群端 |
| `prod-xu.yaml` | `xu` | 目标或非目标接收端 |
| `prod-lxm2.yaml` | `lxm2` | 目标或非目标接收端 |

这些 YAML 只保存 AppKey、用户、token 来源、原始 resource 和超时/队列参数；不应保存 `msync_host`、`rest_base` 或 `sdk_version`。构建 demo：

```bash
mkdir -p ./bin
CGO_ENABLED=1 go build -o ./bin/integration-demo ./cmd/integration-demo
```

macOS 开发验证使用：

```bash
CGO_ENABLED=1 GOARCH=arm64 \
  go build -tags nativecodecdev -o ./bin/integration-demo ./cmd/integration-demo
```

### 三终端时序

终端 1 启动 lxm2：

```bash
./bin/integration-demo -c prod-lxm2.yaml -debug 2>&1 | tee lxm2-receive.log
```

终端 2 启动 xu：

```bash
./bin/integration-demo -c prod-xu.yaml -debug 2>&1 | tee xu-receive.log
```

两个接收端都出现 `dns.resolved` 和 `connection.ready` 后，再在终端 3 使用 `prod.yaml`。群聊矩阵中的 `GROUP_ID` 必须是当前 AppKey 下的真实群，且 `lxm`、`xu`、`lxm2` 都已经入群；不要复用其他环境或历史测试中的群 ID。每条发送命令 ACK 成功后按 Ctrl-C 退出，再执行下一条。

普通 Text：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to xu -send-text "text from go" \
  2>&1 | tee send-text.log
```

CMD：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to xu -send-type command -send-action refresh-cache \
  -send-params "job_id=123,priority=high" \
  2>&1 | tee send-cmd.log
```

Custom：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to xu -send-type custom -send-event order-status \
  -send-params "status=paid" \
  2>&1 | tee send-custom.log
```

普通群消息：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to GROUP_ID -group -send-text "normal group message" \
  2>&1 | tee send-group.log
```

群组定向 Text：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to GROUP_ID -group -directed-users "xu" \
  -send-text "directed text" \
  2>&1 | tee send-directed-text.log
```

群组定向 CMD：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to GROUP_ID -group -directed-users "xu" \
  -send-type command -send-action refresh-cache \
  -send-params "job_id=directed-123" \
  2>&1 | tee send-directed-cmd.log
```

群组定向 Custom：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to GROUP_ID -group -directed-users "xu" \
  -send-type custom -send-event order-status \
  -send-params "status=paid" \
  2>&1 | tee send-directed-custom.log
```

群组定向 Text 加消息级 Ext：

```bash
./bin/integration-demo -c prod.yaml -debug \
  -send-to GROUP_ID -group -directed-users "xu" \
  -send-ext 'trace_id=directed-demo,payload={"source":"go-demo"}' \
  -send-text "directed with ext" \
  2>&1 | tee send-directed-ext.log
```

`-send-ext` 与 `-send-params` 使用相同规则：合法 JSON 对象/数组转为 `json_string`，其他值转为 `string`。逗号用于分隔条目，因此 JSON 内含逗号时应改用 Go API。

### 验收判据

- 每个进程先出现 `dns.resolved`，日志中的 WSS/REST 都来自本次 DNS；
- 发送端出现 `wss.ack status=0` 和 `message.send_succeeded`；
- 普通消息在预期接收端出现新的 `message.received`；
- 定向消息只让 `DirectedUsers` 中的成员新增 `message.received`，非目标成员无新增；
- 带 Ext 的发送/接收日志包含 `ext_count=2`；
- 目标端安全化的 `message_json` 中包含 `ext.trace_id.type=string` 和 `value=directed-demo`；
- token、Authorization 和敏感完整正文没有出现在日志中。

建议同时运行自动化与 native codec 回归：

```bash
go test ./...
go test -race ./...
go vet ./...

CGO_ENABLED=1 GOARCH=arm64 \
  go test -tags nativecodecdev ./...
```

DNS 的非法 JSON、超大响应、缺少 WSS、缺少 REST、scheme 和 priority 选择由单元测试中的受控 HTTP 响应覆盖；真实环境不应提供绕过固定主 DNS 的配置开关。

## 九、优雅退出与重新登录

正常收到 SIGTERM/SIGINT 时：

1. 停止接收新的业务发送请求；
2. 给在途业务处理一个有界排空窗口；
3. 用带超时的 context 调用 `Logout`；
4. 进程最终退出时调用 `Close` 释放 Client；`Close` 可重复调用。

```go
logoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
if err := client.Logout(logoutCtx); err != nil {
	log.Printf("logout failed: %v", err)
}
cancel()

// Logout 后 Client 仍可复用；下面会重新请求 DNS 并建立新会话。
loginCtx, cancelLogin := context.WithTimeout(context.Background(), 15*time.Second)
if err := client.Login(loginCtx, nextUserID, nextToken); err != nil {
	log.Printf("relogin failed: %v", err)
}
cancelLogin()

// 仅在不再使用 Client 时最终释放。
if err := client.Close(context.Background()); err != nil {
	log.Printf("close failed: %v", err)
}
```

`Close` 不会强制终止忽略 context 的业务 handler。handler 启动的 goroutine 和外部资源由业务负责停止。

## 十、生产接入检查表

- token 来自 secret manager/secret file，不在源码、配置仓库或日志中明文保存；
- WSS/REST 未写入业务配置，网络策略允许固定 DNS 引导地址和 DNS 返回域名；
- 原始 `Resource` 已持久化，并理解 SDK 会添加 `go-server-imsdk-` 前缀；
- `MessageHandler` 和所有 listener 都在 `New` 的 `Config` 中注册；
- `MessageHandler` 按 `MetaID` 幂等，可靠落库后才返回 `nil`；
- handler、listener、日志和 telemetry 不执行无界阻塞操作；
- 已处理 DNS、token 轮换/将过期/已过期、用户禁用/移除、踢出和其他设备登录事件；
- 只在成功 `Login` 且 `Connected()` 为 true 时发送；
- 消息级 `Ext`、CMD `Params` 和 Custom `CustomExts` 没有混用；
- 发送结果不确定时复用原 `ClientMessageID`，不会盲目生成新 ID 重发；
- 群组定向消息使用真实群 ID 和群成员的 IM 用户 ID；
- 已将 `Client.Health()` 接入 readiness/监控，通常只有 `Connected()` 为 true 才报告 ready；
- 已验证 `Logout → Login` 复用，以及 SIGTERM 下的有界 Logout/Close；
- 已完成单聊 Text/CMD/Custom、普通群聊、群定向 Text/CMD/Custom、带 Ext 定向消息和 REST 的真实环境验收。
