# Go IM SDK 代码集成指南

本文面向把 SDK 作为 Go Module 导入业务服务的开发者，覆盖安装、初始化、连接、消息接收、文本/CMD/Custom 发送、群组定向发送、REST API、错误处理和优雅退出。

只想在真实环境跑命令验证接口，请看 [INTEGRATION_DEMO_README.md](INTEGRATION_DEMO_README.md)。

## 一、支持范围与构建要求

- 最低 Go 版本：1.21。
- 一个 `Client` 绑定一个 IM `UserID`。
- 仅支持 `wss://` msync 和 `https://` REST。
- Linux 客户发布构建默认使用 Module 内置 native codec，需要 `CGO_ENABLED=1` 和可用的 C/C++ 编译链接工具链。
- `gopbcodec` 只用于仓库内部回归、差分测试和 macOS 本地联调，不属于客户正式发布构建。
- SDK 处理实时消息，不存储会话/未读，也不消费断线期间的积压消息。

安装：

```bash
go get github.com/easemob/go-im-sdk/sdk@latest
```

导入：

```go
import imsdk "github.com/easemob/go-im-sdk/sdk"
```

Linux 生产构建：

```bash
CGO_ENABLED=1 go build ./cmd/your-service
```

在 SDK 仓库内做本地回归：

```bash
go test -tags gopbcodec ./...
```

## 二、初始化参数

最少需要提供：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `MsyncHost` | 是 | `wss://` 长连接地址，未带路径时默认补 `/websocket` |
| `RestBase` | 是 | `https://` REST 根地址，通常已经包含 `/org/app` |
| `AppKey` | 是 | `org#app` 格式 |
| `UserID` | 是 | 当前登录的环信 IM 用户 ID |
| `Token` | 是 | 用户 token，不要写入源码或日志 |
| `Resource` | 是 | 稳定设备/实例标识，1–128 字符，不含空白、`/`、`@` |
| `MessageHandler` | 是 | 实时消息处理函数 |
| `Domain` | 否 | 默认 `easemob.com` |
| `Logger` | 否 | 自定义 `*slog.Logger` |
| `Debug` | 否 | 输出协议命令、队列和 ACK 元数据，不输出 token |

`Resource` 不是每次启动生成的随机值：同一逻辑服务实例重启后应复用原值；同一 IM 用户的不同并行实例必须使用不同值。SDK 不负责持久化或跨机器查重。

## 三、可编译的最小生命周期示例

下面的示例从环境变量读取配置，连接后持续接收消息，在 SIGINT/SIGTERM 时执行 Logout。示例 handler 只打印元数据；生产环境必须替换为数据库、消息队列等可靠处理逻辑。

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
	token, err := requiredEnv("GO_IM_SDK_TOKEN")
	if err != nil {
		return err
	}

	client, err := imsdk.New(imsdk.Config{
		MsyncHost: requiredEnvOrPanic("GO_IM_SDK_MSYNC_HOST"),
		RestBase:  requiredEnvOrPanic("GO_IM_SDK_REST_BASE"),
		AppKey:    requiredEnvOrPanic("GO_IM_SDK_APP_KEY"),
		UserID:    requiredEnvOrPanic("GO_IM_SDK_USER_ID"),
		Token:     token,
		Resource:  requiredEnvOrPanic("GO_IM_SDK_RESOURCE"),
		Logger:    logger,
		MessageHandler: func(ctx context.Context, msg *imsdk.Message) error {
			// 生产环境：先按 msg.MetaID 幂等持久化或可靠投递，再返回 nil。
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				logger.Info("message.received",
					"meta_id", msg.MetaID,
					"from", msg.From,
					"to", msg.To,
					"is_group", msg.IsGroup,
					"body_count", len(msg.Bodies))
				return nil
			}
		},
	})
	if err != nil {
		return fmt.Errorf("create IM client: %w", err)
	}
	defer client.Close(context.Background())

	client.OnConnectionStateChanged(func(state imsdk.ConnState) {
		logger.Info("connection.state", "state", state.String())
	})
	client.OnDisconnect(func(err error) {
		logger.Warn("connection.disconnected", "error", err)
	})
	client.OnTokenRotated(func(newToken string, expiresIn int64) {
		// 不要记录 newToken。应快速交给业务 secret 存储流程持久化。
		_ = newToken
		logger.Info("token.rotated", "expires_in", expiresIn)
	})
	client.OnTokenWillExpire(func(expiresAt time.Time) {
		logger.Warn("token.will_expire", "expires_at", expiresAt)
	})
	client.OnTokenExpired(func() {
		logger.Error("token.expired")
	})
	client.OnUserForbidden(func() {
		logger.Error("user.forbidden")
	})
	client.OnUserRemoved(func() {
		logger.Error("user.removed")
	})

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 15*time.Second)
	err = client.Connect(connectCtx)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("connect IM client: %w", err)
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

func requiredEnvOrPanic(name string) string {
	value, err := requiredEnv(name)
	if err != nil {
		panic(err)
	}
	return value
}
```

运行前设置配置。生产环境建议通过 secret file/secret manager 注入 token；这里用环境变量只是展示最小代码：

```bash
export GO_IM_SDK_MSYNC_HOST='wss://.../websocket'
export GO_IM_SDK_REST_BASE='https://.../org/app'
export GO_IM_SDK_APP_KEY='org#app'
export GO_IM_SDK_USER_ID='server-bot'
export GO_IM_SDK_RESOURCE='server-bot-instance-01'
export GO_IM_SDK_TOKEN='...'

go run .
```

`OnTokenRotated` 收到的 token 必须落入业务 secret 存储，SDK 只更新内存，不负责持久化。所有状态回调都应快速返回，不要在回调线程执行长事务。

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

- 投递语义为 at-least-once，必须用 `MetaID` 或稳定业务 ID 去重。
- 只有可靠持久化/投递成功后才返回 `nil`。
- 必须遵守传入的 `ctx` 并及时返回；context 取消是协作式的。
- 不要依赖 handler “绝不重复”，也不要在日志中输出敏感消息正文。

## 五、连接状态与 token

常用状态/API：

```go
if client.Connected() {
	health := client.Health()
	log.Printf("generation=%d backlog=%d", health.ConnectionGeneration, health.QueueBacklog)
}

if expiresAt, ok := client.TokenExpiresAt(); ok {
	log.Printf("token expires at %s", expiresAt)
}

// 业务主动取得新 token 后更新 SDK 内存中的凭据。
if err := client.UpdateToken(newToken); err != nil {
	return err
}
```

生产程序至少应处理：`OnDisconnect`、`OnTokenRotated`、`OnTokenWillExpire`、`OnTokenExpired`、`OnUserForbidden`、`OnUserRemoved`、`OnUserKickedByOtherDevice` 和 `OnUserLoginAnotherDevice`。

## 六、发送消息

所有发送都使用 `Client.Send(ctx, SendRequest)`。返回的 `SendResult` 表示服务端 ACK：

```go
result, err := client.Send(ctx, request)
if err != nil {
	return err
}
log.Printf("client_id=%d server_id=%d timestamp=%d",
	result.ClientMessageID,
	result.ServerMessageID,
	result.ServerTimestamp)
```

ACK 成功只说明服务端已经受理，不证明接收端已收到或完成业务处理。需要业务级已送达/已读语义时，应设计回执协议。

### 单聊文本

```go
result, err := client.Send(ctx, imsdk.SendRequest{
	To: "xu",
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "hello xu",
	},
})
```

### 普通群聊文本

```go
result, err := client.Send(ctx, imsdk.SendRequest{
	To:      groupID,
	IsGroup: true,
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "group normal message",
	},
})
```

### 群组定向文本

```go
result, err := client.Send(ctx, imsdk.SendRequest{
	To:            groupID,
	IsGroup:       true,
	DirectedUsers: []string{"xu"},
	Body: imsdk.MessageBody{
		Type: imsdk.MessageBodyText,
		Text: "directed to xu",
	},
})
```

`DirectedUsers` 只有在 `IsGroup: true` 时有效；否则 SDK 返回参数错误。列表元素必须是该群真实成员的环信 IM 用户 ID，不是昵称、resource 或完整 JID。

### 群组定向 Custom

```go
result, err := client.Send(ctx, imsdk.SendRequest{
	To:            groupID,
	IsGroup:       true,
	DirectedUsers: []string{"xu"},
	Body: imsdk.MessageBody{
		Type:  imsdk.MessageBodyCustom,
		Event: "order-status",
		CustomExts: map[string]imsdk.KeyValue{
			"status": {
				Type:  imsdk.KeyValueString,
				Value: "paid",
			},
			"payload": {
				Type:  imsdk.KeyValueJSONString,
				Value: `{"order_id":"123","items":[1,2]}`,
			},
		},
	},
})
```

### 群组定向 CMD

```go
result, err := client.Send(ctx, imsdk.SendRequest{
	To:            groupID,
	IsGroup:       true,
	DirectedUsers: []string{"xu"},
	Body: imsdk.MessageBody{
		Type:   imsdk.MessageBodyCommand,
		Action: "refresh-cache",
		Params: map[string]imsdk.KeyValue{
			"job_id": {
				Type:  imsdk.KeyValueString,
				Value: "123",
			},
			"priority": {
				Type:  imsdk.KeyValueString,
				Value: "high",
			},
		},
	},
})
```

发送 CMD 参数和 Custom 扩展时，目前只支持 `KeyValueString` 与 `KeyValueJSONString`，并且 `Value` 必须是 Go `string`。接收端可以解析更多协议类型。

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
	// 只有在业务决定重试时，才用同一个 request.ClientMessageID 重发。
}
```

不要在结果不确定时生成一个新 ID 立即重发，否则可能产生业务重复消息。

## 七、调用 REST API

REST 方法复用初始化时的 `RestBase`、`UserID`、`Resource` 和当前 token。响应 body 是受大小限制且已完整读取的 `[]byte`。

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

调用 `JoinPublicGroup`/`LeaveGroup` 的 Client 身份就是执行操作的 IM 用户。

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

## 八、优雅退出

正常收到 SIGTERM/SIGINT 时：

1. 停止接收新的业务发送请求。
2. 给在途业务处理一个有界的排空窗口。
3. 用带超时的 context 调用 `Logout`。
4. 再调用 `Close` 兜底；`Close` 可重复调用。

```go
logoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := client.Logout(logoutCtx); err != nil {
	log.Printf("logout failed: %v", err)
}
if err := client.Close(context.Background()); err != nil {
	log.Printf("close failed: %v", err)
}
```

`Close` 不会强制终止忽略 context 的业务 handler。handler 启动的 goroutine 和外部资源由业务负责停止。

## 九、生产接入检查表

- token 来自 secret manager/secret file，不在源码、配置仓库或日志中明文保存。
- `Resource` 已持久化，并在同一用户的并行实例间保持唯一。
- `MessageHandler` 按 `MetaID` 幂等，可靠落库后才返回 `nil`。
- handler、回调、日志和 telemetry 不执行无界阻塞操作。
- 已处理 token 轮换、将过期、已过期、用户禁用、移除、踢出和其他设备登录事件。
- 发送结果不确定时复用原 `ClientMessageID`，不会盲目生成新 ID 重发。
- 群组定向消息使用真实群 ID 和群成员的 IM 用户 ID。
- 已将 `Client.Health()` 接入 readiness/监控，通常只有 `Connected()` 为 true 才报告 ready。
- 已验证 SIGTERM 下的有界 Logout/Close。
- 已用 [INTEGRATION_DEMO_README.md](INTEGRATION_DEMO_README.md) 的真实环境命令完成单聊、群聊、群定向 Text/Custom/CMD 和 REST 验收。
