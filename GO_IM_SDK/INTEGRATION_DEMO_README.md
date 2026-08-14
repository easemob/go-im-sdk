# Go IM SDK 集成 Demo 与 API 验收说明

本文用于在真实环境中验证 `cmd/integration-demo`：长连接登录、实时消息收发、群组定向消息、用户属性以及公开群 REST API。

业务工程通过 Go Module 导包的完整代码请看 [GO_SDK_INTEGRATION_GUIDE.md](GO_SDK_INTEGRATION_GUIDE.md)。本篇只保留联调命令和验收标准。

## 一、准备环境

在 `<go-im-sdk-repo>/GO_IM_SDK` 目录执行：

```bash
go version
go test ./cmd/integration-demo
```

复制配置模板：

```bash
cp config.example.yaml prod.yaml
chmod 600 prod.yaml
```

编辑 `prod.yaml`：

```yaml
msync_host: "wss://.../websocket"
rest_base: "https://.../org/app"
app_key: "org#app"
user_id: "your-user"
resource: "go-service-instance-01"
token_file: "/run/secrets/easemob-token"
```

配置要求：

- `msync_host` 必须是 `wss://` 地址，`rest_base` 必须是 `https://` 地址。
- `app_key` 使用 `org#app` 格式。
- `user_id` 是环信 IM 用户 ID，不是昵称。
- `resource` 必须由业务持久化。同一服务实例重启时保持不变；同一 IM 用户的并行实例必须使用不同 resource。
- token 推荐放在权限为 `0600` 的文件中，也可以使用 `GO_IM_SDK_TOKEN_FILE` 或 `GO_IM_SDK_TOKEN`。

### 构建一次，后续直接运行

macOS 本地协议/API 联调推荐使用 Go protobuf 回归模式：

```bash
mkdir -p ./bin
go build -tags gopbcodec -o ./bin/integration-demo ./cmd/integration-demo
```

Linux 客户发布包默认使用随 Module 提供的 native codec，需要启用 CGO 和 C/C++ 工具链：

```bash
CGO_ENABLED=1 go build -o ./bin/integration-demo ./cmd/integration-demo
```

`gopbcodec` 是仓库内部回归/差分测试模式，不属于客户正式发布构建。

## 二、启动长连接接收消息

使用已构建的二进制：

```bash
./bin/integration-demo -c prod.yaml -debug 2>&1 | tee integration.log
```

也可以在 macOS 开发环境直接运行：

```bash
go run -tags gopbcodec ./cmd/integration-demo \
  -c prod.yaml \
  -debug \
  2>&1 | tee integration-gopb.log
```

macOS Apple Silicon 的 native codec 仅用于内部开发验证：

```bash
BUILD_DIR="$PWD/native/build/darwin-arm64" \
  ./scripts/build-native-codec.sh

CGO_ENABLED=1 GOARCH=arm64 \
  go run -tags nativecodecdev ./cmd/integration-demo \
  -c prod.yaml \
  -debug \
  2>&1 | tee integration-native.log
```

如果 Go 默认是 `darwin/amd64`，不要链接 arm64 静态库；应切换到 `GOARCH=arm64`，或使用真正的 amd64 native archive。

### 预期日志

连接成功：

```text
connection.state state=connected
connection.ready
```

收到实时消息：

```text
wss.notice
wss.queue_pull
wss.sync_batch meta_count=1
wss.meta
message.received
```

`message.received` 中的 `message_json` 包含完整 `Message`，包括消息 ID、发送方、接收方、文本、CMD 参数和 Custom 扩展。

`meta_count=0` 通常表示使用 `next_key` 继续拉取后已到达队列尾部，不代表解析失败。

## 三、使用命令行测试消息 API

以下命令均可把 `go run -tags gopbcodec ./cmd/integration-demo` 替换为 `./bin/integration-demo`。

### 单聊文本消息

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to peer-user \
  -send-text "integration test" \
  2>&1 | tee send.log
```

### 单聊 CMD 消息

`-send-params` 使用逗号分隔的 `key=value`：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to peer-user \
  -send-type command \
  -send-action "run-job" \
  -send-params "job_id=123,priority=high" \
  2>&1 | tee send-cmd.log
```

### 单聊 Custom 消息

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to peer-user \
  -send-type custom \
  -send-event "order-status" \
  -send-params "status=paid,amount=99" \
  2>&1 | tee send-custom.log
```

`-send-params` 的值若是合法 JSON 对象或数组，会作为 `json_string` 发送；否则作为普通 `string` 发送。命令行用逗号分隔参数，所以包含逗号的 JSON 值应改用 Go API，示例见 [GO_SDK_INTEGRATION_GUIDE.md](GO_SDK_INTEGRATION_GUIDE.md#六发送消息)。

### 普通群聊消息

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to GROUP_ID \
  -group \
  -send-text "group normal message" \
  2>&1 | tee send-group.log
```

### 群组定向文本消息

`-directed-users` 填逗号分隔的环信 IM 用户 ID，只投递给指定群成员：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to GROUP_ID \
  -group \
  -directed-users "xu" \
  -send-text "directed to xu" \
  2>&1 | tee send-directed-text.log
```

### 群组定向 Custom 消息

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to GROUP_ID \
  -group \
  -directed-users "xu" \
  -send-type custom \
  -send-event "order-status" \
  -send-params "status=paid,amount=99" \
  2>&1 | tee send-directed-custom.log
```

目标端 `message.received` 应包含：

```text
body_0_type=custom
body_0_event=order-status
```

### 群组定向 CMD 消息

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to GROUP_ID \
  -group \
  -directed-users "xu" \
  -send-type command \
  -send-action "refresh-cache" \
  -send-params "job_id=123,priority=high" \
  2>&1 | tee send-directed-cmd.log
```

目标端 `message.received` 应包含：

```text
body_0_type=command
body_0_action=refresh-cache
```

### 发送端如何判定

发送端成功日志：

```text
wss.ack status=0
message.send_succeeded
```

其中包含 `client_message_id`、`server_message_id` 和 `server_timestamp`。

注意：ACK 成功只说明服务端已受理消息，不证明目标客户端已经收到或完成业务处理。端到端投递必须同时检查目标端出现新的 `message.received`。

## 四、群组定向消息端到端验收

### 先确认群 ID 和成员

群组定向消息必须同时满足：

1. `-send-to` 是当前测试群的真实群 ID，不能复用其他环境或旧测试中的群 ID。
2. 发送者和所有 `-directed-users` 都是该群成员。
3. `-directed-users` 使用环信 IM 用户 ID，例如 `xu`，不是昵称、JID、resource 或设备 ID。
4. 接收端先连接并订阅群队列，再发送测试消息。

iOS 日志中显示的 `receiverList` 是 `Meta::toString()` 对 protobuf `directed_users` 字段的展示名称，线上协议字段仍是 `directed_users`。

可以使用 REST 直接检查群成员。`REST_BASE` 与配置中的 `rest_base` 一致，并且已经包含 `/org/app`：

```bash
export REST_BASE='https://a1.example.com/org/app'
export GROUP_ID='your-group-id'
export GO_IM_SDK_TOKEN_FILE='/run/secrets/easemob-token'

curl -sS \
  -H "Authorization: Bearer $(<"$GO_IM_SDK_TOKEN_FILE")" \
  "$REST_BASE/chatgroups/$GROUP_ID?joined_time=true"
```

响应的成员列表中应能看到目标用户。不要把生产 token 直接写进命令、截图、脚本或日志，也不要在启用 shell `xtrace` 时运行该命令。

### 手工三终端测试

假设：

- `lxm` 是群 owner 和发送方，使用 `prod.yaml`；
- `xu` 是目标接收方，使用 `prod-xu.yaml`；
- `lxm2` 是非目标群成员，使用 `prod-lxm2.yaml`。

终端 1，启动非目标接收方：

```bash
./bin/integration-demo \
  -c prod-lxm2.yaml \
  -debug \
  2>&1 | tee lxm2-receive.log
```

终端 2，启动目标接收方：

```bash
./bin/integration-demo \
  -c prod-xu.yaml \
  -debug \
  2>&1 | tee xu-receive.log
```

等两个接收端都出现 `connection.ready`，再在终端 3 发送：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to GROUP_ID \
  -group \
  -directed-users "xu" \
  -send-text "directed to xu" \
  2>&1 | tee send-directed-xu.log
```

验收结果：

- 发送端：新增一条 `message.send_succeeded`。
- `xu`：新增一条 `message.received`，且 `to` 等于本次 `GROUP_ID`。
- `lxm2`：`message.received` 数量不增加。

不要用 `wss.unread_ignored` 判断实时消息是否收到；该日志是 `UNREAD` 保活/离线边界信息。

### 自动脚本测试

仓库脚本会构建 Demo、启动 `lxm2`/`xu`、等待连接、由 `lxm` 创建包含二人的新公开群、等待群队列订阅、发送定向文本消息，然后检查目标端 `+1`、非目标端 `+0`：

```bash
./scripts/test-directed-message.sh
```

默认定向给 `lxm2`。改为定向给 `xu`：

```bash
DIRECTED_USER=xu \
DIRECTED_TEXT='directed to xu' \
./scripts/test-directed-message.sh
```

脚本当前验证的是群组定向文本消息。Custom/CMD 使用上一节命令，在脚本创建并打印的 `GROUP_ID` 上手工验证即可。

脚本每次创建一个新的公开群，以消除错误群 ID、成员不一致和群队列订阅时序造成的假失败；测试结束不会自动解散该群，需要按测试环境的数据清理策略处理。

## 五、使用命令行测试 REST API

Demo 的 REST 操作与 WSS 使用同一份配置和 token。非 2xx 响应会打印 HTTP status、服务端错误码和 request ID，但不会打印 Authorization。

### 安全探测当前用户

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -probe-rest \
  2>&1 | tee rest-probe.log
```

预期日志：`rest.probe_succeeded`。

### 设置当前用户属性

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -set-user 'nickname=Go Demo,department=IM' \
  2>&1 | tee user-set.log
```

### 获取用户属性

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -fetch-users 'your-user,peer-user' \
  -fetch-properties 'nickname,department' \
  2>&1 | tee user-fetch.log
```

省略 `-fetch-properties` 时获取服务端默认返回的属性：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -fetch-users 'your-user' \
  2>&1 | tee user-fetch.log
```

先设置再获取，检查 `rest.fetch_users_succeeded` 的 `body` 是否包含新值。

### 创建公开群

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -create-group 'go-sdk-test-group' \
  -group-members 'lxm2,xu' \
  2>&1 | tee group-create.log
```

从 `rest.create_group_succeeded` 的 `body` 中读取 `data.id`，后续命令必须使用这个实际 ID。服务端响应字段是 `id`，不是 `groupid`。

### 加入公开群

加入者应使用自己的配置文件：

```bash
./bin/integration-demo \
  -c prod-xu.yaml \
  -debug \
  -join-group GROUP_ID \
  2>&1 | tee group-join.log
```

### 退出群

```bash
./bin/integration-demo \
  -c prod-xu.yaml \
  -debug \
  -leave-group GROUP_ID \
  2>&1 | tee group-leave.log
```

## 六、统一日志排查

`-debug` 下 WSS 和 REST 使用同一个 logger，可保存到同一个文件：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -fetch-users 'your-user' \
  2>&1 | tee integration-all.log
```

常用日志：

```text
wss.outbound / wss.inbound
wss.notice / wss.queue_pull / wss.sync_batch
wss.meta / message.received
wss.ack / message.send_succeeded / message.send_failed
rest.request / rest.response / rest.error
```

排查定向消息时按以下顺序：

1. 发送端是否 `message.send_succeeded`。
2. `to` 是否等于本次确认过成员关系的群 ID。
3. 目标端在发送前是否已 `connection.ready` 并收到该群队列的 `wss.notice`。
4. 目标端是否新增 `message.received`。
5. 非目标成员的 `message.received` 是否没有增加。

日志不会输出 Authorization 或 token。Demo 为联调方便，会在消息回调中输出完整 `message_json`；生产业务应按隐私和安全要求缩减或脱敏。

## 七、离线消息边界

Go IM SDK 不消费 `UNREAD` 返回的离线/断线积压队列：

```text
UNREAD -> 仅保活
NOTICE -> SYNC -> 实时消息回调
```

离线漫游消息由用户服务端通过 REST/漫游接口自行拉取。看到 `wss.unread_ignored` 是预期行为，不用于本次实时定向消息验收。

## 八、停止 Demo

测试完成后按：

```text
Ctrl+C
```

Demo 会先发送 logout，再退出进程。若长连接已断开，会记录退出错误，但不会影响进程结束。
