# Go IM SDK 集成 Demo 与 API 验收说明

本文用于在真实环境中验证 `cmd/integration-demo`：DNS 引导登录、长连接、实时消息收发、消息级 Ext、群组定向消息、用户属性以及公开群 REST API。

业务工程通过 Go Module 导包的完整代码请看 [GO_SDK_INTEGRATION_GUIDE.md](GO_SDK_INTEGRATION_GUIDE.md)。本篇只保留联调命令和验收标准。

## 一、准备环境

在 `<go-im-sdk-repo>` 目录执行：

```bash
go version
go test ./cmd/integration-demo
go test ./...
go test -race ./...
go vet ./...
```

复制配置模板：

```bash
cp config.example.yaml prod.yaml
chmod 600 prod.yaml
```

编辑 `prod.yaml`：

```yaml
app_key: "org#app"
user_id: "your-user"
resource: "550e8400-e29b-41d4-a716-446655440000"
token_file: "/run/secrets/easemob-token"
```

配置要求：

- `app_key` 使用 `org#app` 格式。
- `user_id` 是环信 IM 用户 ID，不是昵称。
- `resource` 在首次部署时使用 UUID 一类的原始稳定值，由业务生成并持久化。同一逻辑服务发生宕机、重启或故障转移时必须复用原值；换值会被服务端视为换设备登录。已经使用过的旧值即使不是 UUID 格式也不得替换。SDK 实际使用 `go-server-imsdk-<resource>`，前缀计入最终 128 字符限制，最终值不能包含空白、`/` 或 `@`。
- token 推荐放在权限为 `0600` 的文件中，也可以使用 `GO_IM_SDK_TOKEN_FILE` 或 `GO_IM_SDK_TOKEN`。

一个 IM 用户只能用于一个在线服务实例。测试时不要使用同一用户再启动第二个 Demo，也不要在其他 Client 或设备登录该用户，否则后登录的一方会把当前服务连接踢下线。目前 SDK 没有独立的被踢回调；业务需要自行保证账号独占，并从 `OnDisconnect(userID, error)` 和 SDK 错误码判断连接已经终止。

不再配置 `msync_host`、`rest_base` 或 `sdk_version`。Demo 先用 `AppKey` 和原始 resource 初始化 Client，在 `Config` 中一次性绑定 `MessageHandler` 和所有 listener，再调用 `Login(ctx, userID, token)`。首批同步消息因此不会早于 handler 注册；SDK 不补发历史回调，listener 也不应长时间阻塞。Login 固定请求：

```text
https://rs.easemob.com/easemob/server.json
  ?sdk_version=<SDK 内部版本>
  &app_key=<AppKey>
  &file_version=1
```

DNS 返回的 `msync-wx.hosts` 和 `rest.hosts` 是本次登录的唯一 WSS/REST 地址来源。SDK 优先选择 `priority=1`，否则使用第一个有效 host；WSS 接受 `wss` 和可转换为 `wss` 的 `https`，REST 只接受 `https` 并按 AppKey 组成 `/org/app` 根路径。SDK 最多尝试 3 次可重试的 DNS 错误；请求失败、非 2xx、响应过大、JSON 非法或缺少有效 WSS/REST host 都会使 Login 直接失败，不回退到配置或本地缓存。

### 构建一次，后续直接运行

Linux 客户发布构建使用随 Module 提供的 native codec，需要启用 CGO 和 C/C++ 工具链：

```bash
mkdir -p ./bin
CGO_ENABLED=1 go build -o ./bin/integration-demo ./cmd/integration-demo
```

macOS 仅用于本地 native ABI/API 验证，使用仓库已有的开发静态库：

```bash
CGO_ENABLED=1 GOARCH=arm64 \
  go build -tags nativecodecdev -o ./bin/integration-demo ./cmd/integration-demo

CGO_ENABLED=1 GOARCH=arm64 \
  go test -tags nativecodecdev ./...
```

正式客户构建不使用 Go generated protobuf。

## 二、启动长连接接收消息

使用已构建的二进制：

```bash
./bin/integration-demo -c prod.yaml -debug 2>&1 | tee integration.log
```

也可以在 macOS 开发环境直接运行：

```bash
go run -tags nativecodecdev ./cmd/integration-demo \
  -c prod.yaml \
  -debug \
  2>&1 | tee integration-native.log
```

Apple Silicon 使用 `GOARCH=arm64`；Intel Mac 使用与本机匹配的 amd64 native archive。

### 预期日志

连接成功：

```text
dns.resolved wss=wss://... rest_base=https://.../org/app
connection.state state=connected
connection.ready
```

`dns.resolved` 只在 `-debug` 下输出，可用来确认 WSS 和 REST 都来自 DNS。DNS 阶段失败时，`connection.failed` 的错误会标识 `dns bootstrap` 及具体原因。

收到实时消息：

```text
wss.notice
wss.queue_pull
wss.sync_batch meta_count=1
wss.meta
message.received
```

`message.received` 包含 `ext_count`。其 `message_json` 是脱敏视图：包含消息 ID、发送方、接收方、body 类型以及完整的消息级 `Ext`，不包含文本正文、CustomExts 或原始 payload。

带 Ext 的消息示例：

```json
{
  "ext": {
    "trace_id": {
      "type": "string",
      "value": "demo-123"
    }
  }
}
```

`meta_count=0` 通常表示使用 `next_key` 继续拉取后已到达队列尾部，不代表解析失败。

## 三、使用命令行测试消息 API

以下命令均可直接使用 `./bin/integration-demo`；也可以在 macOS 上用 `go run -tags nativecodecdev` 替换二进制。

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

CMD 消息只携带 action（旧协议的 params 参数已废弃）：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to peer-user \
  -send-type command \
  -send-action "run-job" \
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

### 单聊文本与消息级 Ext

`-send-ext` 使用与 `-send-params` 相同的 `key=value` 解析规则，但它写入整条消息的 `SendRequest.Ext`：

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to peer-user \
  -send-text "text with ext" \
  -send-ext "trace_id=demo-123,payload={\"source\":\"integration-demo\"}" \
  2>&1 | tee send-ext.log
```

命令行中合法 JSON 对象/数组映射为 `json_string`，其他值映射为 `string`。包含逗号的复杂 JSON 仍应改用 Go API。发送成功日志应有 `ext_count=2`，接收端的 `message.received` 应有 `ext_count=2`，且脱敏 `message_json` 中能看到 `trace_id` 和 `payload`。

三类扩展字段的边界：

- `SendRequest.Ext` 是消息级扩展，接收端对应 `Message.Ext`。Go API 支持 `KeyValueBool`、`KeyValueInt`、`KeyValueUint`、`KeyValueLong`、`KeyValueFloat`、`KeyValueDouble`、`KeyValueString` 和 `KeyValueJSONString`，SDK 按 key 稳定排序编码。
- `MessageBody.CustomExts` 只是 Custom body 扩展，发送时支持 `KeyValueString` 和 `KeyValueJSONString`。

`nil` 或空 `SendRequest.Ext` 不会发送 ext，与旧的 wire 行为保持一致。

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

### 带消息级 Ext 的群组定向消息

```bash
./bin/integration-demo \
  -c prod.yaml \
  -debug \
  -send-to GROUP_ID \
  -group \
  -directed-users "xu" \
  -send-ext "trace_id=directed-demo,payload={\"source\":\"go-demo\"}" \
  -send-text "directed with ext" \
  2>&1 | tee send-directed-ext.log
```

发送端应出现 `message.send_succeeded` 且 `ext_count=2`。目标端 `xu` 应新增一条 `message.received`，`ext_count=2`，并在 `message_json.ext` 中看到 `trace_id=directed-demo` 与 JSON 类型的 `payload`；非目标成员不应收到该消息。

### 发送端如何判定

发送端成功日志：

```text
wss.ack status=0
message.send_succeeded
```

其中包含最终服务器消息 ID `message_id`、仅用于 ACK 关联/重试的 `client_message_id`、`server_timestamp` 和 `ext_count`。接收端同一条消息的 `meta_id` 应等于发送成功日志中的 `message_id`，而不是 `client_message_id`。

注意：ACK 成功只说明服务端已受理消息，不证明目标客户端已经收到或完成业务处理。端到端投递必须同时检查目标端出现新的 `message.received`。

## 四、群组定向消息端到端验收

### 先确认群 ID 和成员

群组定向消息必须同时满足：

1. `-send-to` 是当前测试群的真实群 ID，不能复用其他环境或旧测试中的群 ID。
2. 发送者和所有 `-directed-users` 都是该群成员。
3. `-directed-users` 使用环信 IM 用户 ID，例如 `xu`，不是昵称、JID、resource 或设备 ID。
4. 接收端先连接并订阅群队列，再发送测试消息。

iOS 日志中显示的 `receiverList` 是 `Meta::toString()` 对 protobuf `directed_users` 字段的展示名称，线上协议字段仍是 `directed_users`。

可以使用 REST 直接检查群成员。`REST_BASE` 必须使用本次 `dns.resolved` 日志中的 `rest_base`，而不是本地配置值；SDK 已按 `AppKey` 自动补上 `/org/app`：

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
  -send-ext "trace_id=directed-demo,payload={\"source\":\"go-demo\"}" \
  -send-text "directed with ext" \
  2>&1 | tee send-directed-ext.log
```

验收结果：

- 三端的 `-debug` 日志都先出现 `dns.resolved`，WSS 和 REST 都是 DNS 返回值。
- 发送端：新增一条 `message.send_succeeded`，服务端 ACK 成功且 `ext_count=2`。
- `xu`：新增一条 `message.received`，`to` 等于本次 `GROUP_ID`，`ext_count=2`，并且 `message_json.ext` 包含 `trace_id` 和 `payload`。
- `lxm2`：`message.received` 数量不增加。

为避免误判，发送前记录 `xu` 和 `lxm2` 的 `message.received` 基线数；收到 ACK 后同时检查目标端 `+1` 和非目标端 `+0`。

不要用 `wss.unread_pull` 判断实时消息是否收到；该日志表示 `UNREAD` 触发的离线队列拉取。

### 自动脚本测试

仓库脚本会构建 Demo、启动 `lxm2`/`xu`、等待连接、由 `lxm` 创建包含二人的新公开群、等待群队列订阅、发送带消息级 Ext 的定向文本消息，然后检查目标端 `+1`、目标端 Ext、非目标端 `+0`：

```bash
./scripts/test-directed-message.sh
```

macOS Apple Silicon 使用：

```bash
GOARCH=arm64 ./scripts/test-directed-message.sh
```

默认定向给 `lxm2`。改为定向给 `xu`：

```bash
DIRECTED_USER=xu \
DIRECTED_TEXT='directed to xu' \
GOARCH=arm64 ./scripts/test-directed-message.sh
```

macOS 运行脚本时必须显式指定与 native archive 一致的 `GOARCH`；Linux 不需要设置该变量。

脚本默认使用 `DIRECTED_EXT=trace_id=directed-demo`，可通过同名环境变量覆盖。它验证群组定向 Text、消息级 Ext 与目标/非目标范围；Custom/CMD 使用上一节命令，在脚本创建并打印的 `GROUP_ID` 上手工验证即可。

脚本每次创建一个新的公开群，以消除错误群 ID、成员不一致和群队列订阅时序造成的假失败；测试结束不会自动解散该群，需要按测试环境的数据清理策略处理。

### 真实环境完整回归清单

不能只以一次定向 Text 代替全部回归。使用本文第三节的命令逐项完成：

1. 单聊 Text、CMD、Custom；
2. 普通群聊 Text；
3. 群组定向 Text、CMD、Custom；
4. 带消息级 Ext 的群组定向消息；
5. 每次发送都检查发送端 ACK 和目标端的新 `message.received`；定向消息还要检查非目标成员 `+0`；
6. Ext 用例同时检查发送/接收日志的 `ext_count` 和目标端 `message_json.ext`。

每个进程的登录日志都应证明 DNS 拉取成功。无效 DNS 响应、缺少 WSS host 和缺少 REST host 应由离线单元测试验证登录失败，不要修改生产 DNS 或用真实账号制造这些异常。

## 五、使用命令行测试 REST API

Demo 的 REST 操作与 WSS 使用同一次 Login 的 DNS 结果、用户和 token。非 2xx 响应会打印 HTTP status、服务端错误码和 request ID，但不会打印 Authorization。

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
dns.resolved
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

日志不会输出 Authorization 或 token。Demo 的 `message_json` 已脱敏，不输出文本正文、CustomExts 或原始 payload，但会保留本轮验收需要的消息级 Ext。Ext 也应视为业务数据，测试时不要放入 token、Authorization、个人信息或其他敏感值。

## 七、离线消息边界

Go IM SDK 登录后会消费 `UNREAD` 返回的离线/断线积压队列，对每个队列走 SYNC 拉取，并与在线消息同一条链路经回调投递：

```text
UNREAD -> 保活 + 对每个未读队列 SYNC -> 离线消息回调
NOTICE -> SYNC -> 实时消息回调
```

离线消息与在线消息经同一 `MessageHandler` 回调返回（基于队列 cursor 去重，业务按 `MetaID` 幂等）。看到 `wss.unread_pull` 表示正在拉取离线积压队列。若做实时定向消息验收，请注意区分登录时拉取的历史离线消息与本轮实时消息。

## 八、停止 Demo

测试完成后按：

```text
Ctrl+C
```

Demo 会先发送 logout，再退出进程。若长连接已断开，会记录退出错误，但不会影响进程结束。
