# Go IM SDK 安全与性能修复报告

日期：2026-08-13
范围：`native/`（C++ wrapper）与 `sdk/`、`internal/protocol/`（Go）中的 crash 风险、死锁/假死风险、可用性缺陷与性能缺陷。
依据：对 C++ 编解码 ABI、cgo 边界、连接/队列/心跳生命周期、锁模型、退避与背压机制的审查结论。

---

## 1. 总结

本次审查与修复围绕"是否会导致服务器宕机 / 性能异常 / 监控看不见的静默停服"展开。结论是：

- **cgo 边界与 C++ 层本身是稳固的**（`nativecodec.Codec` 用 `RWMutex` 隔离 `em_codec_destroy` 与 in-flight 调用、指针生命周期遵守头文件契约、`new(nothrow)` + `catch-all` + `struct_size` 校验、ACK 路径无持锁阻塞、无锁序倒置），未发现可复现的野指针 / 越界 / double-free。
- 真正的高危问题集中在 **活性（liveness）与背压** 层面：**缺读写超时导致连接假死且永不重连**、**毒消息叠加退避不累积导致无限重连风暴**。这两类问题在正常网络下不暴露，只在真实网络异常（半开连接、对端宿主机宕机、消息损坏、handler 持续失败）下触发，且 `Health()` 仍会误报 `Connected: true`，是典型的"监控发现不了的停服"。

修复按严重度分三期实施，全部通过 `go build ./...`、`go vet ./...`、`go test -race ./...`，`codec.cpp` 单独编译通过。

---

## 2. 严重度分期

| 分期 | 内容 | 类别 |
|---|---|---|
| P0 | 读写超时（假死） | 安全/可用性 |
| P0 | 毒消息死信 + 跨代际退避（重连风暴） | 安全/可用性 |
| P0 | C++ 节点预算无符号下溢 | 安全（DoS 防线） |
| P1 | readPump 队头阻塞（背压耦合 keepalive） | 性能/可用性 |
| P1 | 队列活跃数 O(n) 持锁扫描 | 性能 |
| P1 | native 开发适配器 finalizer（UAF/double-free 地雷） | 安全（死代码） |
| P2 | `LoginState`/`ConnState.String()` 越界 panic | 安全（防御） |
| P2 | 死代码清理 | 维护性 |

---

## 3. 安全问题修复

### 3.1 [P0] 缺少读写超时 → 连接永久假死且永不重连

**问题**
`connection.go` 的 `dial()` 只设置了 `SetReadLimit`，全项目没有任何 `SetWriteDeadline` / `SetReadDeadline`。gorilla/websocket 自身不内置任何超时。

在 TCP 半开连接（LB/NAT 空闲回收、对端宿主机宕机、网线拔出，都不会送来 FIN/RST）下，故障链为：

1. TCP 发送窗口填满，`conn.WriteMessage` 在 `writePump` 中永久阻塞；
2. `r.writes`（容量 `WriteQueueSize`，默认 256）灌满；
3. 所有写入方阻塞在 `sendFrame` 的第一个 select；
4. 心跳看门狗 `heartbeat → sendUnread → sendFrame(r.ctx, ...)` 也被堵死，而 `r.ctx` 只有 `fail()` 才会取消 —— 看门狗被它本该监管的队列卡住；
5. `fail()` 的三个触发点（writePump 出错、readPump 出错、heartbeat 超时分支）此刻全部不可达，`r.done` 永不关闭、`r.ctx` 永不取消。

结果：每条假死连接永久泄漏 writePump/readPump/heartbeat/monitor 及所有阻塞中的 Send goroutine，`monitor` 永不触发重连，客户端永久停止收发消息，而 `Health()` 仍汇报 `Connected: true`。只有显式 `Close()`（其中 `r.conn.Close()` 强行解开阻塞的 Write）才能恢复。

**修复**（`sdk/connection.go`）

- `writePump`：每次写前 `r.conn.SetWriteDeadline(time.Now().Add(cfg.SendTimeout))`，写超时立即返回错误并触发 `fail`。
- `readPump`：每次读前 `r.conn.SetReadDeadline(time.Now().Add(cfg.HeartbeatTimeout))`，对端静默超过心跳超时立即 `ReadMessage` 超时并 `fail`。
- `sendUnread`：改用 `context.WithTimeout(r.ctx, cfg.SendTimeout)`，看门狗不再继承无界阻塞语义。
- `heartbeat`：`sendUnread` 失败时显式 `r.fail(err)`（而非静默 return），确保写超时也能拆链。

**验证**：`go build`、`go vet`、`go test -race ./...` 通过。

---

### 3.2 [P0] 毒消息 → 无限重连风暴，退避永不升级

**问题**
`queue.go` 的 `processBatch` 在 `parseMessage` 失败或 handler 返回 error 时，重试 `HandlerMaxAttempts` 次后调用 `r.fail(ErrHandlerFailed)` 直接拆掉整条 websocket；`monitor` 判定非终态 → 重连。两个缺陷叠加：

- `q.key` 只在成功后才推进，服务端会重投同一批毒消息；
- `reconnect` 每次都是全新调用、`attempt := 1` 从头开始，`reconnectDelay` 永远返回 5–10s，退避在连接代际之间完全不累积。

净效果：一条解不开或业务上不接受的消息造成永久「5–10s 重连 + 重新 provision + 重新 sync」循环，消息永远无法推进，机群规模下是对 msync 的自伤式 DDoS 外加无上限日志膨胀。

**修复**

- 毒消息死信（`sdk/queue.go`）：
  - `processMetas` 对单条消息改为"失败即死信并继续同批次其他消息"，不再向上返回 error 触发拆链；
  - `deliverMessage` 对单条消息做 `HandlerMaxAttempts` 次重试，handler panic 被 per-message `recover` 捕获为错误；
  - `processBatch` 不再因处理失败 `r.fail`，改为死信推进 key。
- 跨代际退避（`sdk/connection.go` + `sdk/client.go`）：
  - Client 新增 `backoffAttempt atomic.Uint32` 与 `lastStableConnect atomic.Int64`；
  - `reconnect` 的 attempt 从共享计数续接，连接只有在稳定运行超过 `reconnectStableWindow`（5 分钟）后才重置退避档位。

**验证**：`go build`、`go vet`、`go test -race ./...` 通过。

---

### 3.3 [P0] C++ 节点预算无符号下溢

**问题**
私有 native codec 实现的 `valid_message_budget` 与 `em_codec_decode_frame` 内层循环中，对单 content 的 KV 数量做预算检查时：

```cpp
size_t values = c.params_size() + c.customexts_size();
if (nodes > 4096u - values) return false;   // values 未预先 <=4096 校验
```

`values` 来自解析后的 protobuf，未像编码侧 `valid_request_budget` 那样先做 `valid_count` 校验。单个 content 的 KV 数超过 4096 时，`4096u - values` 下溢为巨大数，检查被绕过，节点预算（4096）失效。虽仍有 16MiB 输入上限兜底，不构成内存安全崩溃，但削弱了畸形输入的 DoS 防线。

**修复**（私有 native codec 构建工程）

```cpp
if (values > 4096u || nodes > 4096u - values) return false;
```

两处（`valid_message_budget` 与 decode 内层循环）均加 `values > 4096u ||` 前置守卫。

**验证**：`c++ -std=c++11 -Wall` 单独编译 `codec.cpp` 通过（仅 protobuf 2.6.1 的 OSAtomic 弃用告警，无 error）。

---

### 3.4 [P1] native 开发适配器 finalizer + 零同步（UAF/double-free 地雷）

**问题**
`internal/protocol/native/native_darwin_arm64.go` 注册了 `runtime.SetFinalizer(c, func(x *Codec) { x.Close() })`，而该类型 `Codec struct{ h *C.EMCodec }` 没有任何同步（与正确使用 `sync.RWMutex` 的 `nativecodec.Codec` 形成对比）。一旦被接入：

- finalizer 在 GC goroutine 上运行，与另一 goroutine 正在进行的 `C.em_codec_*` 调用并发，`em_codec_destroy` 在使用中释放句柄 → use-after-free 段错误；
- 手动 `Close()` 后 finalizer 再 `Close()`：`c.h = nil` 与 finalizer 的 `if c.h != nil` 都是无屏障普通读写，可能观察到置 nil 前的值 → 重复 `delete` → 堆破坏 / double-free。

该包带 build tag `darwin && arm64 && cgo && nativecodec`，当前无任何文件 import，属死代码；但它是摆在正确实现旁边的一颗上膛枪。

**修复**：删除 finalizer，改为显式 `Close` 契约并加注释说明。

**验证**：`gofmt -e` 通过，`CGO_ENABLED=1 GOARCH=arm64 go build -tags nativecodec` 通过。

---

### 3.5 [P2] `LoginState`/`ConnState.String()` 越界 panic

**问题**
`client.go` 的 `String()` 直接对复合字面量取下标 `[...]string{...}[s]`，枚举范围外的值会 `index out of range` panic。`HealthStatus` 是公开且可 JSON 反序列化的结构体，含 `LoginState` 字段，把不可信 JSON 反序列化回来再打日志即可触发 panic；且每次调用都重新分配数组。

**修复**：改为 `switch` + `default` 分支，返回 `"login_state(%d)"` / `"conn_state(%d)"`。

**验证**：`go build`、`go test -race` 通过。

---

## 4. 性能问题修复

### 4.1 [P1] readPump 队头阻塞 → 心跳误判超时 → 反复重连

**问题**
`readPump` 同步调用 `dispatch`，`dispatch → handleBatch` 在 `HandlerConcurrency`（默认 4）个槽位全忙时阻塞于信号量。readPump 一旦阻塞，所有帧都停止派发 —— 包括唯一能推进 `lastPong` 的 UNREAD 回包。心跳判据 `time.Since(lastPong) > HeartbeatTimeout`（默认 240s）会把一条完全健康的连接误杀；重连后同样的积压再次复现。更糟的是 `processBatch` 在持有 handler 槽位时同步调用 `pullQueue → sendFrame`，会因写背压阻塞，槽位在网络写入期间也被占用。

**修复**（`sdk/connection.go` + `sdk/queue.go`）

- 用固定 worker 池（`HandlerConcurrency` 个）+ 有界 `batches` 队列（容量 `WriteQueueSize`）替代"阻塞式信号量 + 每批一个 goroutine"；
- `readPump` 非阻塞入队，UNREAD 回包（keepalive）处理不再被 handler 背压卡住；
- 队列满视为"消费不过来"的背压信号，`fail` 断开让对端重投（新增错误码 `ErrHandlerBacklog`），并配合跨代际退避避免二次风暴；
- worker 不计入 `r.wg`，保持"不可信 handler 无法阻塞 `Close`"的既有决策。

**验证**：`go build`、`go vet`、`go test -race ./...` 通过。

---

### 4.2 [P1] 队列活跃数 O(n) 持锁扫描

**问题**
`queueBacklog()` 持 `queueMu` 全量扫描 `r.queues`，而它被 `Health()` 在持有 `c.mu.RLock()` 的情况下调用。监控端点高频轮询 `/health` 时会与派发路径争抢 `queueMu`，并存在跨锁（`c.mu.RLock → queueMu`）的脆弱锁序。

**修复**：新增 `activeQueues atomic.Int32`，`startQueue` 置 active 时 +1、`processBatch` 置 inactive 时 -1，`queueBacklog()` 改为 O(1) 原子读，不再持锁扫描。

**验证**：`go build`、`go test -race` 通过。

---

## 5. 行为变更（需业务方知悉）

1. **投递语义由 at-least-once 降级为 best-effort + 日志可见**（毒消息场景）：
   - handler 持续失败 / panic、消息体损坏的消息，现在**死信并推进 key**，不再拆链重连重投；
   - 对应日志（error 级）：
     - `dropping undecodable message`
     - `dead-lettering message after handler failures`
     - `message batch failed; advancing queue (dead-letter)`
   - 建议把这些日志接入告警，并通过 REST 拉取积压作为业务兜底。README 中"handler 返回 nil 后才确认队列进度"的表述需同步更新。

2. **新增错误码 `ErrHandlerBacklog`（`HANDLER_BACKLOG`）**：表示 handler 消费不过来的背压断开，属非终态，会触发重连（带跨代际退避）。

3. **重连退避跨代际累积**：连接稳定运行超过 `reconnectStableWindow`（5 分钟）才重置退避档位；毒消息/慢消费者导致的频繁重建会让退避逐步升级到 60–120s 档。

---

## 6. 验证结果

| 检查项 | 结果 |
|---|---|
| `go build ./...` | 通过 |
| `go vet ./...` | 通过 |
| `go test -race ./...` | 通过 |
| `gofmt -l sdk/ internal/protocol/native/` | 无输出（格式正确） |
| `CGO_ENABLED=1 GOARCH=arm64 go test -tags nativecodecdev ./...`（native 适配器） | 通过 |

---

## 7. 遗留事项（未在本次修复）

1. **handler 忽略 context 的资源泄漏（Go 语言约束，只能缓解无法根治）**：`context.WithTimeout` 是协作式的，无法终止一个不检查 ctx 的阻塞调用。若 handler 长期阻塞且不响应 context，对应的 batch worker 会永久占用槽位。已采取措施：**(a) worker 池已提升为 Client 级固定池**（`HandlerConcurrency` 个，跨连接代际共享，重连不再创建新 worker），把"每次重连累积阻塞 worker"变为"整个 Client 固定上限"——handler 全阻塞时表现为受控停滞（背压断连 + 跨代际退避），而非 goroutine 无限增长；(b) `deliverMessage` 的 watchdog 在 `HandlerTimeout` 后记录 error 告警；(c) `Health().StuckHandlers` 原子计数供运维告警；(d) `MessageHandler` 类型注释写明契约（遵守 ctx、at-least-once + MetaID 幂等）；(e) 配置上限校验（HandlerConcurrency≤64、WriteQueueSize≤65536、MaxFrameBytes≤16MiB、HandlerTimeout≥1s）。**业务方必须保证 handler 响应 context、限制阻塞并自行管理外部资源**；如需硬性保证不宕机，唯一方案是 handler 进程隔离（见第 6 点），首版不引入。

2. **队列条目不删除**：`r.queues` 仍只标记 `active=false` 不删除（保留 `key` 进度，避免下次 NOTICE 从 0 重拉导致重复投递）。单队列几十字节，属温和增长，若用户规模极端可后续引入"key 外置 + 条目回收"。
3. **`reconnectStableWindow` 当前硬编码为 5 分钟**，可提为 `Config` 可配项。
4. **每帧双重 protobuf 解析**（Go 先解 envelope 再交 C++ 再解）：既有性能优化项，非正确性缺陷。
5. **未建立 protobuf wire fuzz corpus 与长期 soak/benchmark 门禁**：正式 GA 前建议补充 native decode fuzz、连接/关闭随机交错与目标吞吐压测。
6. **handler 进程隔离（可选硬性保证）**：若业务要求"handler 无论如何不能拖垮进程"，唯一能提供硬性保证的方案是把 handler 放到独立进程/服务（SDK 进程经 RPC/IPC 调用，超时后由 supervisor 杀进程重启，并可限制内存/CPU/句柄）。在 SDK 进程内直接执行 handler 时，Go 无安全的强制终止任意 goroutine 能力，无法对永久阻塞的外部调用做绝对的"不宕机"保证。
7. 其余低价值 nit（`status_code` 用 -1 做哨兵、`SerializeToString` 返回值忽略、`errors.go` 子串匹配分类）未动，避免扩大改动面。
