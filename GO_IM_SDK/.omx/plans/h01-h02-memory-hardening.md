# go-im-sdk 稳定性与资源上界修复计划

## 目标与边界

本计划仅覆盖 H-01、H-02、M-03～M-07：修复入站 MSync 解码/批次/NOTICE 的资源上界、生命周期 context 语义、NOTICE 可靠拉取、WSS 多 host 恢复和默认消息 ID 回绕。保持 ACK/next_key、handler 重试、at-least-once 投递和 4 MiB WebSocket read limit 的既有协议语义。

约束：

- 不新增第三方依赖；字节预算使用仓库内的小型 mutex/atomic 计数器实现。
- 保持“预算耗尽时断开连接，由服务端重投”的既有策略。
- 对确定性、不可能靠重投恢复的协议超限，必须与临时 handler 背压区分，避免 poison batch 无限重连。
- 固定调优参数继续放在内部常量中，不扩大公开 Config；上线前用服务端硬限制与压测校准数值。

明确不在本轮处理：C-01、H-03、H-04、M-01、M-02。C-01 在当前测试阶段接受风险，后续由仓库配置统一忽略 `*.yaml`；另外四项不作为本轮实现、验收或发布前置条件。除非后续另行立项，本计划不为这些条目安排 PR、测试或门禁。

## 新增问题判定与优先级

| 问题 | 是否成立/需修 | 对 SDK 宿主服务 | 对远端 IM 后端 | 建议优先级 |
|---|---|---|---|---|
| M-03 Close/Logout 无视 context 等锁 | 成立，需修 | 关闭/发布可无限卡住，codec和连接资源延迟释放；默认 transport通常有界，不合作自定义 RoundTripper 时最明显 | 低，最多 session/Logout 延迟 | 中；作为 H-02 Close 生命周期前置 |
| M-04 最后 batch 窗口吞 NOTICE | 确定成立，需修 | 新消息实时性丢失，延迟到下一 NOTICE/重连且理论无上限 | 低到中，消息继续积压，补拉/重连增加少量负载 | 中高，正确性小修 |
| M-05 NOTICE queue/goroutine 无界 | 成立，需修 | 单连接代际内 map、JID、goroutine、GC和连接抖动可放大；多 Client线性叠加 | 中，可能形成 SYNC pull/重连风暴 | 高，和 H-02 一起治理 |
| M-06 只重连单个 DNS host | 成立，需修 | 单 host局部故障可让 Client长期离线，消息延迟和恢复重投显著 | 中，失败拨号集中到坏 endpoint，离线队列增长；无直接内存破坏 | 中，可用性改进 |
| M-07 默认 ClientMessageID 2^32 回绕 | 成立，需修 | ACK关联/结果不确定重试的幂等 ID可能复用；1k/s约49.7天、10k/s约4.97天可达 | 中，可能把新消息当旧幂等请求或关联旧结果；无资源耗尽 | 中，高吞吐部署应提前 |

5 KiB 是单条业务消息限制，不限制锁等待、NOTICE 数量/JID、DNS host恢复或 ID 计数，因此不能作为上述问题的缓解措施。

## 建议首版预算

| 预算 | 建议值 | 目的 |
|---|---:|---|
| envelope 总字段数 | 4,096 | 在解析 wire 数据时提前拒绝数百万最小字段 |
| 重建后的 envelope 总大小 | 16 MiB | 与 `EM_CODEC_MAX_INPUT_BYTES` 对齐，分配前检查 |
| 单个 SYNC Meta 数 | 256 | 限制对象和 cgo crossing 数量 |
| 单个 SYNC payload 合计 | 2 MiB | 在 `C.GoBytes` 前阻止大批 payload Go 拷贝 |
| 单个 SYNC 动态字符串合计 | 1 MiB | 限制 JID、directed users、status 等碎片化分配 |
| 单个 SYNC directed users 总数 | 1,024 | 限制小对象数量和 cgo crossing |
| 单个 SYNC 保守计费 weight | 8 MiB | 覆盖 payload、Meta/JID/string/slice、逐对象对齐及安全系数 |
| 单 Client 批次保留预算 | 32 MiB | 让字节预算早于 256 槽对象队列触发 |
| 同进程所有 Client 批次预算 | 256 MiB | 防止数百 Client 的上界线性放大 |
| DecodeFrame 分级瞬时预留 | 4/16/32/64 MiB | 按未压缩帧大小分档；压缩或无法可靠分类按最高档收费 |
| Decode admission 等待 | 初始 500 ms | 瞬时争用短等，超时才可恢复断链；按 decode p99 校准 |
| 同进程 DecodeFrame 瞬时预算 | 256 MiB | 小帧允许高并发，同时限制大/压缩帧并发压力 |
| 单连接同时激活 queue | 256 | 这是调度容量而非协议合法性上限，超出者进入 FIFO 递延 |
| 单连接 deferred queue | 受 tracked queue budget 约束 | 去重 FIFO，仅保存已有 queueState 引用，不重复持有 JID |
| 单个 tracked queue 最低计费 | 1 KiB | 即使 JID 很短也让 4 MiB run budget 严格导出最多 4,096 个 identity |
| NOTICE/JID 单组件/总字节 | 4 KiB / 16 KiB | 在 C→Go 字符串及tuple/map entry分配前拒绝异常 JID |
| 单连接 tracked queue weight | 4 MiB | 按 JID底层字符串、tuple key、map entry/bucket和queueState保守计费 |
| 同进程 tracked queue weight | 64 MiB | 防止数百 Client 的 queue state 线性放大 |
| pull workers / request channel | 8 / 256 | 取消 per-NOTICE goroutine，限制 queued/in-flight pulls |
| pull response timeout | 30 秒 | 对端接受请求但不返回 SYNC 时受控断链 |

这些是防御性起始值，不是服务端协议事实。合并前必须向服务端确认 `maxMetasPerSync`、`maxDecodedSyncBytes` 和 pull 响应 SLO；queue 的 256 仅是同时激活数，不再作为账号总 queue 数合同。如果诚实服务端可能超过单批值，先协调服务端分页/拆批，或在保持进程总预算不变的前提下调整单批/单 Client 值。

## 实施步骤

### 1. 先补回归测试，锁定现有正常行为

文件：

- `internal/protocol/nativecodec/envelope_test.go`
- 新增 `sdk/queue_backpressure_test.go`

先保留并扩展以下正常路径：

1. 单一 compression + payload 能正常解压，未知字段的原始字节及顺序保持不变。
2. compression=0 的规范 envelope 原样返回。
3. 小批 SYNC 能正常入队、由 worker 处理、推进 next_key，预算最终归零。
4. handler 正常、返回错误、panic 和响应 context 超时的现有语义不变。

这是安全修复的回归基线；测试落地后再修改生产代码。

### 2. H-01：删除按字段保存的解析模型，严格拒绝重复 singular 字段

文件：`internal/protocol/nativecodec/envelope.go:23-160`

实现方案：

1. 删除 `[]wireField`/`parseWireFields` 的全字段保存方式。
2. 新增 O(1) 状态的 wire scanner，一次遍历只记录：
   - 字段总数；
   - 唯一 compression 字段的 raw 起止位置和值；
   - 唯一 payload 字段的 raw 起止位置及内容切片；
   - 是否出现重复 field 4/9。
3. 每解析一个字段先增加计数；超过 4,096 立即返回协议限制错误，不继续扫描剩余输入。
4. field 4 或 field 9 出现第二次时立即拒绝。选择严格拒绝而不是 protobuf last-one-wins，原因是正常服务端应产生 canonical singular 字段，拒绝能获得真正单遍/O(1) 解析，并消除语义歧义。
5. compression=0 时在完成字段数、重复字段和 wire type 校验后返回原输入，不分配输出。
6. compression=1 时只解压唯一 payload，继续保留现有 16 MiB `LimitReader` 防线。
7. 根据两个已记录字段的位置，用原始输入片段拼装一次规范输出：compression 归零，payload 只输出一次，其余未知字段保持原始字节和顺序。
8. 使用 checked arithmetic 预计算确切输出长度；若大于 16 MiB，必须在 `make`/`append` 大分配前返回错误。仅进行一次按确切容量的输出分配。
9. Go 侧在 `internal/protocol` 定义共享的 16 MiB 常量；在启用 nativecodec 的 cgo 测试中断言它等于 `C.EM_CODEC_MAX_INPUT_BYTES`。纯 Go 的 `envelope.go` 不直接依赖 C 宏。
10. 重复 field 4/9、字段数超过4,096、解压结果/重建总长度超限都必须用 `%w` 包装 `internal/protocol.ErrLimitExceeded`；截断、非法 wire type、varint overflow和zlib malformed继续返回普通 malformed error。

兼容性依据：16 MiB限制作用于完整重建envelope，而不只是payload。旧实现理论上可生成`16 MiB payload + envelope overhead`，但`Codec.DecodeFrame`随后会把这份完整缓冲传给输入上限同为16 MiB的native ABI，因此这些输入原本也必然被下游拒绝；提前在Go大分配前拒绝不是受支持行为回归。

兼容性决策：重复 singular protobuf 字段在通用 protobuf 解析器中可能按 last-one-wins 接受，但本协议的正常发送方不应生成它。这里有意收紧入站协议，以安全边界优先。若服务端抓包证明存在合法重复字段，则退回“只规范化输出最后一个 field 4/9”的两遍 O(1) scanner，不能恢复 `[]wireField`。

### 3. H-01：增加恶意输入和 allocation 上限测试

文件：`internal/protocol/nativecodec/envelope_test.go`

必须加入：

1. 两个 field 9（第一个为空、最后一个为合法 zlib）返回错误，输出不产生。
2. 两个 field 4 返回错误。
3. 1,000,000 个最小字段的约 2 MiB 输入在第 4,097 个字段处失败。
4. 使用 `testing.Benchmark` 的 `AllocedBytesPerOp` 或等价稳定方法，对上述百万字段输入断言函数内额外分配小于 256 KiB；输入构造不计入测量。
5. 重建后的 envelope 总长度恰好等于 16 MiB 成功，多一个字节失败；解压 payload 自身为 16 MiB 时因仍有 envelope 开销，也必须在输出分配前失败。
6. payload 本身低于 16 MiB，但加上其他 envelope 字段后重建总长度超过 16 MiB，必须在输出分配前失败。
7. 截断 varint/bytes、varint overflow、非法 field number/wire type 继续失败；overlong 但未 overflow 的 varint 当前会被接受，测试锁定兼容行为，不借本修复额外收紧。
8. 添加 `FuzzDecompressEnvelopePayload` seeds：重复 field 4/9、字段风暴、压缩炸弹、字段顺序交换；性质为成功输出始终不超过 16 MiB，且 field 4/9 各自最多出现一次。
9. 从真实恶意 envelope 经 `Codec.DecodeFrame` 到 readPump 分类的集成测试，验证上述 H-01 limit errors最终成为 terminal `ErrProtocolLimit`；普通 malformed envelope仍为 `ErrProtocol`。

### 4. H-02：在 native → Go 深拷贝之前限制单批 Meta 和 payload

文件：

- 新增 `internal/protocol/limits.go`
- `internal/protocol/nativecodec/codec.go:342-440`
- `internal/protocol/model.go:113-154`

实现方案：

1. 在 `internal/protocol` 定义共享的 `MaxSyncMetas`、`MaxSyncPayloadBytes`、`MaxSyncStringBytes`、`MaxSyncDirectedUsers` 和 `MaxSyncRetainedWeight`，供 codec 和 SDK 同时使用。
2. 将 `decodeFrame`/`readSync` 改为可返回 error。
3. 在进入 Meta 循环前读取 `em_codec_frame_meta_count`；超过 256 立即失败，不复制任何 payload。
4. 每次取得 `em_codec_meta_payload(..., &n)` 后，先用 checked addition 累加 payload 字节；累计超过 2 MiB 时，在调用 `C.GoBytes` 前失败。
5. 在复制字符串和 directed users 时同时限制动态字符串总字节≤1 MiB、directed users 总数≤1,024；必须在 Go 分配前检查。对 C ABI 的 NUL 结尾 `char*` 使用 `strnlen(remaining+1)` 或等价有界 helper，确认额度后才调用 `C.GoStringN`，不能先 `C.GoString` 再计费。
6. 完成 Go 对象构造后，调用确定性的 `SyncRetainedWeight`：使用 `unsafe.Sizeof`、slice 的 `cap` 和所有动态字符串/字节长度，计入 Status/RedirectInfo、queue JID、`Sync`/`Meta`、payload、JID、Ext、directed users 及 slice backing。每个动态分配分别按 Go size-class 的保守粒度向上取整并增加固定 allocator 费用，再对合计应用明确安全系数，最后按 4 KiB 向上取整；溢出或超过 8 MiB 时失败。该 weight 是批次对象的保守计费值，不宣称覆盖 `connectionRun` 等共享对象或 Go runtime 的全部 RSS。
7. 这些确定性单批限制用 `%w` 包装 `internal/protocol.ErrLimitExceeded`。`sdk/connection.go:282-288` 必须用 `errors.Is` 映射为新的 `ErrProtocolLimit`，其他 malformed/native error 仍映射既有 `ErrProtocol`。
8. 把 `ErrProtocolLimit` 列入 `isTerminal`，避免同一个不可拆分的 poison batch 自动重连循环。明确并测试其现有 terminal 副作用：Client 清空 user/token/session、进入 Logout，业务收到断连后必须修正服务端或重新 Login。
9. 当前 `decodeFrame` 在识别 kind 前就调用 `readStatus`；需先读取 frame kind。对 SYNC 创建同一个 string tracker并同时传给 `readStatus`、Redirect 和 `readSync`，确保 status/redirect/queue/meta 的全部字符串共享1 MiB额度且均在 Go 分配前检查。非SYNC frame保持业务语义和native 16 MiB总上限，同时复用第6.3节的通用JID分配前限制。

说明：native protobuf 对象在检查前已解析，仍可能产生受 16 MiB native 上限约束的瞬时 C++ 内存；这里的早检目标是阻止额外的大规模 C→Go 深拷贝，并为后续排队提供可信 weight。

### 5. H-02：实现解码 admission gate 与两级批次字节预算

文件：

- `sdk/client.go:146-211,247-259`
- `sdk/connection.go:23-27`
- `sdk/queue.go:68-115`
- `sdk/errors.go:16-45`

先实现一个无第三方依赖、带溢出检查的 `byteBudget`：

```go
type byteBudget struct {
    mu    sync.Mutex
    used  int64
    limit int64
    wait  chan struct{}
}

func (b *byteBudget) TryAcquire(n int64) bool
func (b *byteBudget) Acquire(ctx context.Context, n int64) error
func (b *byteBudget) Release(n int64) error
func (b *byteBudget) Used() int64
```

`TryAcquire`/`Acquire` 必须以 `n > limit-used` 判断，禁止使用可能溢出的 `used+n`；`Acquire` 在锁下取得当前 `wait` channel，额度释放时 close并换新 channel，随后在锁外 select `wait/ctx.Done()` 重试，不为每个等待者创建 goroutine。`n<=0`、`used<0` 或 `n>used` 的 Release 均返回 invariant error且不修改计数。测试中任何 invariant error 都直接失败，生产中记录 error 日志，不能静默产生负数。

依赖归属：`byteBudget`、共享process budget容器和基础并发/invariant测试必须先在独立基础提交C0落地。D1的tracked queue budget与E的decode/batch budget只复用这一实现，不得各自复制第二套计数器。

#### 5.1 DecodeFrame 瞬时 admission

1. sdk 包增加独立的 256 MiB process decode budget。复用 H-01 的 O(1) envelope scanner，在任何解压/大分配之前完成 fail-closed 分类并返回 reservation tier：

   | 输入类别 | 初始 reservation |
   |---|---:|
   | 明确未压缩且原始帧≤64 KiB | 4 MiB |
   | 明确未压缩且原始帧≤1 MiB | 16 MiB |
   | 明确未压缩且原始帧≤4 MiB | 32 MiB |
   | 压缩 envelope | 64 MiB |
   | wire 合法但无法可靠证明未压缩 | 64 MiB |

   在`internal/protocol`增加可选的`DecodeAdmissionEstimator`接口，由nativecodec基于同一scanner返回类别；测试/注入codec未实现时一律按64 MiB最高档。明确 malformed、重复 compression/payload 或字段数超限直接返回相应 protocol error；不允许把 ambiguous 输入归入低档。preflight与实际decompress各做一次有界O(n)扫描，但都不保存字段slice或做大分配。分档值是provisional release gate，必须用各类恶意 corpus 的 subprocess heap probe 校准，以实测 decode 峰值至少 2 倍裕量定稿；若任一档不足，必须提高该tier、收紧对应解码上限或重做process RSS容量评估，不能带着低估值发布。
2. admission 发生在 `ReadMessage` 返回之后、调用 `codec.DecodeFrame(data)` 之前。使用 `context.WithTimeout(r.ctx, decodeAdmissionWait)` 调用 `Acquire`，初值 500 ms、按最大帧 decode p99 校准；瞬时不足先等待，超时才以可重连的 `ErrHandlerBacklog` 断链。等待必须可被 run context 立即取消。
3. 不采用严格 FIFO weighted semaphore，额度释放后让等待者重新竞争，使小 reservation 不被一个大 reservation 队首阻塞。需要通过压力测试验证小帧不会长期饥饿，大帧等待超时率可观测。
4. reservation 持有到 `dispatch(frame)` 返回：若 SYNC 成功入队，此时 batch reservation 已接管长期 retained weight；其他 frame 或失败路径直接释放 decode reservation。
5. 该 gate 严格限制 process reservation 总和，但不单独证明完整 RSS 硬上界：gorilla/websocket 已在 admission 前持有 raw `data`，native parser也可能在 Go 侧 Meta/string 限制生效前分配 C/C++ 对象。最终容量规划必须加上 `活跃 Client 数 × 4 MiB raw frame`、native峰值和runtime/连接基础内存，并用 subprocess/soak 验证实测 RSS。若需要连 raw frame 都纳入硬上界，后续必须改为 `NextReader` 流式读取并在读前/读中计费。

#### 5.2 一次性 batch reservation

每个 job 不保存裸 `weight`，而是持有一次性 reservation：

```go
type batchReservation struct {
    client  *byteBudget
    process *byteBudget
    weight  int64
    mu       sync.Mutex
    released bool
}

func tryReserveBatch(client, process *byteBudget, weight int64) (*batchReservation, bool)
func (r *batchReservation) Release() error
```

接线与所有权规则：

1. 每个 Client 持有 32 MiB batch budget；sdk 包持有所有 Client 共享的 256 MiB process batch budget。它与 process decode budget是两个独立池，Health 分别展示。
2. `batchJob` 增加不可变 `reservation *batchReservation`；weight 由 reservation 暴露为只读值。
3. `handleBatch` 在向 channel 发送之前计算 weight，依次非阻塞获取 Client 和 process batch budget；第二级失败时 reservation 负责回滚第一级，然后以现有 `ErrHandlerBacklog` 断链。
4. enqueue 前失败或 `ctx.Done()`/channel default/full：所有权仍在 producer，由 producer 调用一次 `Release`。
5. enqueue 成功：所有权原子地转移给 channel/job，producer 此后不得释放。
6. worker 收到 job 后立即进入独立 wrapper，第一句安装 `defer job.reservation.Release()`，确保正常、错误、panic、旧 generation 和取消路径都只释放一次。
7. Close drain 收到的 job 由 drain 释放；`Release` 在 reservation mutex 下将 `released` 从 false 改为 true，重复调用返回 invariant error且不再修改任一 budget，必须在测试/日志中暴露。
8. 不删除 256 槽对象上限；对象上限与字节上限是互补防线。
9. 预算耗尽属于临时消费背压，继续使用 non-terminal `ErrHandlerBacklog`，由现有退避重连和服务端重投恢复；不要与确定性的 `ErrProtocolLimit` 混用。
10. 连接断开时旧 job 仍保持计费，直到 worker 丢弃/处理它，防止跨代际绕过预算。

#### 5.3 M-03：可取消 connect operation 与异步 Close finalizer

文件：`sdk/connection.go:63-71,538-630,701-798`、`sdk/client.go:146-179`、`sdk/dns.go:96-105`

不能继续使用“Close 无条件等待 `connectMu`”的方案；这会固化 M-03。改成 `Client.mu` 保护的单一 connect operation：

1. Client 增加 `connectDone chan struct{}`（nil表示空闲）、`closeOnce`、`closeDone`。`beginConnect(ctx)` 在锁下检查 closed和当前 operation：空闲则登记 done并取得所有权；占用则在锁外 select `connectDone/ctx.Done()` 后重试。`endConnect` 在所有路径关闭 done并清空当前 operation。
2. `Login`、reconnect和`Logout`统一走 begin/end helper。`Logout` 在取得 operation前不得清 user/token/session；等待超时原样返回 `ctx.Err()`，状态保持不变。reconnect取得所有权后再次核验 closed、旧 run和generation。
3. 首次 `Close` 立即在 `Client.mu` 下标记 closed、取消 eventCtx、detach当前 run并启动唯一 finalizer；调用方只 select `closeDone/ctx.Done()`。context先结束就返回 `ctx.Err()`，finalizer继续收敛。
4. 第二次及后续 `Close` 必须等待同一个 `closeDone`或自身 context，不能因为 `closed==true` 直接返回nil。
5. finalizer 顺序固定为：shutdown当前run并等待其run级finalizer释放queue状态 → 等待已登记connectDone真正结束及其临时/非current run完成各自finalize（closed后不会有新operation）→ 确认所有codec producer静默或由nativecodec RWMutex阻挡迟到调用 → drain尚未被worker领取的batch reservations → `codec.Close()` exactly once → close(closeDone)。queue reservations由第6.4节每个connectionRun自有finalizer释放，Client finalizer不得重复释放。
6. finalizer不等待永久阻塞的用户 MessageHandler；被其实际持有的 in-flight batch reservation继续计费到handler返回。已开始的 Send/codec调用依靠run取消、codec RWMutex和明确的producer guard安全结束。
7. 不把 `HTTPClient.Do` 简单套后台goroutine伪造取消：Go无法杀死任意不合作 RoundTripper，只会隐藏goroutine和迟到Response.Body泄漏。文档明确自定义 Transport必须遵守 `Request.Context`；若它永不返回，Login和finalizer可能永久驻留，但 Close/Logout调用方仍必须按自己的context返回，且codec不能被过早销毁。

返回契约必须写入 GoDoc/README：首次调用会立即把 Client 标为 closed；只有返回 `nil` 才表示共享 finalizer 已完成。所有要求有界时延的调用（包括第二次及后续 `Close`）都必须传 deadline；如果自定义 Transport 不合作，`Close(context.Background())` 可能永久等待。保留等待同一个 `closeDone` 的语义，因为“清理未完成却立即返回 nil”会错误承诺资源已销毁。

M-03确定性测试：blocking RoundTripper忽略request context；Login进入后短deadline Close快速返回`context.DeadlineExceeded`并立即呈closed/logout，release前codec未关闭，release后异步exactly-once关闭；Logout超时不清登录状态；首次Close超时后第二次Close用自身短deadline返回`ctx.Err()`，释放Transport后再次Close返回nil；Login/reconnect/Logout/Close交错race；保留callback内Close无自死锁测试；与batch/queue reservation drain组合测试。

### 6. M-04 / M-05：NOTICE 不丢失与有界 queue/pull 调度

文件：

- `internal/protocol/nativecodec/codec.go:99-106,361-431`
- `sdk/queue.go:11-65,68-152`
- `sdk/connection.go:37-56,168-184,217-260`
- `sdk/client.go:124-137,268-284`
- `README.md:77-82`

#### 6.1 M-04 NOTICE pending 状态机

1. 用显式 `idle/deferred/active` phase 代替单一 `active bool`；`queueState` 同时保留 `processing`、`noticePending`、`pullQueued`、`pullInFlight` 和 cursor key。`deferred` 表示已在 FIFO 等待 active slot，不能重复入队。
2. connectionRun 增加有界、预分配的 `deferredQueues` ring/deque，只保存已有 `*queueState` 引用，不复制JID底层字符串或构造持久拼接ID；其容量由 tracked queue budget和每queue最低计费导出。
3. 新 NOTICE 命中 active queue：只设 `noticePending=true`并合并重复通知；processing时不能立即发第二个pull，否则返回的SYNC会被现有processing分支丢弃。命中deferred queue不重复入FIFO；命中idle queue则在有active slot时转active，否则转deferred并排入FIFO。
4. 非最后batch处理完成后，本来就会按新key继续pull：清除pending，由该continuation覆盖通知，恰好调度一次。
5. `IsLast`或`NextKey==0`时：若pending=true，则清pending、保持active和activeQueues不变，使用已更新/保留的q.key立即调度一次pull；若无pending则转idle、释放active slot，并从FIFO提升最老的deferred queue。
6. phase、pending、processing、key、FIFO入出、active计数和pull调度决策必须在同一queueMu临界区原子完成，网络/codec操作在锁外执行。将状态转换抽成纯helper，便于确定性交错测试。

#### 6.2 M-05 有界 pull scheduler 与响应超时

1. 每个connectionRun启动8个固定pull workers和容量256的`pullRequests` channel，纳入run WaitGroup；删除`go r.pullQueue`。每个queue增加`pullQueued/pullInFlight/pullSentAt`，任何时刻最多一个queued/in-flight pull。
2. 任意时刻 `activeQueues<=256` 且每个active queue最多一个queued/in-flight pull，因此正常状态下 `pullRequests` 不应满；channel满视为状态机 invariant 破坏，记录完整计数并以`ErrHandlerBacklog`受控断链，而不是把第257个NOTICE判成协议错误。worker使用`context.WithTimeout(r.ctx, sendTimeout)`覆盖等待write queue和写完成；writePump原有socket deadline继续保留。
3. pull写成功后标记outstanding；收到该queue的SYNC时清除。使用一个run级watchdog扫描最老`pullSentAt`，超过初始30秒仍无SYNC响应则按网络/协议停滞断链；不要为每个queue永久创建timer goroutine。
4. Close/fail取消pull workers/watchdog并等待其退出；M-03 finalizer只有在这些producer静默后才能drain reservation和关闭codec。

#### 6.3 JID、queue数量与字节预算

1. 重构nativecodec `goString/goJID` 为可返回error的有界helper；NOTICE JID每组件≤4 KiB、总≤16 KiB，在`C.GoStringN`前用有界strnlen检查。该helper同时用于UNREAD/NOTICE/SYNC/Meta，避免非SYNC路径绕过字符串分配上限。
2. PR D1只启用有明确资源依据的wire/JID长度限制；仓库没有证明`/`、控制字符、全空组件或其他字符组合在四个独立wire字符串中一定非法，因此这些规则不能在D1 terminal拒绝，统一移到D2 observe-only/合同验证。
3. queue map key改用可比较的结构体元组`{AppKey, Name, Domain}`，避免`BareID()`字符串拼接和分隔符碰撞；NOTICE建表和SYNC查表必须调用同一个`canonicalQueueIdentity`。首版只采用已有代码注释和真实流量支持的规则：Resource不参与identity，AppKey/Name/Domain按原值精确匹配。若服务端确认空值等价，必须在NOTICE和SYNC两侧同时归一化，不能只改一侧。
4. 创建新identity前计算queue reservation：计入JID四个底层字符串、tuple key、map entry/bucket、queueState、FIFO引用及allocator裕量，且每个identity最低按1 KiB收费。分别获取单run 4 MiB和进程共享64 MiB budget；因此即使全是最短JID，单run tracked identities也严格不超过4,096，无需再把固定1,024计数当协议合同。
5. active slot满时不获取新的active额度、不报错、不断链，而是把已成功计费的新queue放入去重FIFO；active释放后按FIFO提升。deferred、idle和active都属于known状态并持续计费，重复NOTICE不重复收费。
6. 单run/process tracked queue budget或FIFO容量耗尽时，在map insertion和调度前回滚reservation，使用带`reason=queue_capacity`的non-terminal `ErrHandlerBacklog`受控断链并采用不短于普通重连退避的capacity backoff；不得进入永久Logout。已知identity的NOTICE仍可继续处理到断链发生。
7. inactive entry不能直接TTL/LRU删除，因为其中key用于增量同步，`sdk/queue.go:138-140`已说明丢key可能从0错误重拉/漏拉。FIFO只限制激活顺序，不能解决cursor长期累积；只有服务端确认key=0安全、提供queue enumeration/持久cursor，或提高经内存预算验证的容量后，才能支持超过本地硬容量的合法大账号。持续超过容量的账号可能反复重连但不会OOM，这是首版明确的已知限制。
8. 字符集合、全空JID及AppKey/Domain/Resource语义allowlist拆到可选PR D2：先以observe-only指标验证生产抓包和服务端合同，再启用terminal拒绝；D2不得阻塞M-04/M-05正确性和资源修复发布。

#### 6.4 connectionRun 自有资源终结

1. 每个`connectionRun`增加`finalizeOnce/releaseQueuesOnce`。`r.finalize`只在`r.cancel`且read/write/pull/watchdog producers全部退出后执行：在queueMu下detach并清空queues/pullRequests状态，再exactly-once释放该run的全部queue reservations。
2. `shutdown`、monitor观察到done、failed provision、redirect中间run、superseded reconnect、terminal fail、Logout和Close全部调用同一个run finalizer；sync.Once允许monitor与显式shutdown竞合但只释放一次。
3. 取消run后旧batch job不会推进key：它在访问queue前检查r.ctx/generation；即使job仍通过r指针保留connectionRun，已detach的queue map也可回收。测试覆盖未安装临时run和非current run，不能只测Client Close持有的当前run。

#### 6.5 可观测性

1. `HealthStatus` 增加：
   - `BatchBacklog`；
   - `BatchBacklogBytes`；
   - `BatchBudgetBytes`；
   - `ProcessBatchBacklogBytes`；
   - `ProcessDecodeInFlightBytes`；
   - `DecodeAdmissionWaiters`、按tier的wait duration/timeout计数；
   - `KnownQueues`/`DeferredQueues`/`TrackedQueueBytes`；
   - `PullsQueued`/`PullsInFlight`/最老pull等待时长；
   - `PendingNotices`；
   - `ClientBatchBudgetRejects`/`ProcessBatchBudgetRejects`；
   - `DecodeAdmissionTimeouts`/`QueueCapacityReconnects`；
   - `RejectedBatches`/`RejectedQueues`/`RejectedNotices` 单调计数。
2. 断链日志包含batch/queue weights、`budget_scope=client|process`、Client/process used/limit、active/known/deferred/pending queue count、pull queued/in-flight/age、decode tier/wait、meta_count、queue id和拒绝原因；不得记录payload或完整敏感JID。灰度单列process batch budget争用断链率、decode admission timeout率和连续queue-capacity重连次数，作为256 MiB/等待时长/queue budget调参依据。

### 7. H-02：并发、生命周期和内存上界测试

文件：新增 `sdk/queue_backpressure_test.go`，必要时扩展 `sdk/client_test.go`。

必须覆盖：

1. `byteBudget` 并发 `TryAcquire/Acquire/Release` 在 `go test -race` 下 used 永不小于 0、永不超过 limit；等待者在Release后继续、context取消立即退出且不泄漏goroutine。
2. 正常入队增加两级 used，worker 完成后两级均归零。
3. channel full、ctx canceled、Client budget exhausted、process batch budget exhausted、decode等待成功与超时分别验证无额度泄漏。
4. 填入不同大小 job，证明队列在达到 32 MiB Client budget 前后按字节拒绝，而不是等到 256 个对象。
5. 100 个测试 Client 竞争共享 process budget，证明累计 accepted weight 始终不超过 256 MiB；测试使用小比例注入预算，避免实际申请 256 MiB。
6. 旧 generation job 被丢弃后释放；新 generation 不能绕过旧 job 已占用的额度。
7. `Close` drain queued job；模拟一个永久阻塞 handler 时，仅 in-flight job 保持计费，Close 本身仍返回。
8. 加入 `Close` 与 `handleBatch` 成功入队并发的 race 用例，证明 producer 静默之后不会再出现新 job/额度泄漏。
9. 前256个queue进入active，第257个进入deferred FIFO；任一active完成后第257个以保存的cursor被提升。400个合法queue最终按FIFO全部获得调度，deferred的重复NOTICE不重复入队/收费；tracked queue budget耗尽在map insertion前以non-terminal `ErrHandlerBacklog(reason=queue_capacity)`受控失败且不进入Logout。
10. 单批 Meta=257、payload 合计超过 2 MiB、动态字符串超过 1 MiB、directed users超过1,024、保守 weight超过8 MiB 均在预期阶段失败。
11. typed/sentinel error 测试验证 `ErrLimitExceeded` 经多层 `%w` 后仍由 `errors.Is` 映射为 `ErrProtocolLimit`，普通 malformed error仍为 `ErrProtocol`；并验证 terminal 路径清空登录凭据的副作用。
12. 确定性 estimator 表驱动测试覆盖最大 Meta、不同 payload size class、大量短字符串、Status/Redirect、queue JID 和 directed users，证明 weight 不低于计划定义的保守公式。
13. fake WSS 集成测试：慢 handler + 多 queue SYNC 触发 `ErrHandlerBacklog`、连接关闭、accepted reservation 不越界；恢复消费后重连可继续处理重投消息且 next_key 不提前推进。
14. 独立 subprocess/benchmark 对每个decode tier和恶意corpus测 `HeapAlloc/TotalAlloc` 峰值并验证reservation至少保留2倍裕量；process reservation总和严格≤256 MiB。RSS、GC pause 和重连率放在外部 soak/压测门禁，不在普通单测里做易抖动的精确 RSS 断言。
15. M-04 barrier交错：final batch processing期间注入一个或多个NOTICE，释放handler后断言恰好一次follow-up pull、使用更新后的key且activeQueues不抖动；覆盖IsLast、NextKey=0、non-last continuation和inactive边界，并运行race。
16. M-05 使用注入的小cap和阻塞writer：断言pull workers/in-flight/channel/map/queue weight均不越界，不依赖易抖动的`runtime.NumGoroutine`；覆盖send deadline、sent-but-no-SYNC watchdog、cancel/Close pool退出。
17. NOTICE/JID表驱动测试覆盖超长组件/总长并证明超限在Go字符串和queue identity大分配前失败；全空、控制字符、分隔符、foreign AppKey/Domain和真实direct/group/system形态在D1必须安全进入结构体tuple且互不碰撞，仅在D2 observe-only/合同测试中决定语义，未确认前不得terminal拒绝。
18. 多Client竞争64 MiB process queue budget：获取失败只触发可重连backlog；持有者释放后等待方可恢复且未被terminal Logout。
19. run finalizer覆盖failed provision、redirect中间run、superseded reconnect、terminal fail、Logout、Close和monitor竞合；queue reservation/map exactly-once释放，未安装/非current run无泄漏。
20. canonical identity覆盖NOTICE带Resource而SYNC不带、精确AppKey/Name/Domain tuple匹配，以及包含分隔符时不同三元组仍不碰撞；服务端确认后的空值补全规则与非等价表示拒绝属于D2，避免把未证实规则混入D1。
21. decode admission分三类验证：64个4 MiB最小档同时持有时reservation恰好受256 MiB约束；100个同时到达的小帧在已校准decode p99和500 ms窗口内全部完成且零admission断链；刻意持有超过等待上限时，超出请求有界返回non-terminal backlog。另覆盖大/压缩帧等待后继续、malformed/ambiguous不被低档计费和各tier exactly-once释放。

### 8. M-06：保留DNS候选、失败轮换并抖动刷新

文件：`sdk/dns.go:24-179`、`sdk/client.go:146-169`、`sdk/connection.go:63-166,538-630`、`sdk/dns_test.go`

1. `resolveDNSPayload`不再只返回一个WSS字符串，改为有序去重的candidate set：所有合法priority=1按响应顺序在前，其余合法host随后；初期REST仍保持单endpoint，避免把本修复扩大到REST重试语义。
2. Client保存WSS candidates、当前index、成功的effective endpoint、DNS generation/refreshedAt。初次Login和重连对可重试的dial/TLS/handshake/timeout错误轮换下一candidate；鉴权、协议限制、handler backlog等非endpoint错误不得通过换host掩盖。
3. server redirect成功后记录`connectionRun.endpoint`为当前effective endpoint；普通断线先重连实际成功过的endpoint，失败后再回到剩余DNS candidates，不能无条件退回最初`msyncHost`。
4. 当前candidate一整轮失败后才重抓DNS。按`AppKey + resolver/HTTPClient identity`实现无第三方依赖的进程内coalescing（mutex+inflight done+cache），避免把不同自定义Transport的结果错误共享；默认HTTPClient的同AppKey Client可自然合并。等待支持context取消；设置初始5分钟fresh TTL、30分钟stale/physical TTL、最多256 entries和LRU/惰性清理。inflight entry不可提前淘汰，完成后恢复为可淘汰；若容量全被inflight占用，新key绕过缓存执行有界独立refresh，不能无界插入。刷新前加入随机抖动，失败在physical TTL内继续使用stale candidates。具体TTL/容量/抖动必须用服务端DNS更新策略校准。
5. endpoint失败指标记录host、失败阶段、轮换次数、DNS generation和刷新结果；不得把userID作为高基数label。现有重连退避仍保留，host轮换不能形成无延迟紧循环。
6. Go版只允许WSS/HTTPS，不实现native `switchProtocol`的明文降级；M-06只补安全协议内的host轮换和DNS refresh。

M-06测试：候选顺序/去重/非法项；第一host失败第二host成功；整轮失败只刷新一次并采用新generation；并发多Client同key refresh合并；大量不同key始终≤256 entries，fresh/stale/physical过期正确，inflight与eviction竞合不提前删除；redirect成功后普通断线不退回旧host；terminal auth/协议/handler错误不轮换；refresh等待可被Close/context取消；race。对照能力见`emclient-linux/include/emdnsmanager.h:241-253,288-291`。

### 9. M-07：单一完整uint64默认消息ID生成器

文件：`sdk/client.go:168-169,181-203,316`、`sdk/message.go:174-182,219-232`、`sdk/queue.go:287-320`、`sdk/message_test.go:262-273`

1. 删除`idPrefix | (idCounter & 0xffffffff)`。New时把完整32位随机值放到counter的bits 31～62、低31位清零并保证最高位为0，因此每个Client至少还有2^63个可自动分配ID，同时不降低现有32位随机熵。
2. `nextMessageID`改为CAS单调加一并返回`(uint64,error)`：current==MaxUint64后永久返回新增`ErrMessageIDExhausted`，绝不wrap到0。Send在codec编码、pending登记和网络写入前返回该错误。
3. 显式非零`SendRequest.ClientMessageID`不受自动生成器耗尽影响，继续原样传递并由pending map拒绝当前在途重复。
4. 删除`message.go`中第二套`messageIDCounter/nextClientMessageID`；`buildOutgoingMeta`变成纯构造函数并要求传入非零ID，所有生产和测试路径统一经Client生成器或显式业务ID。
5. README继续强调：自动ID只保证当前Client生命周期；结果不确定重试以及跨进程重启必须复用业务持久化的显式ClientMessageID。

口径说明：第二套生成器的低20位suffix每`2^20`次回绕，但完整ID还包含`UnixMilli<<20`，且当前公开`Send`正常路径先使用`Client.nextMessageID`，所以不能表述为“累计约100万条就确定性复用完整ID”。仍删除它，是为了消除双生成器分叉、测试旁路和时钟回退/同毫秒组合带来的潜在碰撞面；M-07的确定性生产缺陷仍是Client生成器低32位在`2^32`次后回绕。

M-07测试无需发送2^32次：把counter设到旧低32边界前后，证明继续递增且不等于首ID；设为MaxUint64-1，最后一次返回Max、之后稳定exhaustion且不编码/不登记pending/不写帧；并发CAS生成全唯一并跑race；显式ID原样；ACK仍关联完整uint64；自动ID永不为0。native wire已经使用完整`uint64_t`，仍需用真实服务做最大值邻近ID兼容验证。

## 错误语义

| 场景 | 错误码 | 是否自动重连 | 理由 |
|---|---|---|---|
| 重复 field 4/9、字段数超限、重建总长超限 | `ErrProtocolLimit` | 否 | 同一对端重发不会自愈 |
| 单批 Meta/payload/retained bytes 超限 | `ErrProtocolLimit` | 否 | 必须由服务端拆批或修正 |
| Client/process batch budget 暂时耗尽 | `ErrHandlerBacklog` | 是，沿用退避 | handler 恢复后可重投 |
| process decode admission 暂时耗尽 | `ErrHandlerBacklog` | 是，沿用退避 | 瞬时解码压力下降后可重投 |
| active queue 已满 | 无错误，进入deferred FIFO | 不断链 | 这是合法调度拥塞，不是协议违规 |
| 单run/process tracked queue budget或FIFO耗尽 | `ErrHandlerBacklog`（`queue_capacity`） | 是，使用capacity退避 | 保证内存有界但不永久Logout合法大账号 |
| NOTICE/JID长度或wire确定超限 | `ErrProtocolLimit` | 否 | 确定性资源/格式超限不会靠重连自愈 |
| D2已确认的JID语义违规 | `ErrProtocolLimit` | 否 | 仅在服务端合同和observe-only数据确认后启用 |
| pull request channel暂时耗尽 | `ErrHandlerBacklog` | 是，沿用退避 | 调度压力下降后可重投NOTICE |
| pull写成后长时间无SYNC响应 | `ErrTimeout` | 是，沿用退避/host轮换 | 视为当前连接或endpoint停滞 |
| Close/Logout等待connect operation时context结束 | 原样`ctx.Err()` | 不适用 | API按调用方deadline返回，finalizer可继续收敛 |
| 一轮WSS candidates均失败 | 现有网络错误，触发DNS refresh | 是，沿用退避 | 新generation host可能恢复 |
| 自动ClientMessageID到达MaxUint64 | `ErrMessageIDExhausted` | 不断连接 | 显式业务ID仍可继续发送，绝不wrap |

## 验收标准

1. 任何不超过 4 MiB 的入站 frame 都不能让 `decompressEnvelopePayload` 构造超过 16 MiB 的输出。
2. 包含重复 field 4/9 的 frame 在任何 payload 解压和大输出分配前失败。
3. 百万最小字段测试额外分配小于 256 KiB，并在读取到第 4,097 个字段后失败。
4. 任一已接受 SYNC：Meta≤256、payload 合计≤2 MiB、动态字符串≤1 MiB、directed users≤1,024、保守 retained weight≤8 MiB。
5. 任一时刻每 Client queued/in-flight batch weight≤32 MiB；所有 Client 的 batch reservation合计≤256 MiB；活跃 DecodeFrame reservation合计≤256 MiB。64个最小档可同时持有；100个小型ACK/UNREAD同时到达时在已校准p99/500 ms窗口内完成且零admission断链；超过等待上限按预期返回可恢复backlog。并发race测试中reservation始终不越界。
6. 每连接active queue≤256、tracked queue weight≤4 MiB、每identity最低计费1 KiB；全进程queue weight≤64 MiB。第257个queue递延且最终按FIFO激活；容量超限发生在map insertion/pull调度前，只触发可恢复capacity断链而不永久Logout。
7. 预算拒绝不会推进 next_key，服务端重投后不会丢消息；已成功完成的批次保持原有 at-least-once 语义。
8. 所有 acquire 路径都通过 reservation 实现 exactly-once release；Close/取消/旧 generation/channel full/panic/并发 producer 均无额度泄漏或双重扣减。
9. 正常联调抓包中的 canonical envelope 和现有 `meta_count<=8` 流量全部通过，不增加错误或断连。
10. final batch processing窗口内的NOTICE一定合并成恰好一次后续pull；同queue永远不出现两个queued/in-flight pull。
11. pull goroutine固定为8 workers，request channel≤256，pull response超过30秒受控断链；Close后worker/watchdog全部退出。
12. 进程queue budget争用只产生可恢复backlog；每个connectionRun在所有安装/未安装/redirect/superseded/terminal路径都exactly-once释放自有queue状态和reservation。FIFO不通过TTL/LRU丢弃cursor。
13. blocking自定义RoundTripper场景下，Close/Logout在自身context结束时返回；Close不会提前销毁codec，迟到operation结束后finalizer exactly once完成。
14. 可重试endpoint故障会遍历全部候选并在整轮后合并刷新DNS；cache≤256 entries并按physical TTL回收；terminal业务/协议错误不轮换；redirect成功后不错误退回旧host。
15. 自动消息ID跨旧2^32边界仍唯一，MaxUint64后稳定拒绝且不产生任何发送副作用；并发race全唯一。
16. client/process batch拒绝、各decode tier等待/超时及queue capacity重连都有分原因指标；灰度能单独判断共享256 MiB预算是否过紧。
17. `go test ./...`、`go test -race ./...`、`go vet ./...`、nativecodecdev测试和Linux amd64/arm64验证全部通过。

## 上线顺序

1. PR/提交A：H-01单遍envelope硬化与allocation/fuzz测试，优先紧急合并。
2. PR/提交B：M-07单一完整uint64 ID生成器；低耦合，可与A并行但独立发布。
3. PR/提交C：M-03 connectDone/closeDone生命周期协议，为后续reservation drain提供正确Close基础。
4. PR/提交C0：抽取唯一`byteBudget`、共享process budget容器及并发/invariant测试，供D1/E共同复用；不在本提交接业务策略。
5. PR/提交D1：M-04 pending NOTICE + M-05有界pull scheduler、deferred FIFO、JID长度限制、tuple identity、queue byte budgets和指标；active满只递延，容量满只可恢复断链。
6. PR/提交E：H-02单批限制、分级decode admission、短时可取消等待和batch/process reservations；基于C/C0/D1的finalizer、budget和scheduler实现，不能只缩小对象channel。
7. PR/提交F：M-06 WSS candidate轮换、进程内DNS refresh coalescing和endpoint指标；独立灰度验证局部host故障。
8. 可选PR/提交D2：生产抓包observe-only和服务端合同确认后，再启用字符集合、全空JID及AppKey/Domain/Resource语义allowlist；不阻塞D1/E/F。
9. 灰度观察分scope batch budget拒绝、各decode tier等待/超时、queue capacity重连、pending/pull age、batch/queue weights、GC/RSS、host rotation、DNS refresh和总重连率；合法流量超限优先让服务端拆批/修正合同，不直接放宽进程总预算。

## 风险与缓解

- **合法非 canonical envelope 被拒绝**：上线前抓包确认 field 4/9 singular；如确有重复，改为 O(1) 两遍 last-one-wins，不恢复字段 slice。
- **服务端总是重发同一个超大批次**：使用 terminal `ErrProtocolLimit`，并要求服务端拆批；不能把确定性超限当临时 backlog。
- **全局 budget 造成 Client 间相互影响**：Client 级预算提供公平上限，全局预算只在进程整体接近风险线时拒绝；通过 Health/计数器定位高占用 Client。
- **256 MiB process batch预算在数百Client突发时争用**：它是共享动态池而非静态平分；保留初值并单列`ProcessBatchBudgetRejects`及其导致的重连率，用soak结果调参，不能只因理论Client数直接放大上限。
- **decode分级计费低估native峰值**：未知类别一律按64 MiB，所有tier用恶意corpus和至少2倍实测裕量校准；reservation只作为受验证的并发压力边界，不冒充完整RSS证明。
- **误把逻辑 reservation 当完整 RSS 上限**：文档和指标明确分开 batch retained、decode transient 与已读 raw WebSocket buffer；整体容量规划仍需加上 Client 基础内存和 `Client数×4 MiB` 最坏 raw frame，并通过 subprocess/soak 校准。
- **永久阻塞 handler 导致 process budget 长期占用**：这是对真实 retained memory 的正确计费；最多 4 个 in-flight job/Client，并通过 `StuckHandlers` 告警。业务仍必须响应 context。
- **预算估算低估对象开销**：对固定结构使用保守费用并计算所有动态字符串/切片内容；Meta 数硬上限作为第二道防线；用 heap profile 校准常量。
- **不合作HTTP Transport永不返回**：SDK无法强杀用户代码；Close API按context返回但finalizer/codec可能驻留。文档要求Transport遵守Request.Context，并对异常Client数告警。
- **JID allowlist误伤真实队列形态**：D1只做资源长度限制并使用无分隔符拼接碰撞的tuple identity；字符集合、全空JID和业务语义全部留给D2，先用生产抓包覆盖AppKey空、direct Domain空、group conference domain和system queue，未验证前不发布terminal拒绝。
- **inactive queue错误回收导致游标丢失**：首版不做TTL/LRU；deferred FIFO不解决known cursor累积，超过字节容量的合法大账号可能反复重连但不会OOM。只有服务端证明key=0安全、提供持久cursor/enumeration或经容量验证提高预算后才可真正扩展。
- **DNS refresh羊群**：按`AppKey + resolver identity`合并inflight请求、最小刷新间隔和随机抖动；刷新失败保留stale candidates，不能每个Client独立紧循环请求bootstrap。
- **完整uint64 ID服务兼容性**：wire ABI已是uint64且旧随机prefix可能设置最高位；仍用真实服务验证边界ID。跨重启幂等继续要求业务显式持久ID。

## 预计改动文件

- `internal/protocol/nativecodec/envelope.go`
- `internal/protocol/nativecodec/envelope_test.go`
- `internal/protocol/nativecodec/codec.go`
- `internal/protocol/codec.go`
- `internal/protocol/model.go`
- 新增 `internal/protocol/limits.go`
- `sdk/client.go`
- `sdk/connection.go`
- `sdk/queue.go`
- `sdk/errors.go`
- 新增 `sdk/queue_backpressure_test.go`
- `sdk/dns.go`
- `sdk/dns_test.go`
- `sdk/login_test.go`及新增生命周期交错测试
- `sdk/message.go`
- `sdk/message_test.go`
- `README.md`
- `SAFETY_REVIEW.md`
