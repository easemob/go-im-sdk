# Go IM SDK 安全与并发审计报告

审计日期：2026-08-12  
审计范围：Go WSS/REST/队列/回调生命周期、cgo 边界、C ABI、C++ protobuf-lite 编解码、Linux amd64/arm64 制品。

## 结论

当前工作树未发现可复现的野指针、堆越界、use-after-free、double-free 或 Go 数据竞争。审计中发现的高风险生命周期竞态、protobuf 非法枚举导致的进程 abort、回调锁死和 cgo 分配失败路径已修复并通过回归验证。

服务端已约束单条业务消息最大 5KB，因此 SDK 不额外把 5KB 硬编码成业务 API 限制。native 层保留 16MiB 输入/输出上限，仅用于畸形帧和非业务 C ABI 调用的防御；它不是服务端消息大小策略。

客户已确认目标运行环境为 glibc 2.28、GCC 8.5.0，并提供 Clang 18.1.8。AMD64 archive 已在 Rocky Linux 8 基线重建，smoke 和符号检查通过（最高 `GLIBC_2.14`、`GLIBCXX_3.4.21`）。ARM64 archive 仍待同等基线环境重建；此前 Bookworm ARM64 archive 只能作为开发验证制品。

## 已发现并修复

- Client 的初次连接、重连和 Close 之间增加生命周期取消与串行屏障；关闭后不会安装新连接，也不会在仍有连接操作时销毁 native codec。
- native Codec 使用 `RWMutex` 保护 opaque handle；Encode/Decode 与 Close 不会并发释放或 double-free。
- 所有公开 protobuf enum 输入在 setter 前使用生成的 `*_IsValid` 校验；非法 command、namespace、route、message type、content type 返回 `EM_CODEC_INVALID_ARGUMENT`，不再触发 protobuf 2.6.1 的 assert/abort。
- 空 UNREAD payload 不再调用 protobuf 2.6.1 的空消息序列化，消除了 sanitizer 检出的 `memcpy(NULL, NULL, 0)` UB。
- cgo 的字符串数组、content、KV 改为 C heap 分配，检查 `calloc` 失败并清理已分配资源；C++ 输出使用 `nothrow` 并返回错误。
- C ABI 输出 buffer 若未释放便复用会返回参数错误，避免旧指针被覆盖后泄漏；头文件已明确输入借用、输出释放和 getter 生命周期。
- frame/output 上限为 16 MiB；字符串上限 4096 字节；meta、content、KV、directed user 等使用总节点预算（4096）和索引边界检查，降低畸形输入的内存放大风险。
- 回调注册和读取使用同一 mutex，并在锁内取得函数快照；race detector 未发现数据竞争。
- 事件队列有界且非阻塞，慢回调不会反压 WSS read pump；Close 不等待 event dispatcher，回调内调用 Close 不会自死锁。
- MessageHandler 不计入 transport wait group；即使忽略 context，Close 仍能完成。handler 返回后先检查 run context，关闭后不会继续调用 codec 或推进 queue。
- Close 后清空 run/session 引用，避免 Client 长期存活时保留整棵连接状态。
- 修复 Logout 响应与对端立即关闭 websocket 的选择竞态；Linux race 环境连续 20 次 WSS 全链路测试通过。

## 锁与线程模型检查

- `Client.mu`：保护登录/连接状态、run、token/session、错误和 callbacks。
- `Client.connectMu`：串行初始连接/重连握手，并作为 Close 销毁 codec 前的操作屏障。
- `nativecodec.Codec.mu`：保护 C handle 生命周期；调用持读锁，销毁持写锁。
- `connectionRun.pendingMu`：保护待 ACK map；ACK channel 有缓冲，不在锁内等待消费者。
- `connectionRun.queueMu`：保护 queue 状态、key、active/processing；旧 generation 不能提交新 key。
- WebSocket 使用单 read pump、单 write pump，避免并发 writer。
- 原子变量用于 generation、message ID、last inbound/pong，无需额外读写锁。

没有发现必须新增读写锁的共享字段。继续修改时应维持“锁内只更新/快照，锁外执行回调和网络 I/O”的规则。

## 内存与边界检查

- C++ 所有 frame getter 在访问 repeated 字段前检查 index；Go 在释放 `EMCodecFrame` 前深拷贝字符串和字节。
- C ABI 不让 C++ 异常跨边界；异常转换为 `EM_CODEC_INTERNAL_ERROR`。
- ASan+UBSan codec smoke 通过，未报告 heap OOB、UAF、double-free、leak 或 UB。
- 严格 C11 头文件编译、静态链接和运行已验证；最终程序不依赖系统 protobuf，protobuf-lite 符号包含在 `.a` 且位于隔离 namespace。

## 性能评估

当前性能足以支撑首版，但仍有两项明确优化空间：

1. 接收 CHAT frame 时 C++ 会先解析 MessageBody，并为 content 生成 raw 副本；Go 队列处理时又调用 DecodeMessageBody。可以改成 frame 层只保留 meta payload，或一次返回规范化结构，减少一次 PB 解析和若干复制。
2. 发送链路目前可能先 EncodeMessageBody 发生 C→Go 复制，再 EncodeSync 发生 Go→C 复制；C ABI 已有组合编码能力，后续可接通为单次 native 调用。

此外 getter-per-field 会产生较多 cgo crossing。若压测显示消息吞吐受限，可改为单次批量结构或规范化 buffer。上述均是性能优化，不是当前正确性/内存安全缺陷。

## 已执行验证

- Linux ARM64、AMD64 C++ static archive build + native smoke：通过。
- ASan + UBSan native smoke：通过。
- Linux Go 1.21 ARM64 最终净化发布树：`go test ./...`、`go test -race ./...`、`go vet ./...`、`go build ./cmd/server`：通过。
- Linux Go 1.21 AMD64 最终净化发布树：同上：通过。
- macOS native developer adapter：单测、race、`GOEXPERIMENT=cgocheck2`：通过。
- WSS 登录、发送/ACK、收消息/提交 next_key、登出全链路在 race 下连续 20 次：通过。
- 发布包 allowlist、协议源码泄漏扫描、manifest SHA-256、module zip：通过。

## 剩余风险与发布前门禁

- glibc 2.28、GCC 8.5.0/Clang 18.1.8 基线已确认；正式发布前仍必须拿到以该基线重建的 archive 和符号检查结果。
- MessageHandler 的 timeout 是协作式：SDK 可以及时 Close，但无法强制终止一个忽略 context 的用户 goroutine；用户 handler 自身可能泄漏。文档必须要求 handler 响应 context、限制阻塞并自行管理外部资源。
- 事件队列满时会丢弃状态回调并记录 warning；这是保护网络 I/O 的既定策略。关键业务状态应通过 `Health()` 查询，后续可增加 telemetry 计数。
- 尚未建立 protobuf wire fuzz corpus 和长期 soak/benchmark 门禁。正式 GA 前建议加入 native decode fuzz、连接/关闭随机交错和目标吞吐压测。
- 通用 C ABI 调用者必须传有效、生命周期覆盖调用期且 NUL 结尾的字符串；Go wrapper 始终满足此约束。

总体风险评级：开发阶段 **低到中**；在确定并验证 Linux ABI 基线、补齐 fuzz/soak 后可降为发布可接受水平。
