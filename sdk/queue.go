package sdk

import (
	"context"
	"errors"
	"fmt"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

const (
	maxActiveQueues      = 256
	pullWorkerCount      = 8
	pullRequestCapacity  = 256
	queueResponseTimeout = 30 * time.Second
	minQueueWeight       = int64(1 << 10)
	maxTrackedQueues     = int(connectionQueueBudgetBytes / minQueueWeight)
)

type queueKey struct {
	appKey string
	name   string
	domain string
}

func newQueueKey(j internalprotocol.JID) queueKey {
	return queueKey{appKey: j.AppKey, name: j.Name, domain: j.Domain}
}

func (k queueKey) String() string {
	// Queue components originate at the remote protocol boundary. Keep their
	// contents out of logs while still providing enough shape to correlate
	// malformed or stalled identities.
	return fmt.Sprintf("app_bytes=%d/name_bytes=%d/domain_bytes=%d", len(k.appKey), len(k.name), len(k.domain))
}

type queuePhase uint8

const (
	queueIdle queuePhase = iota
	queueDeferred
	queueActive
)

type queueState struct {
	identity      queueKey
	jid           internalprotocol.JID
	key           uint64
	phase         queuePhase
	processing    bool
	noticePending bool
	pullQueued    bool
	pullInFlight  bool
	pullSentAt    time.Time
	weight        int64
}

type deferredQueueRing struct {
	items []*queueState
	head  int
	size  int
}

func newDeferredQueueRing(capacity int) deferredQueueRing {
	return deferredQueueRing{items: make([]*queueState, capacity)}
}

func (q *deferredQueueRing) Len() int { return q.size }

func (q *deferredQueueRing) Push(state *queueState) bool {
	if state == nil || q.size == len(q.items) {
		return false
	}
	index := (q.head + q.size) % len(q.items)
	q.items[index] = state
	q.size++
	return true
}

func (q *deferredQueueRing) Pop() *queueState {
	if q.size == 0 {
		return nil
	}
	state := q.items[q.head]
	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return state
}

func validateQueueJID(j internalprotocol.JID) error {
	total := 0
	for _, component := range [...]string{j.AppKey, j.Name, j.Domain, j.ClientResource} {
		if len(component) > internalprotocol.MaxJIDComponentBytes {
			return newError(ErrProtocolLimit, "queue jid", fmt.Sprintf("component exceeds %d bytes", internalprotocol.MaxJIDComponentBytes))
		}
		total += len(component)
	}
	if total > internalprotocol.MaxJIDBytes {
		return newError(ErrProtocolLimit, "queue jid", fmt.Sprintf("total length exceeds %d bytes", internalprotocol.MaxJIDBytes))
	}
	return nil
}

func queueRetainedWeight(j internalprotocol.JID) int64 {
	// The fixed charge covers queueState, tuple/map headers and one potential
	// deferred-ring slot. Retained string backing bytes are charged in addition.
	bytes := minQueueWeight + int64(len(j.AppKey)+len(j.Name)+len(j.Domain)+len(j.ClientResource))
	return ((bytes + minQueueWeight - 1) / minQueueWeight) * minQueueWeight
}

func (r *connectionRun) ensureQueueStructuresLocked() {
	if r.queues == nil {
		r.queues = make(map[queueKey]*queueState)
	}
	if len(r.deferred.items) == 0 {
		r.deferred = newDeferredQueueRing(maxTrackedQueues)
	}
	if r.pullRequests == nil {
		r.pullRequests = make(chan *queueState, pullRequestCapacity)
	}
	if r.queueBudget == nil {
		r.queueBudget = mustNewByteBudget(connectionQueueBudgetBytes)
	}
	if r.queueTimeout <= 0 {
		r.queueTimeout = queueResponseTimeout
	}
}

func (r *connectionRun) queueHealth() (active, known, deferred, pulls int, tracked int64, rejects uint64) {
	r.queueMu.Lock()
	known = len(r.queues)
	deferred = r.deferred.Len()
	for _, q := range r.queues {
		if q.pullQueued || q.pullInFlight {
			pulls++
		}
	}
	if r.queueBudget != nil {
		tracked = r.queueBudget.Used()
	}
	r.queueMu.Unlock()
	return int(r.activeQueues.Load()), known, deferred, pulls, tracked, r.capacityHits.Load()
}

func (r *connectionRun) reserveQueueLocked(weight int64) (bool, error) {
	r.ensureQueueStructuresLocked()
	localOK, err := r.queueBudget.TryAcquire(weight)
	if err != nil || !localOK {
		return false, err
	}
	processOK, err := processQueueBudget.TryAcquire(weight)
	if err != nil || !processOK {
		releaseErr := r.queueBudget.Release(weight)
		if err != nil {
			return false, errors.Join(err, releaseErr)
		}
		return false, releaseErr
	}
	return true, nil
}

func (r *connectionRun) enqueuePullLocked(q *queueState) error {
	if q == nil || q.phase != queueActive || q.processing || q.pullQueued || q.pullInFlight {
		return nil
	}
	q.pullQueued = true
	select {
	case r.pullRequests <- q:
		return nil
	default:
		q.pullQueued = false
		return newError(ErrHandlerBacklog, "queue pull", "pull scheduler invariant: request queue full")
	}
}

func (r *connectionRun) activateOrDeferLocked(q *queueState) error {
	if r.activeQueues.Load() < maxActiveQueues {
		q.phase = queueActive
		r.activeQueues.Add(1)
		return r.enqueuePullLocked(q)
	}
	q.phase = queueDeferred
	if !r.deferred.Push(q) {
		return newError(ErrHandlerBacklog, "queue notice", "queue_capacity: deferred queue full")
	}
	return nil
}

func (r *connectionRun) promoteDeferredLocked() error {
	for r.activeQueues.Load() < maxActiveQueues {
		q := r.deferred.Pop()
		if q == nil {
			return nil
		}
		if q.phase != queueDeferred {
			continue
		}
		q.phase = queueActive
		r.activeQueues.Add(1)
		return r.enqueuePullLocked(q)
	}
	return nil
}

func (r *connectionRun) startQueue(j internalprotocol.JID) {
	if err := validateQueueJID(j); err != nil {
		r.fail(err)
		return
	}
	identity := newQueueKey(j)
	var scheduleErr error
	r.queueMu.Lock()
	r.ensureQueueStructuresLocked()
	q := r.queues[identity]
	if q != nil {
		// Keep the tuple components on the canonical backing strings retained by
		// the map key/q.identity. A repeated NOTICE is decoded into fresh strings;
		// retaining all of j would otherwise keep a second uncharged copy of the
		// three identity components. Resource is deliberately outside identity and
		// may be replaced after charging any growth.
		canonical := internalprotocol.JID{
			AppKey:         q.identity.appKey,
			Name:           q.identity.name,
			Domain:         q.identity.domain,
			ClientResource: j.ClientResource,
		}
		newWeight := queueRetainedWeight(canonical)
		if newWeight > q.weight {
			delta := newWeight - q.weight
			reserved, err := r.reserveQueueLocked(delta)
			if err != nil || !reserved {
				r.capacityHits.Add(1)
				r.queueMu.Unlock()
				reason := "queue_capacity: tracked queue byte budget exhausted while growing identity"
				if err != nil {
					reason += ": " + err.Error()
				}
				r.fail(newError(ErrHandlerBacklog, "queue notice", reason))
				return
			}
			q.weight = newWeight
		}
		// Never lower q.weight: replacing a large JID with a smaller spelling is
		// harmless over-accounting and avoids release/accounting races.
		q.jid = canonical
		switch q.phase {
		case queueActive:
			q.noticePending = true
		case queueDeferred:
			// The fixed FIFO already owns exactly one entry for this queue.
		case queueIdle:
			scheduleErr = r.activateOrDeferLocked(q)
		}
		r.queueMu.Unlock()
		if scheduleErr != nil {
			r.capacityHits.Add(1)
			r.fail(scheduleErr)
		}
		return
	}

	weight := queueRetainedWeight(j)
	reserved, err := r.reserveQueueLocked(weight)
	if err != nil || !reserved {
		r.capacityHits.Add(1)
		r.queueMu.Unlock()
		reason := "queue_capacity: tracked queue byte budget exhausted"
		if err != nil {
			reason += ": " + err.Error()
		}
		r.fail(newError(ErrHandlerBacklog, "queue notice", reason))
		return
	}
	q = &queueState{identity: identity, jid: j, weight: weight}
	r.queues[identity] = q
	scheduleErr = r.activateOrDeferLocked(q)
	if scheduleErr != nil {
		delete(r.queues, identity)
		if q.phase == queueActive {
			r.activeQueues.Add(-1)
		}
		q.phase = queueIdle
		_ = r.queueBudget.Release(weight)
		_ = processQueueBudget.Release(weight)
		r.capacityHits.Add(1)
	}
	r.queueMu.Unlock()
	if scheduleErr != nil {
		r.fail(scheduleErr)
	}
}

func (r *connectionRun) startPullScheduler() {
	r.queueMu.Lock()
	r.ensureQueueStructuresLocked()
	r.queueMu.Unlock()
	r.wg.Add(pullWorkerCount + 1)
	for i := 0; i < pullWorkerCount; i++ {
		go r.pullWorker()
	}
	go r.pullResponseWatchdog()
}

func (r *connectionRun) pullWorker() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		select {
		case q := <-r.pullRequests:
			if q != nil {
				r.executePull(q)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *connectionRun) executePull(q *queueState) {
	r.queueMu.Lock()
	if r.ctx.Err() != nil || q.phase != queueActive || !q.pullQueued || q.processing || q.pullInFlight {
		q.pullQueued = false
		r.queueMu.Unlock()
		return
	}
	q.pullQueued = false
	q.pullInFlight = true
	jid := q.jid
	key := q.key
	identity := q.identity
	timeout := r.queueTimeout
	r.queueMu.Unlock()

	frame, err := r.client.codec.EncodeSync(internalprotocol.SyncRequest{Queue: &jid, Key: key})
	if r.client.debug {
		r.client.logger.Debug("wss.queue_pull", "queue_shape", identity.String(), "resource_bytes", len(jid.ClientResource), "key", key)
	}
	if err == nil {
		ctx, cancel := context.WithTimeout(r.ctx, sendTimeout)
		err = r.sendFrame(ctx, frame)
		cancel()
	}
	if err != nil {
		r.queueMu.Lock()
		q.pullInFlight = false
		q.pullSentAt = time.Time{}
		r.queueMu.Unlock()
		if r.ctx.Err() == nil {
			r.fail(err)
		}
		return
	}
	r.queueMu.Lock()
	// A very fast response may already have cleared pullInFlight and moved the
	// queue into processing before sendFrame returns.
	if q.pullInFlight && !q.processing {
		q.pullSentAt = time.Now()
		if timeout <= 0 {
			q.pullSentAt = time.Time{}
		}
	}
	r.queueMu.Unlock()
}

func (r *connectionRun) pullResponseWatchdog() {
	defer r.wg.Done()
	timeout := r.queueTimeout
	if timeout <= 0 {
		timeout = queueResponseTimeout
	}
	interval := timeout / 4
	if interval <= 0 || interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			var stalled queueKey
			found := false
			r.queueMu.Lock()
			for _, q := range r.queues {
				if q.pullInFlight && !q.pullSentAt.IsZero() && now.Sub(q.pullSentAt) > timeout {
					stalled = q.identity
					found = true
					break
				}
			}
			r.queueMu.Unlock()
			if found {
				r.fail(newError(ErrTimeout, "queue pull", "SYNC response timeout for "+stalled.String()))
				return
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *connectionRun) handleBatch(d *internalprotocol.Sync) {
	if d.Status != nil && d.Status.Code != internalprotocol.StatusOK {
		r.fail(protocolError("sync", int32(d.Status.Code), d.Status.Reason))
		return
	}
	if d.Queue == nil {
		return
	}
	if err := validateQueueJID(*d.Queue); err != nil {
		r.fail(err)
		return
	}
	identity := newQueueKey(*d.Queue)
	r.queueMu.Lock()
	q := r.queues[identity]
	if q == nil || q.phase != queueActive || q.processing {
		r.queueMu.Unlock()
		return
	}
	q.pullQueued = false
	q.pullInFlight = false
	q.pullSentAt = time.Time{}
	q.processing = true
	r.queueMu.Unlock()
	resetProcessing := func() {
		r.queueMu.Lock()
		if current := r.queues[identity]; current == q {
			current.processing = false
		}
		r.queueMu.Unlock()
	}
	weight, err := internalprotocol.SyncRetainedWeight(d)
	if err != nil {
		resetProcessing()
		code := ErrProtocol
		if errors.Is(err, internalprotocol.ErrLimitExceeded) {
			code = ErrProtocolLimit
		}
		r.fail(wrapError(code, "sync batch weight", err))
		return
	}
	if r.client.batchBudget == nil {
		resetProcessing()
		r.fail(newError(ErrHandlerBacklog, "sync", "client batch budget is not initialized"))
		return
	}
	processBudget := r.processBatchBudget
	if processBudget == nil {
		processBudget = processBatchBudget
	}
	reservation, reserved, scope, err := tryReserveBatchScoped(r.client.batchBudget, processBudget, weight)
	if err != nil || !reserved {
		resetProcessing()
		switch scope {
		case batchBudgetScopeClient:
			r.client.clientBatchRejects.Add(1)
		case batchBudgetScopeProcess:
			r.client.processBatchRejects.Add(1)
		}
		reason := "batch byte budget exhausted"
		if err != nil {
			reason += ": " + err.Error()
		}
		r.fail(newError(ErrHandlerBacklog, "sync", reason))
		return
	}
	// 非阻塞入队：readPump 不能被 handler 背压卡住，否则 keepalive（UNREAD 回包）
	// 无法处理，健康连接会被心跳误杀。队列满时视为消费不过来的背压信号，
	// 断开让对端重投（配合跨代际退避，避免形成自伤式重连风暴）。
	select {
	case r.client.batches <- batchJob{queue: identity, d: d, r: r, reservation: reservation}:
	case <-r.ctx.Done():
		resetProcessing()
		if releaseErr := reservation.Release(); releaseErr != nil {
			r.client.logger.Error("batch budget release invariant failed", "error", releaseErr)
		}
	default:
		resetProcessing()
		if releaseErr := reservation.Release(); releaseErr != nil {
			r.client.logger.Error("batch budget release invariant failed", "error", releaseErr)
		}
		r.fail(newError(ErrHandlerBacklog, "sync", "batch queue full"))
	}
}

// batchWorker 消费 Client 级批次队列。worker 是 Client 级固定池，跨连接代际共享，
// 重连不会创建新 worker；不计入任何 WaitGroup：handler 是不可信的用户代码，可能
// 忽略 context 长期阻塞，计入 wg 会让 Close 永久等待。worker 依靠 eventCtx 退出。
// 取出任务后由 job.r.processBatch 内部的 ctx/generation 检查丢弃旧连接的任务。
func (c *Client) batchWorker() {
	for {
		if c.eventCtx.Err() != nil {
			return
		}
		select {
		case job := <-c.batches:
			c.processBatchJob(job)
		case <-c.eventCtx.Done():
			return
		}
	}
}

func (c *Client) processBatchJob(job batchJob) {
	defer func() {
		if job.reservation == nil {
			if c.logger != nil {
				c.logger.Error("batch job missing reservation")
			}
			return
		}
		if err := job.reservation.Release(); err != nil && c.logger != nil {
			c.logger.Error("batch budget release invariant failed", "error", err)
		}
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			if c.logger != nil {
				c.logger.Error("batch worker panic", "panic", recovered)
			}
			if job.r != nil && job.r.ctx != nil && job.r.ctx.Err() == nil {
				job.r.fail(newError(ErrHandlerFailed, "batch worker", fmt.Sprint(recovered)))
			}
		}
	}()
	if job.reservation == nil || job.r == nil || job.r.ctx == nil || job.r.ctx.Err() != nil ||
		job.r.generation != c.generation.Load() || (c.eventCtx != nil && c.eventCtx.Err() != nil) {
		return
	}
	job.r.processBatch(job.queue, job.d)
}

func (r *connectionRun) processBatch(identity queueKey, d *internalprotocol.Sync) {
	// processMetas 内部已对单条消息做重试与死信，只有 ctx 取消或 handler panic
	// 才会返回错误。这里把这两种情况也视为"该批次无法推进"，死信并推进 key，
	// 而不是拆链——否则重投会再次触发同样的问题，形成自伤式重连风暴。
	if err := r.processMetas(d.Metas); err != nil {
		if r.ctx.Err() != nil {
			return
		}
		r.client.logger.Error("message batch failed; advancing queue (dead-letter)",
			"queue", identity.String(), "next_key", d.NextKey, "error", err)
	}
	if r.ctx.Err() != nil {
		return
	}
	r.queueMu.Lock()
	q := r.queues[identity]
	if q == nil || r.generation != r.client.generation.Load() {
		r.queueMu.Unlock()
		return
	}
	q.processing = false
	// NextKey=0 表示队列已到尾部（没有更多消息），此时应保留上一次同步到的 key，
	// 而不是把 key 清零。否则下次 NOTICE 到来时会用 key=0 从头重拉，服务器返回空，
	// 导致新消息无法增量拉取（这是“第二次 NOTICE 收不到消息”的根因）。
	if d.NextKey != 0 {
		q.key = d.NextKey
	}
	var scheduleErr error
	if d.IsLast || d.NextKey == 0 {
		if q.noticePending {
			q.noticePending = false
			scheduleErr = r.enqueuePullLocked(q)
		} else {
			q.phase = queueIdle
			r.activeQueues.Add(-1)
			scheduleErr = r.promoteDeferredLocked()
		}
	} else {
		// The continuation pull covers every NOTICE coalesced while this batch
		// was processing, so schedule exactly one request with the new cursor.
		q.noticePending = false
		scheduleErr = r.enqueuePullLocked(q)
	}
	r.queueMu.Unlock()
	if scheduleErr != nil {
		r.capacityHits.Add(1)
		r.fail(scheduleErr)
	}
}

func (r *connectionRun) finalizeQueues() {
	r.finalizeOnce.Do(func() {
		r.queueMu.Lock()
		var tracked int64
		for _, q := range r.queues {
			tracked += q.weight
		}
		r.queues = nil
		r.deferred = deferredQueueRing{}
		r.activeQueues.Store(0)
		local := r.queueBudget
		r.queueMu.Unlock()
		if tracked == 0 {
			return
		}
		localErr := local.Release(tracked)
		processErr := processQueueBudget.Release(tracked)
		if err := errors.Join(localErr, processErr); err != nil && r.client != nil && r.client.logger != nil {
			r.client.logger.Error("queue budget release invariant failed", "bytes", tracked, "error", err)
		}
	})
}

func (r *connectionRun) processMetas(metas []internalprotocol.Meta) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("handler panic: %v", v)
		}
	}()
	for _, m := range metas {
		if r.client.debug {
			r.client.logger.Debug("wss.meta", "meta_id", m.ID, "namespace", m.Namespace, "from", m.From.ID(), "to", m.To.ID(), "payload_bytes", len(m.Payload))
		}
		if err := r.ctx.Err(); err != nil {
			return err
		}
		switch m.Namespace {
		case internalprotocol.NamespaceChat:
			msg, e := parseMessage(r.client, m)
			if e != nil {
				// 消息体损坏：重投也解不开，死信该条并继续同批次其他消息，
				// 避免拆链后服务端重投同一毒消息形成自伤式重连风暴。
				if r.client.debug {
					r.client.logger.Debug("wss.message_decode_error", "meta_id", m.ID, "error", e)
				}
				r.client.logger.Error("dropping undecodable message", "meta_id", m.ID, "error", e)
				continue
			}
			if err := r.ctx.Err(); err != nil {
				return err
			}
			if e := r.deliverMessage(msg); e != nil {
				// 重试后仍失败：死信该条，继续同批次其他消息，保持连接稳定。
				r.client.logger.Error("dead-lettering message after handler failures", "meta_id", m.ID, "error", e)
			}
		case internalprotocol.NamespaceStatistic:
			r.handleStatistic(m.Payload)
		default:
			// 不识别的消息类型（Notify/MUC/Roster/Conference/Query 等）直接丢弃。
			continue
		}
	}
	return nil
}

// deliverMessage 对单条消息执行 handler，最多重试 handlerMaxAttempts 次。
// 返回错误表示该条消息需要死信；连接本身不受影响。handler panic 被当作
// 永久失败捕获，避免一个 panic 拖垮整个批次。
func (r *connectionRun) deliverMessage(msg *Message) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("handler panic: %v", v)
		}
	}()
	for attempt := 1; attempt <= handlerMaxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(r.ctx, handlerTimeout)
		// 协作式超时的兜底观测：Go 无法强制终止不合作的 handler。若 handler
		// 忽略 context、在超时后仍未返回，这里记录告警，帮助业务方发现并
		// 修复不检查 ctx 的 handler，避免其永久占用 worker 槽位并跨重连累积。
		watchdog := time.AfterFunc(handlerTimeout, func() {
			r.client.stuckHandlers.Add(1)
			r.client.logger.Error("message handler ignored its context and is still running",
				"meta_id", msg.MetaID)
		})
		err = r.client.cfg.MessageHandler(ctx, msg)
		if !watchdog.Stop() {
			// watchdog 已触发（handler 超时后才返回），回退计数。Stop() 返回
			// false 仅表示定时器已触发，回调中的 +1 可能尚未执行，因此这里可能
			// 短暂出现 -1；StuckHandlers 仅用于观测/告警（阈值 >0），短暂负值无害。
			r.client.stuckHandlers.Add(-1)
		}
		cancel()
		if err == nil {
			return nil
		}
		if attempt < handlerMaxAttempts {
			select {
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			case <-r.ctx.Done():
				return err
			}
		}
	}
	return err
}

func (r *connectionRun) handleStatistic(payload []byte) {
	s, decodeErr := r.client.codec.DecodeStatistic(payload)
	if decodeErr != nil {
		return
	}
	var err error
	switch s.Operation {
	case internalprotocol.StatisticInformation:
		return
	case internalprotocol.StatisticUserRemoved:
		err = newError(ErrAuthentication, "session", "user removed")
	case internalprotocol.StatisticUserLoginAnotherDevice:
		err = newError(ErrAuthentication, "session", "another device login")
	case internalprotocol.StatisticUserKickedByChangePassword:
		err = newError(ErrKickedChangePass, "session", "password changed")
	case internalprotocol.StatisticUserKickedByOtherDevice:
		err = newError(ErrAuthentication, "session", "kicked by other device")
	}
	if err != nil {
		r.fail(err)
	}
}

func (r *connectionRun) completeACK(d *internalprotocol.Sync) {
	r.pendingMu.Lock()
	ch := r.pending[d.MetaID]
	delete(r.pending, d.MetaID)
	r.pendingMu.Unlock()
	if ch == nil {
		return
	}
	if d.Status != nil && d.Status.Code != internalprotocol.StatusOK {
		ch <- ackResult{err: protocolError("send ack", int32(d.Status.Code), d.Status.Reason)}
		return
	}
	ch <- ackResult{result: &SendResult{
		MessageID: d.ServerID, ClientMessageID: d.MetaID,
		ServerMessageID: d.ServerID, ServerTimestamp: d.Timestamp,
	}}
}
func (r *connectionRun) cancelPending(cause error) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	for id, ch := range r.pending {
		ch <- ackResult{err: &SDKError{Code: ErrSendOutcomeUnknown, Operation: "send", Reason: "connection closed before ACK", Cause: cause}}
		delete(r.pending, id)
	}
}

func (c *Client) Send(ctx context.Context, req SendRequest) (*SendResult, error) {
	c.mu.RLock()
	r := c.run
	connected := c.connState == ConnStateConnected
	loginState := c.state
	userID := c.userID
	c.mu.RUnlock()
	if loginState == LoginStateLogout || loginState == LoginStateLoggingIn {
		return nil, newError(ErrNotLoggedIn, "send", "call Login successfully before Send")
	}
	if !connected || r == nil {
		return nil, newError(ErrNotConnected, "send", "")
	}
	id := req.ClientMessageID
	if id == 0 {
		var err error
		id, err = c.nextMessageID()
		if err != nil {
			return nil, err
		}
	}
	meta, err := buildSendMeta(c, userID, req, id)
	if err != nil {
		return nil, err
	}
	frame, err := c.codec.EncodeSync(internalprotocol.SyncRequest{Meta: &meta})
	if err != nil {
		return nil, err
	}
	wait := make(chan ackResult, 1)
	r.pendingMu.Lock()
	if _, exists := r.pending[id]; exists {
		r.pendingMu.Unlock()
		return nil, fmt.Errorf("ClientMessageID %d is already pending", id)
	}
	r.pending[id] = wait
	r.pendingMu.Unlock()
	defer func() { r.pendingMu.Lock(); delete(r.pending, id); r.pendingMu.Unlock() }()
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	if err = r.sendFrame(sendCtx, frame); err != nil {
		return nil, err
	}
	select {
	case out := <-wait:
		return out.result, out.err
	case <-sendCtx.Done():
		return nil, &SDKError{Code: ErrSendOutcomeUnknown, Operation: "send", Reason: "ACK timeout", Cause: sendCtx.Err()}
	case <-r.done:
		return nil, newError(ErrSendOutcomeUnknown, "send", "connection closed before ACK")
	}
}
