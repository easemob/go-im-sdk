package sdk

import (
	"context"
	"fmt"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

type queueState struct {
	jid        internalprotocol.JID
	key        uint64
	active     bool
	processing bool
}

func queueID(j internalprotocol.JID) string {
	return j.BareID()
}
func (r *connectionRun) queueBacklog() int {
	return int(r.activeQueues.Load())
}

func (r *connectionRun) startQueue(j internalprotocol.JID) {
	id := queueID(j)
	r.queueMu.Lock()
	q := r.queues[id]
	if q == nil {
		q = &queueState{jid: j}
		r.queues[id] = q
	}
	if q.active {
		r.queueMu.Unlock()
		return
	}
	q.active = true
	r.activeQueues.Add(1)
	key := q.key
	r.queueMu.Unlock()
	go r.pullQueue(id, key)
}

func (r *connectionRun) pullQueue(id string, key uint64) {
	r.queueMu.Lock()
	q := r.queues[id]
	var jid *internalprotocol.JID
	if q != nil {
		copy := q.jid
		jid = &copy
	}
	r.queueMu.Unlock()
	if jid == nil {
		return
	}
	frame, err := r.client.codec.EncodeSync(internalprotocol.SyncRequest{Queue: jid, Key: key})
	if r.client.debug {
		r.client.logger.Debug("wss.queue_pull", "queue_key", id, "queue_jid", jid.ID(), "key", key)
	}
	if err == nil {
		err = r.sendFrame(r.ctx, frame)
	}
	if err != nil {
		r.fail(err)
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
	id := queueID(*d.Queue)
	if id == "///" {
		return
	}
	r.queueMu.Lock()
	q := r.queues[id]
	if q == nil || !q.active || q.processing {
		r.queueMu.Unlock()
		return
	}
	q.processing = true
	r.queueMu.Unlock()
	// 非阻塞入队：readPump 不能被 handler 背压卡住，否则 keepalive（UNREAD 回包）
	// 无法处理，健康连接会被心跳误杀。队列满时视为消费不过来的背压信号，
	// 断开让对端重投（配合跨代际退避，避免形成自伤式重连风暴）。
	select {
	case r.client.batches <- batchJob{id: id, d: d, r: r}:
	case <-r.ctx.Done():
	default:
		r.queueMu.Lock()
		q.processing = false
		r.queueMu.Unlock()
		r.fail(newError(ErrHandlerBacklog, "sync", "batch queue full"))
	}
}

// batchWorker 消费 Client 级批次队列。worker 是 Client 级固定池，跨连接代际共享，
// 重连不会创建新 worker；不计入任何 WaitGroup：handler 是不可信的用户代码，可能
// 忽略 context 长期阻塞，计入 wg 会让 Close 永久等待。worker 依靠 eventCtx 退出。
// 取出任务后由 job.r.processBatch 内部的 ctx/generation 检查丢弃旧连接的任务。
func (c *Client) batchWorker() {
	for {
		select {
		case job := <-c.batches:
			job.r.processBatch(job.id, job.d)
		case <-c.eventCtx.Done():
			return
		}
	}
}

func (r *connectionRun) processBatch(id string, d *internalprotocol.Sync) {
	// processMetas 内部已对单条消息做重试与死信，只有 ctx 取消或 handler panic
	// 才会返回错误。这里把这两种情况也视为"该批次无法推进"，死信并推进 key，
	// 而不是拆链——否则重投会再次触发同样的问题，形成自伤式重连风暴。
	if err := r.processMetas(d.Metas); err != nil {
		if r.ctx.Err() != nil {
			return
		}
		r.client.logger.Error("message batch failed; advancing queue (dead-letter)",
			"queue", id, "next_key", d.NextKey, "error", err)
	}
	if r.ctx.Err() != nil {
		return
	}
	r.queueMu.Lock()
	q := r.queues[id]
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
	if d.IsLast || d.NextKey == 0 {
		q.active = false
		r.activeQueues.Add(-1)
		r.queueMu.Unlock()
		return
	}
	next := q.key
	r.queueMu.Unlock()
	r.pullQueue(id, next)
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
		case internalprotocol.NamespaceNotify:
			continue
		case internalprotocol.NamespaceMUC:
			continue
		}
	}
	return nil
}

// deliverMessage 对单条消息执行 handler，最多重试 HandlerMaxAttempts 次。
// 返回错误表示该条消息需要死信；连接本身不受影响。handler panic 被当作
// 永久失败捕获，避免一个 panic 拖垮整个批次。
func (r *connectionRun) deliverMessage(msg *Message) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("handler panic: %v", v)
		}
	}()
	for attempt := 1; attempt <= r.client.cfg.HandlerMaxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(r.ctx, r.client.cfg.HandlerTimeout)
		// 协作式超时的兜底观测：Go 无法强制终止不合作的 handler。若 handler
		// 忽略 context、在超时后仍未返回，这里记录告警，帮助业务方发现并
		// 修复不检查 ctx 的 handler，避免其永久占用 worker 槽位并跨重连累积。
		watchdog := time.AfterFunc(r.client.cfg.HandlerTimeout, func() {
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
		if attempt < r.client.cfg.HandlerMaxAttempts {
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
		id = c.nextMessageID()
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
	sendCtx, cancel := context.WithTimeout(ctx, c.cfg.SendTimeout)
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
