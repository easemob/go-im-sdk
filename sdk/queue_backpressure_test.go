package sdk

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unsafe"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

func newQueueTestRun(t *testing.T, localBudget int64) (*Client, *connectionRun) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	budget, err := newByteBudget(localBudget)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{
		cfg:         Config{MessageHandler: func(context.Context, *Message) error { return nil }},
		logger:      defaultLogger(),
		batches:     make(chan batchJob, 512),
		batchBudget: mustNewByteBudget(clientBatchBudgetBytes),
		eventCtx:    context.Background(),
		codec:       &lifecycleContextCodec{closed: make(chan struct{})},
	}
	client.generation.Store(1)
	run := &connectionRun{
		client: client, ctx: ctx, cancel: cancel, generation: 1,
		writes: make(chan writeRequest, writeQueueSize), done: make(chan struct{}),
		pending: make(map[uint64]chan ackResult), queues: make(map[queueKey]*queueState),
		deferred: newDeferredQueueRing(maxTrackedQueues), pullRequests: make(chan *queueState, pullRequestCapacity),
		queueBudget: budget, queueTimeout: queueResponseTimeout,
	}
	client.run = run
	t.Cleanup(func() { run.shutdown(newError(ErrStreamClosed, "test", "cleanup")) })
	return client, run
}

func testQueueJID(index int) internalprotocol.JID {
	return internalprotocol.JID{AppKey: "org#app", Name: fmt.Sprintf("queue-%03d", index), Domain: "conference.example"}
}

func testQueueWeight(count int) int64 {
	var total int64
	for i := 0; i < count; i++ {
		total += queueRetainedWeight(testQueueJID(i))
	}
	return total
}

func dequeuePullForResponse(t *testing.T, run *connectionRun) *queueState {
	t.Helper()
	select {
	case q := <-run.pullRequests:
		run.queueMu.Lock()
		if !q.pullQueued || q.phase != queueActive {
			run.queueMu.Unlock()
			t.Fatalf("invalid queued pull state: %#v", q)
		}
		q.pullQueued = false
		q.pullInFlight = true
		q.pullSentAt = time.Now()
		run.queueMu.Unlock()
		return q
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pull request")
		return nil
	}
}

func TestNoticeDuringLastBatchSchedulesExactlyOneFollowUp(t *testing.T) {
	client, run := newQueueTestRun(t, connectionQueueBudgetBytes)
	jid := testQueueJID(1)
	run.startQueue(jid)
	q := dequeuePullForResponse(t, run)
	batch := &internalprotocol.Sync{Queue: &jid, NextKey: 42, IsLast: true}
	run.handleBatch(batch)

	var job batchJob
	select {
	case job = <-client.batches:
	case <-time.After(time.Second):
		t.Fatal("SYNC batch was not admitted")
	}
	// Duplicate notices in the processing window coalesce into one pending bit.
	run.startQueue(jid)
	run.startQueue(jid)
	client.processBatchJob(job)

	run.queueMu.Lock()
	if q.phase != queueActive || q.noticePending || q.key != 42 || !q.pullQueued || q.pullInFlight || q.processing {
		run.queueMu.Unlock()
		t.Fatalf("follow-up state=%#v", q)
	}
	run.queueMu.Unlock()
	if got := len(run.pullRequests); got != 1 {
		t.Fatalf("follow-up pull count=%d, want 1", got)
	}
	if followUp := <-run.pullRequests; followUp != q {
		t.Fatal("follow-up scheduled the wrong queue")
	}
}

func TestDeferredQueuesPromoteInFIFOOrder(t *testing.T) {
	_, run := newQueueTestRun(t, connectionQueueBudgetBytes)
	const total = 400
	for i := 0; i < total; i++ {
		run.startQueue(testQueueJID(i))
	}
	active, known, deferred, pulls, tracked, rejects := run.queueHealth()
	if active != maxActiveQueues || known != total || deferred != total-maxActiveQueues || pulls != maxActiveQueues ||
		tracked != testQueueWeight(total) || rejects != 0 {
		t.Fatalf("initial health active=%d known=%d deferred=%d pulls=%d tracked=%d rejects=%d",
			active, known, deferred, pulls, tracked, rejects)
	}

	for i := 0; i < total-maxActiveQueues; i++ {
		q := dequeuePullForResponse(t, run)
		if q.identity != newQueueKey(testQueueJID(i)) {
			t.Fatalf("active pull %d=%s, want %s", i, q.identity, newQueueKey(testQueueJID(i)))
		}
		run.queueMu.Lock()
		q.pullInFlight = false
		q.pullSentAt = time.Time{}
		q.processing = true
		run.queueMu.Unlock()
		run.processBatch(q.identity, &internalprotocol.Sync{Queue: &q.jid, IsLast: true})

		promoted := run.queues[newQueueKey(testQueueJID(maxActiveQueues+i))]
		if promoted == nil || promoted.phase != queueActive || !promoted.pullQueued {
			t.Fatalf("deferred queue %d was not promoted in FIFO order: %#v", maxActiveQueues+i, promoted)
		}
	}
	active, known, deferred, pulls, tracked, rejects = run.queueHealth()
	if active != maxActiveQueues || known != total || deferred != 0 || pulls != maxActiveQueues ||
		tracked != testQueueWeight(total) || rejects != 0 {
		t.Fatalf("final health active=%d known=%d deferred=%d pulls=%d tracked=%d rejects=%d",
			active, known, deferred, pulls, tracked, rejects)
	}
}

func TestQueueIdentityIsTupleAndOnlyLengthIsRejected(t *testing.T) {
	_, run := newQueueTestRun(t, connectionQueueBudgetBytes)
	first := internalprotocol.JID{AppKey: "a/b", Name: "c", Domain: "d"}
	second := internalprotocol.JID{AppKey: "a", Name: "b/c", Domain: "d"}
	empty := internalprotocol.JID{}
	run.startQueue(first)
	run.startQueue(second)
	run.startQueue(empty)
	if got := len(run.queues); got != 3 {
		t.Fatalf("tuple identities collapsed or empty JID was rejected: known=%d", got)
	}

	_, oversized := newQueueTestRun(t, connectionQueueBudgetBytes)
	oversized.startQueue(internalprotocol.JID{Name: strings.Repeat("x", internalprotocol.MaxJIDComponentBytes+1)})
	select {
	case <-oversized.done:
	default:
		t.Fatal("oversized JID did not terminate the run")
	}
	oversized.client.mu.RLock()
	err := oversized.client.lastErr
	oversized.client.mu.RUnlock()
	if errorCode(err) != ErrProtocolLimit || !isTerminal(err) || len(oversized.queues) != 0 {
		t.Fatalf("oversized JID error=%v terminal=%v known=%d", err, isTerminal(err), len(oversized.queues))
	}
}

func TestQueueCapacityFailureIsRecoverableAndRollsBack(t *testing.T) {
	baseline := processQueueBudget.Used()
	perQueue := queueRetainedWeight(testQueueJID(1))
	client, run := newQueueTestRun(t, 2*perQueue)
	run.startQueue(testQueueJID(1))
	run.startQueue(testQueueJID(2))
	run.startQueue(testQueueJID(3))

	select {
	case <-run.done:
	default:
		t.Fatal("capacity exhaustion did not stop the run")
	}
	client.mu.RLock()
	err := client.lastErr
	client.mu.RUnlock()
	active, known, deferred, _, tracked, rejects := run.queueHealth()
	if errorCode(err) != ErrHandlerBacklog || isTerminal(err) || active != 2 || known != 2 || deferred != 0 ||
		tracked != 2*perQueue || rejects != 1 {
		t.Fatalf("capacity result err=%v active=%d known=%d deferred=%d tracked=%d rejects=%d",
			err, active, known, deferred, tracked, rejects)
	}
	run.shutdown(newError(ErrStreamClosed, "test", "finalize"))
	if got := run.queueBudget.Used(); got != 0 {
		t.Fatalf("local queue budget after finalize=%d", got)
	}
	if got := processQueueBudget.Used(); got != baseline {
		t.Fatalf("process queue budget after finalize=%d, want baseline %d", got, baseline)
	}
	// A second finalization must not release either budget twice.
	run.shutdown(newError(ErrStreamClosed, "test", "second finalize"))
	if got := processQueueBudget.Used(); got != baseline {
		t.Fatalf("process queue budget after second finalize=%d, want baseline %d", got, baseline)
	}
}

func TestDuplicateQueueIdentityWeightOnlyGrowsAndRollsBackOnCapacity(t *testing.T) {
	base := internalprotocol.JID{AppKey: "org#app", Name: "same", Domain: "conference.example"}
	large := internalprotocol.JID{
		AppKey: strings.Clone(base.AppKey), Name: strings.Clone(base.Name), Domain: strings.Clone(base.Domain),
	}
	if unsafe.StringData(large.AppKey) == unsafe.StringData(base.AppKey) {
		t.Fatal("test requires distinct string backing for repeated identity")
	}
	large.ClientResource = strings.Repeat("r", int(3*minQueueWeight))
	baseWeight := queueRetainedWeight(base)
	largeWeight := queueRetainedWeight(large)

	_, growing := newQueueTestRun(t, connectionQueueBudgetBytes)
	growing.startQueue(base)
	growing.startQueue(large)
	q := growing.queues[newQueueKey(base)]
	if q == nil || q.weight != largeWeight || q.jid.ClientResource != large.ClientResource || growing.queueBudget.Used() != largeWeight {
		t.Fatalf("grown queue=%#v used=%d want=%d", q, growing.queueBudget.Used(), largeWeight)
	}
	if unsafe.StringData(q.jid.AppKey) != unsafe.StringData(q.identity.appKey) ||
		unsafe.StringData(q.jid.Name) != unsafe.StringData(q.identity.name) ||
		unsafe.StringData(q.jid.Domain) != unsafe.StringData(q.identity.domain) {
		t.Fatal("repeated identity retained a second uncharged string backing")
	}
	growing.startQueue(base)
	if q.weight != largeWeight || growing.queueBudget.Used() != largeWeight {
		t.Fatalf("queue charge decreased: weight=%d used=%d", q.weight, growing.queueBudget.Used())
	}

	client, constrained := newQueueTestRun(t, baseWeight)
	constrained.startQueue(base)
	constrained.startQueue(large)
	q = constrained.queues[newQueueKey(base)]
	client.mu.RLock()
	err := client.lastErr
	client.mu.RUnlock()
	if q == nil || q.weight != baseWeight || q.jid.ClientResource != "" || constrained.queueBudget.Used() != baseWeight ||
		errorCode(err) != ErrHandlerBacklog || isTerminal(err) || constrained.capacityHits.Load() != 1 {
		t.Fatalf("growth rollback queue=%#v used=%d err=%v rejects=%d",
			q, constrained.queueBudget.Used(), err, constrained.capacityHits.Load())
	}
}

func TestProcessQueueBudgetFailureRollsBackRunReservation(t *testing.T) {
	baseline := processQueueBudget.Used()
	remaining := processQueueBudget.Limit() - baseline
	if remaining <= 0 {
		t.Fatalf("process queue budget unexpectedly full: used=%d", baseline)
	}
	acquired, err := processQueueBudget.TryAcquire(remaining)
	if err != nil || !acquired {
		t.Fatalf("fill process budget: acquired=%v err=%v", acquired, err)
	}
	defer func() {
		if err := processQueueBudget.Release(remaining); err != nil {
			t.Errorf("release process budget: %v", err)
		}
	}()

	client, run := newQueueTestRun(t, connectionQueueBudgetBytes)
	run.startQueue(testQueueJID(1))
	client.mu.RLock()
	runErr := client.lastErr
	client.mu.RUnlock()
	if errorCode(runErr) != ErrHandlerBacklog || isTerminal(runErr) || len(run.queues) != 0 || run.queueBudget.Used() != 0 ||
		run.capacityHits.Load() != 1 {
		t.Fatalf("process-full rollback err=%v known=%d local=%d rejects=%d",
			runErr, len(run.queues), run.queueBudget.Used(), run.capacityHits.Load())
	}
}

func TestPullSchedulerAndWatchdogExitWithRun(t *testing.T) {
	_, workers := newQueueTestRun(t, connectionQueueBudgetBytes)
	workers.startPullScheduler()
	workerDone := make(chan struct{})
	go func() {
		workers.shutdown(newError(ErrStreamClosed, "test", "stop workers"))
		close(workerDone)
	}()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("pull workers did not stop with run context")
	}

	_, watchdog := newQueueTestRun(t, connectionQueueBudgetBytes)
	watchdog.queueTimeout = 20 * time.Millisecond
	jid := testQueueJID(1)
	identity := newQueueKey(jid)
	watchdog.queueMu.Lock()
	watchdog.queues[identity] = &queueState{
		identity: identity, jid: jid, phase: queueActive,
		pullInFlight: true, pullSentAt: time.Now().Add(-time.Second),
	}
	watchdog.activeQueues.Store(1)
	watchdog.queueMu.Unlock()
	watchdog.wg.Add(1)
	go watchdog.pullResponseWatchdog()
	select {
	case <-watchdog.done:
	case <-time.After(time.Second):
		t.Fatal("queue response watchdog did not stop a stalled pull")
	}
	watchdog.client.mu.RLock()
	err := watchdog.client.lastErr
	watchdog.client.mu.RUnlock()
	if errorCode(err) != ErrTimeout {
		t.Fatalf("watchdog error=%v, want ErrTimeout", err)
	}
}
