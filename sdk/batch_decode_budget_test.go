package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

type budgetTestCodec struct {
	messageTestCodec
	class       internalprotocol.DecodeAdmissionClass
	estimateErr error
	decode      func() (*internalprotocol.Frame, error)
}

func (c *budgetTestCodec) EstimateDecodeAdmission([]byte) (internalprotocol.DecodeAdmissionClass, error) {
	return c.class, c.estimateErr
}

func (c *budgetTestCodec) DecodeFrame([]byte) (*internalprotocol.Frame, error) {
	return c.decode()
}

func newDecodeBudgetRun(codec internalprotocol.Codec, budget *byteBudget, wait time.Duration) (*Client, *connectionRun, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{codec: codec, logger: defaultLogger(), eventCtx: context.Background()}
	client.generation.Store(1)
	return client, &connectionRun{client: client, ctx: ctx, cancel: cancel, generation: 1, decodeBudget: budget, decodeWait: wait}, cancel
}

func TestDecodeAdmissionHeldThroughDispatchAndReleased(t *testing.T) {
	budget := mustNewByteBudget(decodeAdmissionTinyBytes)
	codec := &budgetTestCodec{class: internalprotocol.DecodeAdmissionTiny}
	codec.decode = func() (*internalprotocol.Frame, error) {
		if got := budget.Used(); got != decodeAdmissionTinyBytes {
			t.Fatalf("decode budget used=%d, want %d", got, decodeAdmissionTinyBytes)
		}
		return &internalprotocol.Frame{Command: internalprotocol.CommandUnread, Unread: &internalprotocol.Unread{Status: &internalprotocol.Status{Code: internalprotocol.StatusOK}}}, nil
	}
	_, run, cancel := newDecodeBudgetRun(codec, budget, time.Second)
	defer cancel()
	if err := run.decodeAndDispatch([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("decode budget after dispatch=%d", got)
	}
}

func TestDecodeAdmissionTimeoutAndProtocolClassification(t *testing.T) {
	budget := mustNewByteBudget(decodeAdmissionTinyBytes)
	if ok, err := budget.TryAcquire(decodeAdmissionTinyBytes); err != nil || !ok {
		t.Fatalf("fill decode budget=(%v,%v)", ok, err)
	}
	codec := &budgetTestCodec{class: internalprotocol.DecodeAdmissionTiny, decode: func() (*internalprotocol.Frame, error) {
		return nil, errors.New("decode must not run")
	}}
	client, run, cancel := newDecodeBudgetRun(codec, budget, 10*time.Millisecond)
	err := run.decodeAndDispatch(nil)
	cancel()
	if errorCode(err) != ErrHandlerBacklog || client.decodeTimeouts.Load() != 1 {
		t.Fatalf("timeout error=%v code=%s count=%d", err, errorCode(err), client.decodeTimeouts.Load())
	}
	if err := budget.Release(decodeAdmissionTinyBytes); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name string
		err  error
		want ErrorCode
	}{
		{name: "malformed", err: errors.New("malformed wire"), want: ErrProtocol},
		{name: "limit", err: fmt.Errorf("scan: %w", internalprotocol.ErrLimitExceeded), want: ErrProtocolLimit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			codec := &budgetTestCodec{estimateErr: tt.err, decode: func() (*internalprotocol.Frame, error) { return nil, nil }}
			_, run, cancel := newDecodeBudgetRun(codec, mustNewByteBudget(decodeAdmissionTinyBytes), time.Second)
			defer cancel()
			if err := run.decodeAndDispatch(nil); errorCode(err) != tt.want {
				t.Fatalf("error=%v code=%s want=%s", err, errorCode(err), tt.want)
			}
		})
	}
}

func TestDecodeAdmissionSixtyFourHoldersBoundAndHundredArrivalBurst(t *testing.T) {
	const holders = 64
	budget := mustNewByteBudget(processDecodeBudgetBytes)
	entered := make(chan struct{}, holders)
	release := make(chan struct{})
	codec := &budgetTestCodec{class: internalprotocol.DecodeAdmissionTiny}
	codec.decode = func() (*internalprotocol.Frame, error) {
		entered <- struct{}{}
		<-release
		return &internalprotocol.Frame{Command: internalprotocol.CommandUnread, Unread: &internalprotocol.Unread{}}, nil
	}
	client, run, cancel := newDecodeBudgetRun(codec, budget, 20*time.Millisecond)
	defer cancel()

	errs := make(chan error, holders)
	for i := 0; i < holders; i++ {
		go func() { errs <- run.decodeAndDispatch(nil) }()
	}
	for i := 0; i < holders; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("only %d/%d decode holders entered", i, holders)
		}
	}
	if got := budget.Used(); got != processDecodeBudgetBytes {
		t.Fatalf("held decode weight=%d, want %d", got, processDecodeBudgetBytes)
	}
	if err := run.decodeAndDispatch(nil); errorCode(err) != ErrHandlerBacklog {
		t.Fatalf("65th holder error=%v, want ErrHandlerBacklog", err)
	}
	if got := client.decodeTimeouts.Load(); got != 1 {
		t.Fatalf("decode timeout count=%d, want 1", got)
	}
	close(release)
	for i := 0; i < holders; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("holder %d: %v", i, err)
		}
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("decode budget after release=%d", got)
	}

	burstBudget := mustNewByteBudget(processDecodeBudgetBytes)
	burstCodec := &budgetTestCodec{class: internalprotocol.DecodeAdmissionTiny}
	burstCodec.decode = func() (*internalprotocol.Frame, error) {
		return &internalprotocol.Frame{Command: internalprotocol.CommandUnread, Unread: &internalprotocol.Unread{}}, nil
	}
	_, burstRun, burstCancel := newDecodeBudgetRun(burstCodec, burstBudget, decodeAdmissionWait)
	defer burstCancel()
	const arrivals = 100
	var wg sync.WaitGroup
	burstErrs := make(chan error, arrivals)
	for i := 0; i < arrivals; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := burstRun.decodeAndDispatch(nil); err != nil {
				burstErrs <- err
			}
		}()
	}
	wg.Wait()
	close(burstErrs)
	for err := range burstErrs {
		t.Errorf("100-frame burst admission: %v", err)
	}
	if got := burstBudget.Used(); got != 0 {
		t.Fatalf("burst decode budget after completion=%d", got)
	}
}

func TestBatchWorkerReleasesOnPanicAndOldGeneration(t *testing.T) {
	for _, oldGeneration := range []bool{false, true} {
		t.Run(fmt.Sprintf("old_generation_%v", oldGeneration), func(t *testing.T) {
			clientBudget := mustNewByteBudget(1 << 20)
			processBudget := mustNewByteBudget(1 << 20)
			reservation, ok, err := tryReserveBatch(clientBudget, processBudget, 4<<10)
			if err != nil || !ok {
				t.Fatalf("reserve=(%v,%v)", ok, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &Client{logger: defaultLogger(), eventCtx: context.Background()}
			client.generation.Store(1)
			runGeneration := uint64(1)
			if oldGeneration {
				runGeneration = 0
			}
			run := &connectionRun{client: client, ctx: ctx, cancel: cancel, generation: runGeneration, done: make(chan struct{})}
			client.processBatchJob(batchJob{r: run, d: nil, reservation: reservation})
			if clientBudget.Used() != 0 || processBudget.Used() != 0 {
				t.Fatalf("budgets after worker client=%d process=%d", clientBudget.Used(), processBudget.Used())
			}
			if !oldGeneration {
				select {
				case <-run.done:
				default:
					t.Fatal("worker panic did not fail the run")
				}
				if errorCode(client.lastErr) != ErrHandlerFailed {
					t.Fatalf("worker panic error=%v", client.lastErr)
				}
			}
		})
	}
}

func TestCloseDrainsQueuedBatchReservations(t *testing.T) {
	client, _ := newLifecycleContextClient(nil)
	client.batchBudget = mustNewByteBudget(1 << 20)
	processBudget := mustNewByteBudget(1 << 20)
	reservation, ok, err := tryReserveBatch(client.batchBudget, processBudget, 4<<10)
	if err != nil || !ok {
		t.Fatalf("reserve=(%v,%v)", ok, err)
	}
	client.batches <- batchJob{reservation: reservation}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.batchBudget.Used() != 0 || processBudget.Used() != 0 || len(client.batches) != 0 {
		t.Fatalf("close drain client=%d process=%d queued=%d", client.batchBudget.Used(), processBudget.Used(), len(client.batches))
	}
}

func TestBatchChannelFullReleasesProducerReservation(t *testing.T) {
	client, run := newQueueTestRun(t, connectionQueueBudgetBytes)
	client.batches = make(chan batchJob, 1)
	client.batches <- batchJob{}
	client.batchBudget = mustNewByteBudget(1 << 20)
	processBudget := mustNewByteBudget(1 << 20)
	run.processBatchBudget = processBudget

	jid := testQueueJID(900)
	run.startQueue(jid)
	_ = dequeuePullForResponse(t, run)
	run.handleBatch(&internalprotocol.Sync{Queue: &jid, IsLast: true})
	if client.batchBudget.Used() != 0 || processBudget.Used() != 0 {
		t.Fatalf("channel-full budgets client=%d process=%d", client.batchBudget.Used(), processBudget.Used())
	}
	if errorCode(client.lastErr) != ErrHandlerBacklog {
		t.Fatalf("channel-full error=%v", client.lastErr)
	}
	run.queueMu.Lock()
	processing := run.queues[newQueueKey(jid)].processing
	run.queueMu.Unlock()
	if processing {
		t.Fatal("channel-full queue remained processing")
	}
}
