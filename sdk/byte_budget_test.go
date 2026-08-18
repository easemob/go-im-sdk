package sdk

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestByteBudgetTryAcquireRelease(t *testing.T) {
	b := mustByteBudget(t, 10)

	acquired, err := b.TryAcquire(6)
	if err != nil || !acquired {
		t.Fatalf("TryAcquire(6) = (%v, %v), want (true, nil)", acquired, err)
	}
	acquired, err = b.TryAcquire(5)
	if err != nil || acquired {
		t.Fatalf("TryAcquire(5) = (%v, %v), want (false, nil)", acquired, err)
	}
	if got := b.Used(); got != 6 {
		t.Fatalf("Used() = %d, want 6", got)
	}
	if got := b.Limit(); got != 10 {
		t.Fatalf("Limit() = %d, want 10", got)
	}
	if err := b.Release(6); err != nil {
		t.Fatalf("Release(6): %v", err)
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used() after release = %d, want 0", got)
	}
}

func TestByteBudgetAcquireWakesAfterRelease(t *testing.T) {
	b := mustByteBudget(t, 10)
	if acquired, err := b.TryAcquire(10); err != nil || !acquired {
		t.Fatalf("fill budget = (%v, %v)", acquired, err)
	}

	result := make(chan error, 1)
	go func() {
		result <- b.Acquire(context.Background(), 4)
	}()

	select {
	case err := <-result:
		t.Fatalf("Acquire returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := b.Release(4); err != nil {
		t.Fatalf("Release(4): %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not wake after release")
	}
	if got := b.Used(); got != 10 {
		t.Fatalf("Used() = %d, want 10", got)
	}
	if err := b.Release(10); err != nil {
		t.Fatalf("final Release(10): %v", err)
	}
}

func TestByteBudgetAcquireCancellationDoesNotLeakWaiters(t *testing.T) {
	b := mustByteBudget(t, 1)
	if acquired, err := b.TryAcquire(1); err != nil || !acquired {
		t.Fatalf("fill budget = (%v, %v)", acquired, err)
	}

	const waiters = 64
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			results <- b.Acquire(ctx, 1)
		}()
	}
	// Let the goroutines reach the shared wait channel so this exercises the
	// blocked-waiter cancellation path rather than only pre-canceled calls.
	time.Sleep(20 * time.Millisecond)
	cancel()

	deadline := time.After(2 * time.Second)
	for i := 0; i < waiters; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("waiter returned %v, want context.Canceled", err)
			}
		case <-deadline:
			t.Fatalf("only %d/%d canceled waiters returned", i, waiters)
		}
	}
	if got := b.Used(); got != 1 {
		t.Fatalf("Used() after canceled waiters = %d, want 1", got)
	}
	if err := b.Release(1); err != nil {
		t.Fatalf("Release(1): %v", err)
	}
}

func TestByteBudgetInvariantErrorsDoNotChangeUsage(t *testing.T) {
	b := mustByteBudget(t, 10)
	if acquired, err := b.TryAcquire(6); err != nil || !acquired {
		t.Fatalf("TryAcquire(6) = (%v, %v)", acquired, err)
	}

	assertInvariant := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, errByteBudgetInvariant) {
			t.Fatalf("%s error = %v, want byte budget invariant error", name, err)
		}
		if got := b.Used(); got != 6 {
			t.Fatalf("Used() after %s = %d, want 6", name, got)
		}
	}

	for _, n := range []int64{0, -1, 11} {
		_, err := b.TryAcquire(n)
		assertInvariant("TryAcquire", err)
	}
	for _, n := range []int64{0, -1, 11} {
		assertInvariant("Acquire", b.Acquire(context.Background(), n))
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	assertInvariant("Acquire canceled invalid weight", b.Acquire(canceled, 0))
	for _, n := range []int64{0, -1, 7} {
		assertInvariant("Release", b.Release(n))
	}
	assertInvariant("Acquire(nil)", b.Acquire(nil, 1))

	if err := b.Release(6); err != nil {
		t.Fatalf("Release(6): %v", err)
	}
	if err := b.Release(6); !errors.Is(err, errByteBudgetInvariant) {
		t.Fatalf("duplicate Release(6) = %v, want invariant error", err)
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used() after duplicate release = %d, want 0", got)
	}

	if _, err := newByteBudget(-1); !errors.Is(err, errByteBudgetInvariant) {
		t.Fatalf("newByteBudget(-1) = %v, want invariant error", err)
	}
	var nilBudget *byteBudget
	if _, err := nilBudget.TryAcquire(1); !errors.Is(err, errByteBudgetInvariant) {
		t.Fatalf("nil TryAcquire = %v, want invariant error", err)
	}
	if err := nilBudget.Release(1); !errors.Is(err, errByteBudgetInvariant) {
		t.Fatalf("nil Release = %v, want invariant error", err)
	}
}

func TestByteBudgetCheckedArithmetic(t *testing.T) {
	b := mustByteBudget(t, math.MaxInt64)
	if acquired, err := b.TryAcquire(math.MaxInt64 - 1); err != nil || !acquired {
		t.Fatalf("large TryAcquire = (%v, %v)", acquired, err)
	}
	if acquired, err := b.TryAcquire(2); err != nil || acquired {
		t.Fatalf("overflowing TryAcquire = (%v, %v), want (false, nil)", acquired, err)
	}
	if acquired, err := b.TryAcquire(1); err != nil || !acquired {
		t.Fatalf("final-byte TryAcquire = (%v, %v)", acquired, err)
	}
	if got := b.Used(); got != math.MaxInt64 {
		t.Fatalf("Used() = %d, want MaxInt64", got)
	}
	if err := b.Release(math.MaxInt64); err != nil {
		t.Fatalf("Release(MaxInt64): %v", err)
	}
}

func TestByteBudgetConcurrentBounds(t *testing.T) {
	const (
		limit      = int64(256)
		goroutines = 32
		iterations = 2000
	)
	b := mustByteBudget(t, limit)

	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		weight := int64(i%16 + 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				acquired, err := b.TryAcquire(weight)
				if err != nil {
					errs <- err
					return
				}
				used := b.Used()
				if used < 0 || used > limit {
					errs <- byteBudgetInvariantf("observed used %d", used)
					return
				}
				if !acquired {
					continue
				}
				if err := b.Release(weight); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent budget operation: %v", err)
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used() after concurrent operations = %d, want 0", got)
	}
}

func TestByteBudgetCorruptStateIsRejectedWithoutMutation(t *testing.T) {
	tests := []struct {
		limit int64
		used  int64
	}{
		{limit: 10, used: -1},
		{limit: 10, used: 11},
		{limit: -1, used: 0},
	}
	for _, initial := range tests {
		b := &byteBudget{limit: initial.limit, used: initial.used}
		beforeLimit, beforeUsed := b.limit, b.used
		if _, err := b.TryAcquire(1); !errors.Is(err, errByteBudgetInvariant) {
			t.Errorf("TryAcquire state (%d/%d) = %v", beforeUsed, beforeLimit, err)
		}
		if err := b.Release(1); !errors.Is(err, errByteBudgetInvariant) {
			t.Errorf("Release state (%d/%d) = %v", beforeUsed, beforeLimit, err)
		}
		if b.limit != beforeLimit || b.used != beforeUsed {
			t.Errorf("state changed from (%d/%d) to (%d/%d)", beforeUsed, beforeLimit, b.used, b.limit)
		}
	}
}

func mustByteBudget(t *testing.T, limit int64) *byteBudget {
	t.Helper()
	b, err := newByteBudget(limit)
	if err != nil {
		t.Fatalf("newByteBudget(%d): %v", limit, err)
	}
	return b
}
