package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// errByteBudgetInvariant identifies programming and ownership errors in a
// byteBudget. Capacity exhaustion is not an invariant error: TryAcquire
// reports it as (false, nil), while Acquire waits for capacity or context
// cancellation.
var errByteBudgetInvariant = errors.New("byte budget invariant violation")

// byteBudget is a small weighted semaphore for bounding in-memory work.
//
// It intentionally does not provide strict FIFO ordering. When capacity is
// released all waiters are notified and may compete again, which lets small
// reservations make progress without allocating a goroutine per waiter.
type byteBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
	wait  chan struct{}
}

func newByteBudget(limit int64) (*byteBudget, error) {
	if limit < 0 {
		return nil, byteBudgetInvariantf("negative limit %d", limit)
	}
	return &byteBudget{
		limit: limit,
		wait:  make(chan struct{}),
	}, nil
}

// TryAcquire reserves n bytes if capacity is immediately available.
func (b *byteBudget) TryAcquire(n int64) (bool, error) {
	if b == nil {
		return false, byteBudgetInvariantf("nil budget")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tryAcquireLocked(n)
}

// Acquire waits until n bytes can be reserved or ctx is done.
func (b *byteBudget) Acquire(ctx context.Context, n int64) error {
	if b == nil {
		return byteBudgetInvariantf("nil budget")
	}
	if ctx == nil {
		return byteBudgetInvariantf("nil context")
	}
	b.mu.Lock()
	err := b.validateAcquireLocked(n)
	b.mu.Unlock()
	if err != nil {
		return err
	}

	for {
		// Do not grant a new reservation to a caller whose cancellation was
		// already observable before it competed for the budget.
		if err := ctx.Err(); err != nil {
			return err
		}

		b.mu.Lock()
		acquired, err := b.tryAcquireLocked(n)
		if err != nil {
			b.mu.Unlock()
			return err
		}
		if acquired {
			b.mu.Unlock()
			return nil
		}
		wait := b.waitLocked()
		b.mu.Unlock()

		select {
		case <-wait:
			// Capacity changed. Re-check under the lock; another waiter may
			// have acquired it first.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release returns n reserved bytes. Invalid or duplicate releases are
// rejected without changing the current usage.
func (b *byteBudget) Release(n int64) error {
	if b == nil {
		return byteBudgetInvariantf("nil budget")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.validateLocked(); err != nil {
		return err
	}
	if n <= 0 {
		return byteBudgetInvariantf("release must be positive: %d", n)
	}
	if n > b.used {
		return byteBudgetInvariantf("release %d exceeds used %d", n, b.used)
	}

	b.used -= n
	close(b.waitLocked())
	b.wait = make(chan struct{})
	return nil
}

func (b *byteBudget) Used() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func (b *byteBudget) Limit() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit
}

func (b *byteBudget) tryAcquireLocked(n int64) (bool, error) {
	if err := b.validateAcquireLocked(n); err != nil {
		return false, err
	}

	// Subtraction is safe after validateLocked, and this comparison avoids
	// overflowing used+n near math.MaxInt64.
	if n > b.limit-b.used {
		return false, nil
	}
	b.used += n
	return true, nil
}

func (b *byteBudget) validateAcquireLocked(n int64) error {
	if err := b.validateLocked(); err != nil {
		return err
	}
	if n <= 0 {
		return byteBudgetInvariantf("acquire must be positive: %d", n)
	}
	if n > b.limit {
		return byteBudgetInvariantf("acquire %d exceeds limit %d", n, b.limit)
	}
	return nil
}

func (b *byteBudget) validateLocked() error {
	switch {
	case b.limit < 0:
		return byteBudgetInvariantf("negative limit %d", b.limit)
	case b.used < 0:
		return byteBudgetInvariantf("negative used %d", b.used)
	case b.used > b.limit:
		return byteBudgetInvariantf("used %d exceeds limit %d", b.used, b.limit)
	default:
		return nil
	}
}

func (b *byteBudget) waitLocked() chan struct{} {
	if b.wait == nil {
		b.wait = make(chan struct{})
	}
	return b.wait
}

func byteBudgetInvariantf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errByteBudgetInvariant, fmt.Sprintf(format, args...))
}
