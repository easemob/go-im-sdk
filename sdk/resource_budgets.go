package sdk

import (
	"errors"
	"fmt"
	"sync"
)

const (
	clientBatchBudgetBytes     = int64(32 << 20)
	processBatchBudgetBytes    = int64(256 << 20)
	processDecodeBudgetBytes   = int64(256 << 20)
	connectionQueueBudgetBytes = int64(4 << 20)
	processQueueBudgetBytes    = int64(64 << 20)
)

var (
	processBatchBudget  = mustNewByteBudget(processBatchBudgetBytes)
	processDecodeBudget = mustNewByteBudget(processDecodeBudgetBytes)
	processQueueBudget  = mustNewByteBudget(processQueueBudgetBytes)
)

func mustNewByteBudget(limit int64) *byteBudget {
	b, err := newByteBudget(limit)
	if err != nil {
		panic(err)
	}
	return b
}

// batchReservation owns matching charges in a Client-local and process-wide
// budget. Ownership transfers to a batchJob only after the job is enqueued.
type batchReservation struct {
	mu       sync.Mutex
	client   *byteBudget
	process  *byteBudget
	weight   int64
	released bool
}

type batchBudgetScope uint8

const (
	batchBudgetScopeNone batchBudgetScope = iota
	batchBudgetScopeClient
	batchBudgetScopeProcess
)

func tryReserveBatch(client, process *byteBudget, weight int64) (*batchReservation, bool, error) {
	reservation, ok, _, err := tryReserveBatchScoped(client, process, weight)
	return reservation, ok, err
}

func tryReserveBatchScoped(client, process *byteBudget, weight int64) (*batchReservation, bool, batchBudgetScope, error) {
	clientOK, err := client.TryAcquire(weight)
	if err != nil || !clientOK {
		return nil, false, batchBudgetScopeClient, err
	}
	processOK, err := process.TryAcquire(weight)
	if err != nil || !processOK {
		if releaseErr := client.Release(weight); releaseErr != nil {
			return nil, false, batchBudgetScopeProcess, errors.Join(err, releaseErr)
		}
		return nil, false, batchBudgetScopeProcess, err
	}
	return &batchReservation{client: client, process: process, weight: weight}, true, batchBudgetScopeNone, nil
}

func (r *batchReservation) Weight() int64 {
	if r == nil {
		return 0
	}
	return r.weight
}

func (r *batchReservation) Release() error {
	if r == nil {
		return fmt.Errorf("%w: nil batch reservation", errByteBudgetInvariant)
	}
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return fmt.Errorf("%w: batch reservation released twice", errByteBudgetInvariant)
	}
	r.released = true
	r.mu.Unlock()

	clientErr := r.client.Release(r.weight)
	processErr := r.process.Release(r.weight)
	return errors.Join(clientErr, processErr)
}
