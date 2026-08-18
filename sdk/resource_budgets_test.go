package sdk

import (
	"errors"
	"testing"
)

func TestBatchReservationAcquireRollbackAndRelease(t *testing.T) {
	client := mustNewByteBudget(8)
	process := mustNewByteBudget(6)

	reservation, ok, err := tryReserveBatch(client, process, 4)
	if err != nil || !ok || reservation.Weight() != 4 {
		t.Fatalf("reserve = (%v, %v, %v)", reservation, ok, err)
	}
	if client.Used() != 4 || process.Used() != 4 {
		t.Fatalf("used client=%d process=%d", client.Used(), process.Used())
	}
	if err := reservation.Release(); err != nil {
		t.Fatal(err)
	}
	if client.Used() != 0 || process.Used() != 0 {
		t.Fatalf("used after release client=%d process=%d", client.Used(), process.Used())
	}
	if err := reservation.Release(); !errors.Is(err, errByteBudgetInvariant) {
		t.Fatalf("duplicate release = %v", err)
	}

	if acquired, err := process.TryAcquire(6); err != nil || !acquired {
		t.Fatalf("fill process = (%v, %v)", acquired, err)
	}
	reservation, ok, err = tryReserveBatch(client, process, 4)
	if err != nil || ok || reservation != nil {
		t.Fatalf("process-full reserve = (%v, %v, %v)", reservation, ok, err)
	}
	if client.Used() != 0 {
		t.Fatalf("client rollback left %d bytes", client.Used())
	}
	if err := process.Release(6); err != nil {
		t.Fatal(err)
	}

	client = mustNewByteBudget(4)
	process = mustNewByteBudget(8)
	if acquired, fillErr := client.TryAcquire(1); fillErr != nil || !acquired {
		t.Fatalf("fill client=(%v,%v)", acquired, fillErr)
	}
	reservation, ok, scope, err := tryReserveBatchScoped(client, process, 4)
	if err != nil || ok || reservation != nil || scope != batchBudgetScopeClient {
		t.Fatalf("client scope reserve=(%v,%v,%v,%v)", reservation, ok, scope, err)
	}
	if err := client.Release(1); err != nil {
		t.Fatal(err)
	}
}
