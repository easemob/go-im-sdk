package sdk

import (
	"sync"
	"testing"
)

func TestTraceIDGeneratorUniqueConcurrent(t *testing.T) {
	const goroutines = 64
	const perGoroutine = 20000

	var g traceIDGenerator
	seen := make(map[uint64]struct{}, goroutines*perGoroutine)
	var mu sync.Mutex
	var wg sync.WaitGroup
	dup := make(chan uint64, 1)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				id := g.next()
				mu.Lock()
				if _, ok := seen[id]; ok {
					select {
					case dup <- id:
					default:
					}
				}
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	select {
	case id := <-dup:
		t.Fatalf("duplicate trace id %d", id)
	default:
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("expected %d unique ids, got %d", goroutines*perGoroutine, len(seen))
	}
}

func TestTraceIDGeneratorMonotonic(t *testing.T) {
	var g traceIDGenerator
	prev := g.next()
	for i := 0; i < 100000; i++ {
		next := g.next()
		if next <= prev {
			t.Fatalf("non-monotonic: %d <= %d", next, prev)
		}
		prev = next
	}
}

func TestTraceIDGeneratorBitLayout(t *testing.T) {
	var g traceIDGenerator
	id := g.next()
	ms := id >> traceSequenceBits
	seq := id & traceSequenceMask

	// 时间戳部分应为 Unix 毫秒（2020~2100 约 1.5e12 ~ 4.2e12）。
	if ms < 1_500_000_000_000 || ms > 4_200_000_000_000 {
		t.Fatalf("unexpected timestamp portion: %d", ms)
	}
	if seq >= 1<<traceSequenceBits {
		t.Fatalf("sequence portion overflow: %d", seq)
	}
}
