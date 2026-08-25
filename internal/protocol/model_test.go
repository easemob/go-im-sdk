package protocol

import (
	"errors"
	"strings"
	"testing"
)

func TestWireEnumValues(t *testing.T) {
	checks := map[string]struct{ got, want int32 }{
		"command sync": {int32(CommandSync), 0}, "command provision": {int32(CommandProvision), 3},
		"namespace statistic": {int32(NamespaceStatistic), 0}, "namespace chat": {int32(NamespaceChat), 1}, "namespace notify": {int32(NamespaceNotify), 5},
		"route online": {int32(RouteOnline), 1}, "route direct": {int32(RouteDirect), 2},
		"status redirect": {int32(StatusRedirect), 5}, "status resource changed": {int32(StatusResourceChanged), 20},
		"content video": {int32(ContentVideo), 2}, "content voice": {int32(ContentVoice), 4}, "content custom": {int32(ContentCustom), 7},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s=%d want %d", name, c.got, c.want)
		}
	}
}

func TestSyncRetainedWeightAccountsForDynamicCapacity(t *testing.T) {
	base := &Sync{Metas: []Meta{{Payload: []byte{1}}}}
	baseWeight, err := SyncRetainedWeight(base)
	if err != nil {
		t.Fatal(err)
	}
	withCapacity := &Sync{
		Status: &Status{Reason: strings.Repeat("r", 1024), Redirects: make([]RedirectInfo, 1, 8)},
		Queue:  &JID{AppKey: "app", Name: "queue", Domain: "example.test", ClientResource: "resource"},
		Metas:  make([]Meta, 1, 8),
	}
	withCapacity.Metas[0] = Meta{
		From:          JID{AppKey: "app", Name: "from", Domain: "example.test"},
		To:            JID{AppKey: "app", Name: "to", Domain: "example.test"},
		Payload:       make([]byte, 1, 1<<20),
		Ext:           make([]KeyValue, 1, 8),
		DirectedUsers: make([]string, 1, 16),
		Environment:   "production",
	}
	withCapacity.Metas[0].Ext[0] = KeyValue{Key: "key", String: strings.Repeat("v", 1024)}
	withCapacity.Metas[0].DirectedUsers[0] = "user"
	capacityWeight, err := SyncRetainedWeight(withCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if capacityWeight <= baseWeight {
		t.Fatalf("capacity weight = %d, want greater than base %d", capacityWeight, baseWeight)
	}
	if uint64(capacityWeight)%retainedPageSize != 0 {
		t.Fatalf("capacity weight = %d, want %d-byte page rounding", capacityWeight, retainedPageSize)
	}
}

func TestSyncRetainedWeightAccountsForMetaAttributes(t *testing.T) {
	base := &Sync{Metas: []Meta{{Payload: []byte{1}}}}
	baseWeight, err := SyncRetainedWeight(base)
	if err != nil {
		t.Fatal(err)
	}
	withAttributes := &Sync{Metas: []Meta{{Payload: []byte{1}, Attributes: make([]byte, 1<<20)}}}
	attributesWeight, err := SyncRetainedWeight(withAttributes)
	if err != nil {
		t.Fatal(err)
	}
	if attributesWeight <= baseWeight {
		t.Fatalf("attributes weight = %d, want greater than base %d", attributesWeight, baseWeight)
	}
}

func TestSyncRetainedWeightRejectsOverLimit(t *testing.T) {
	sync := &Sync{Metas: []Meta{{Payload: make([]byte, 5<<20)}}}
	weight, err := SyncRetainedWeight(sync)
	if weight != 0 || !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("SyncRetainedWeight() = (%d, %v), want 0 ErrLimitExceeded", weight, err)
	}
}

func TestSyncRetainedWeightAcceptsConfiguredSyncMaxima(t *testing.T) {
	sync := &Sync{Metas: make([]Meta, MaxSyncMetas)}
	user := strings.Repeat("u", MaxSyncStringBytes/MaxSyncDirectedUsers)
	for i := range sync.Metas {
		sync.Metas[i].Payload = make([]byte, MaxSyncPayloadBytes/MaxSyncMetas)
		sync.Metas[i].DirectedUsers = []string{user, user, user, user}
	}
	weight, err := SyncRetainedWeight(sync)
	if err != nil {
		t.Fatal(err)
	}
	if weight <= 0 || weight > MaxSyncRetainedWeight {
		t.Fatalf("SyncRetainedWeight() = %d, want within (0, %d]", weight, MaxSyncRetainedWeight)
	}
}

func TestSyncRetainedWeightCheckedArithmetic(t *testing.T) {
	if _, ok := checkedRetainedAdd(^uint64(0), 1); ok {
		t.Fatal("checkedRetainedAdd accepted overflow")
	}
	if _, ok := checkedRetainedMul(^uint64(0), 2); ok {
		t.Fatal("checkedRetainedMul accepted overflow")
	}
	if _, ok := checkedRetainedRoundUp(^uint64(0), retainedPageSize); ok {
		t.Fatal("checkedRetainedRoundUp accepted overflow")
	}
}
