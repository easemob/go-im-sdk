package sdk

import (
	"fmt"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

const (
	// decodeAdmissionWait must absorb a GC stop-the-world pause or a scheduling
	// stall on a CPU-starved host without escalating to a disconnect. readPump
	// is blocked for the duration, so the ceiling is the 240s heartbeat
	// tolerance rather than anything tighter; 3s stays far below the 30s SYNC
	// watchdog while giving transient pressure room to clear.
	decodeAdmissionWait       = 3 * time.Second
	decodeAdmissionTinyBytes  = int64(4 << 20)
	decodeAdmissionSmallBytes = int64(16 << 20)
	decodeAdmissionLargeBytes = int64(32 << 20)
	decodeAdmissionMaxBytes   = int64(64 << 20)
)

func decodeAdmissionWeight(codec internalprotocol.Codec, data []byte) (int64, error) {
	estimator, ok := codec.(internalprotocol.DecodeAdmissionEstimator)
	if !ok {
		return decodeAdmissionMaxBytes, nil
	}
	class, err := estimator.EstimateDecodeAdmission(data)
	if err != nil {
		return 0, err
	}
	switch class {
	case internalprotocol.DecodeAdmissionTiny:
		return decodeAdmissionTinyBytes, nil
	case internalprotocol.DecodeAdmissionSmall:
		return decodeAdmissionSmallBytes, nil
	case internalprotocol.DecodeAdmissionLarge:
		return decodeAdmissionLargeBytes, nil
	case internalprotocol.DecodeAdmissionCompressed, internalprotocol.DecodeAdmissionAmbiguous:
		return decodeAdmissionMaxBytes, nil
	default:
		return 0, fmt.Errorf("unknown decode admission class %d", class)
	}
}
