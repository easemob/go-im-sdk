package sdk

import (
	"fmt"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

const (
	decodeAdmissionWait       = 500 * time.Millisecond
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
