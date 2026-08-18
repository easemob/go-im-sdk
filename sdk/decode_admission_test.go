package sdk

import (
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

type admissionTestCodec struct {
	messageTestCodec
	class internalprotocol.DecodeAdmissionClass
	err   error
}

func (c *admissionTestCodec) EstimateDecodeAdmission([]byte) (internalprotocol.DecodeAdmissionClass, error) {
	return c.class, c.err
}

func TestDecodeAdmissionWeights(t *testing.T) {
	tests := []struct {
		class internalprotocol.DecodeAdmissionClass
		want  int64
	}{
		{internalprotocol.DecodeAdmissionTiny, decodeAdmissionTinyBytes},
		{internalprotocol.DecodeAdmissionSmall, decodeAdmissionSmallBytes},
		{internalprotocol.DecodeAdmissionLarge, decodeAdmissionLargeBytes},
		{internalprotocol.DecodeAdmissionCompressed, decodeAdmissionMaxBytes},
		{internalprotocol.DecodeAdmissionAmbiguous, decodeAdmissionMaxBytes},
	}
	for _, tt := range tests {
		got, err := decodeAdmissionWeight(&admissionTestCodec{class: tt.class}, nil)
		if err != nil || got != tt.want {
			t.Fatalf("class %d: weight=%d err=%v, want %d", tt.class, got, err, tt.want)
		}
	}
	var codec internalprotocol.Codec = &messageTestCodec{}
	if got, err := decodeAdmissionWeight(codec, nil); err != nil || got != decodeAdmissionMaxBytes {
		t.Fatalf("fallback weight=%d err=%v", got, err)
	}
}
