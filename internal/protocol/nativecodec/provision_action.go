package nativecodec

import (
	"fmt"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

// Provision.action_version is protobuf field 17 (length-delimited string).
const provisionActionVersionField = 17

// withProvisionActionVersion writes protocol.ActionVersion onto the inner
// Provision payload of an MSync envelope. Native encode does not carry this
// field; the Go SDK owns the advertised feature version.
func withProvisionActionVersion(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	envelope, err := scanEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("parse msync envelope: %w", err)
	}
	if !envelope.hasPayload {
		return nil, fmt.Errorf("msync envelope has no payload")
	}
	payload, changed, err := upsertStringField(envelope.payload, provisionActionVersionField, protocol.ActionVersion)
	if err != nil {
		return nil, err
	}
	if !changed {
		return data, nil
	}
	outLen := envelope.payloadRaw.start + bytesFieldSize(msyncPayloadField, len(payload)) + (len(data) - envelope.payloadRaw.end)
	if outLen < 0 || outLen > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: rebuilt msync envelope exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
	out := make([]byte, 0, outLen)
	out = append(out, data[:envelope.payloadRaw.start]...)
	out = appendPayloadField(out, payload)
	return append(out, data[envelope.payloadRaw.end:]...), nil
}

func bytesFieldSize(number uint64, payloadLen int) int {
	return varintSize(number<<3|wireBytes) + varintSize(uint64(payloadLen)) + payloadLen
}

func upsertStringField(message []byte, number uint64, value string) ([]byte, bool, error) {
	offset := 0
	for offset < len(message) {
		start := offset
		tag, next, err := readVarint(message, offset)
		if err != nil {
			return nil, false, err
		}
		offset = next
		field := tag >> 3
		wireType := byte(tag & 7)
		end, err := skipField(message, offset, wireType)
		if err != nil {
			return nil, false, err
		}
		if field == number {
			if wireType != wireBytes {
				return nil, false, fmt.Errorf("provision field %d has wire type %d", number, wireType)
			}
			length, valueStart, err := readVarint(message, offset)
			if err != nil {
				return nil, false, err
			}
			if length > uint64(len(message)-valueStart) {
				return nil, false, fmt.Errorf("truncated string field %d", number)
			}
			if string(message[valueStart:valueStart+int(length)]) == value {
				return message, false, nil
			}
			out := make([]byte, 0, start+bytesFieldSize(number, len(value))+len(message)-end)
			out = append(out, message[:start]...)
			out = appendStringField(out, number, value)
			out = append(out, message[end:]...)
			return out, true, nil
		}
		offset = end
	}
	out := make([]byte, 0, len(message)+bytesFieldSize(number, len(value)))
	out = append(out, message...)
	return appendStringField(out, number, value), true, nil
}

func appendStringField(dst []byte, number uint64, value string) []byte {
	dst = appendKey(dst, number, wireBytes)
	dst = appendVarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func skipField(data []byte, offset int, wireType byte) (int, error) {
	switch wireType {
	case wireVarint:
		_, next, err := readVarint(data, offset)
		return next, err
	case wireFixed64:
		if len(data)-offset < 8 {
			return 0, fmt.Errorf("truncated fixed64")
		}
		return offset + 8, nil
	case wireBytes:
		length, next, err := readVarint(data, offset)
		if err != nil {
			return 0, err
		}
		if length > uint64(len(data)-next) {
			return 0, fmt.Errorf("truncated bytes")
		}
		return next + int(length), nil
	case wireFixed32:
		if len(data)-offset < 4 {
			return 0, fmt.Errorf("truncated fixed32")
		}
		return offset + 4, nil
	default:
		return 0, fmt.Errorf("unsupported wire type %d", wireType)
	}
}
