package nativecodec

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

const (
	msyncCompressField = 4
	msyncPayloadField  = 9
	maxEnvelopeFields  = 4096
)

// decompressEnvelopePayload removes the optional zlib compression from the
// outer MSync envelope before handing the frame to the native codec.  The
// native codec owns all protocol semantics; this small wire-level adapter only
// needs the two MSync fields that describe compression and payload. Keeping it
// independent of generated Go protobuf code lets the public SDK ship only the
// native static archive and C ABI header.
func decompressEnvelopePayload(data []byte) ([]byte, error) {
	if len(data) > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: msync envelope exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
	envelope, err := scanEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("parse msync envelope: %w", err)
	}
	if !envelope.hasCompression || envelope.compression == 0 {
		return data, nil
	}
	if envelope.compression != 1 {
		return nil, fmt.Errorf("unsupported payload compression algorithm %d", envelope.compression)
	}
	if !envelope.hasPayload {
		return nil, fmt.Errorf("compressed msync envelope has no payload")
	}

	reader, err := zlib.NewReader(bytes.NewReader(envelope.payload))
	if err != nil {
		return nil, fmt.Errorf("open zlib payload: %w", err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(reader, protocol.MaxCodecInputBytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read zlib payload: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close zlib payload: %w", closeErr)
	}
	if len(payload) > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: decompressed payload exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}

	outLen, err := rebuiltEnvelopeSize(len(data), envelope, len(payload))
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, outLen)

	first, second := envelope.compressionRaw, envelope.payloadRaw
	firstPayload := false
	if second.start < first.start {
		first, second = second, first
		firstPayload = true
	}
	out = append(out, data[:first.start]...)
	if firstPayload {
		out = appendPayloadField(out, payload)
	} else {
		out = appendCompressionField(out)
	}
	out = append(out, data[first.end:second.start]...)
	if firstPayload {
		out = appendCompressionField(out)
	} else {
		out = appendPayloadField(out, payload)
	}
	out = append(out, data[second.end:]...)
	return out, nil
}

func estimateDecodeAdmission(data []byte) (protocol.DecodeAdmissionClass, error) {
	if len(data) > protocol.MaxCodecInputBytes {
		return 0, fmt.Errorf("%w: msync envelope exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
	envelope, err := scanEnvelope(data)
	if err != nil {
		return 0, fmt.Errorf("parse msync envelope admission: %w", err)
	}
	if envelope.hasCompression {
		switch envelope.compression {
		case 0:
			// Classify by input size below.
		case 1:
			if !envelope.hasPayload {
				return 0, fmt.Errorf("compressed msync envelope has no payload")
			}
			return protocol.DecodeAdmissionCompressed, nil
		default:
			return protocol.DecodeAdmissionAmbiguous, nil
		}
	}
	switch {
	case len(data) <= 64<<10:
		return protocol.DecodeAdmissionTiny, nil
	case len(data) <= 1<<20:
		return protocol.DecodeAdmissionSmall, nil
	case len(data) <= 4<<20:
		return protocol.DecodeAdmissionLarge, nil
	default:
		return protocol.DecodeAdmissionAmbiguous, nil
	}
}

const (
	wireVarint     = 0
	wireFixed64    = 1
	wireBytes      = 2
	wireStartGroup = 3
	wireEndGroup   = 4
	wireFixed32    = 5
)

type wireRange struct {
	start int
	end   int
}

type envelopeScan struct {
	fieldCount     int
	hasCompression bool
	compression    uint64
	compressionRaw wireRange
	hasPayload     bool
	payload        []byte
	payloadRaw     wireRange
}

// scanEnvelope validates the complete protobuf wire stream while retaining
// only the two singular fields needed by the compression adapter. Its memory
// use is constant regardless of the number of envelope fields.
func scanEnvelope(data []byte) (envelopeScan, error) {
	var result envelopeScan
	for offset := 0; offset < len(data); {
		start := offset
		tag, next, err := readVarint(data, offset)
		if err != nil {
			return envelopeScan{}, err
		}
		offset = next
		number := tag >> 3
		wireType := byte(tag & 7)
		if number == 0 {
			return envelopeScan{}, fmt.Errorf("invalid field number 0")
		}
		result.fieldCount++
		if result.fieldCount > maxEnvelopeFields {
			return envelopeScan{}, fmt.Errorf("%w: msync envelope has more than %d fields", protocol.ErrLimitExceeded, maxEnvelopeFields)
		}
		if number == msyncCompressField && result.hasCompression {
			return envelopeScan{}, fmt.Errorf("%w: duplicate msync compression field", protocol.ErrLimitExceeded)
		}
		if number == msyncPayloadField && result.hasPayload {
			return envelopeScan{}, fmt.Errorf("%w: duplicate msync payload field", protocol.ErrLimitExceeded)
		}

		var varintValue uint64
		var bytesValue []byte
		switch wireType {
		case wireVarint:
			value, next, err := readVarint(data, offset)
			if err != nil {
				return envelopeScan{}, err
			}
			varintValue = value
			offset = next
		case wireFixed64:
			if len(data)-offset < 8 {
				return envelopeScan{}, fmt.Errorf("truncated fixed64 field %d", number)
			}
			offset += 8
		case wireBytes:
			length, next, err := readVarint(data, offset)
			if err != nil {
				return envelopeScan{}, err
			}
			offset = next
			if length > uint64(len(data)-offset) {
				return envelopeScan{}, fmt.Errorf("truncated bytes field %d", number)
			}
			bytesValue = data[offset : offset+int(length)]
			offset += int(length)
		case wireFixed32:
			if len(data)-offset < 4 {
				return envelopeScan{}, fmt.Errorf("truncated fixed32 field %d", number)
			}
			offset += 4
		case wireStartGroup, wireEndGroup:
			return envelopeScan{}, fmt.Errorf("unsupported group wire type %d", wireType)
		default:
			return envelopeScan{}, fmt.Errorf("unsupported wire type %d", wireType)
		}

		switch number {
		case msyncCompressField:
			if wireType != wireVarint {
				return envelopeScan{}, fmt.Errorf("msync compress_algorimth has wire type %d", wireType)
			}
			result.hasCompression = true
			result.compression = varintValue
			result.compressionRaw = wireRange{start: start, end: offset}
		case msyncPayloadField:
			if wireType != wireBytes {
				return envelopeScan{}, fmt.Errorf("msync payload has wire type %d", wireType)
			}
			result.hasPayload = true
			result.payload = bytesValue
			result.payloadRaw = wireRange{start: start, end: offset}
		}
	}
	return result, nil
}

func rebuiltEnvelopeSize(inputLen int, envelope envelopeScan, payloadLen int) (int, error) {
	removed := (envelope.compressionRaw.end - envelope.compressionRaw.start) +
		(envelope.payloadRaw.end - envelope.payloadRaw.start)
	if removed < 0 || removed > inputLen {
		return 0, fmt.Errorf("invalid msync envelope field ranges")
	}

	total := uint64(inputLen - removed)
	var ok bool
	total, ok = checkedAdd(total, uint64(varintSize(msyncCompressField<<3|wireVarint)+1))
	if !ok {
		return 0, fmt.Errorf("%w: rebuilt msync envelope length overflows", protocol.ErrLimitExceeded)
	}
	total, ok = checkedAdd(total, uint64(varintSize(msyncPayloadField<<3|wireBytes)))
	if !ok {
		return 0, fmt.Errorf("%w: rebuilt msync envelope length overflows", protocol.ErrLimitExceeded)
	}
	total, ok = checkedAdd(total, uint64(varintSize(uint64(payloadLen))))
	if !ok {
		return 0, fmt.Errorf("%w: rebuilt msync envelope length overflows", protocol.ErrLimitExceeded)
	}
	total, ok = checkedAdd(total, uint64(payloadLen))
	if !ok || total > protocol.MaxCodecInputBytes {
		return 0, fmt.Errorf("%w: rebuilt msync envelope exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
	return int(total), nil
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if a > ^uint64(0)-b {
		return 0, false
	}
	return a + b, true
}

func varintSize(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

func appendCompressionField(dst []byte) []byte {
	dst = appendKey(dst, msyncCompressField, wireVarint)
	return appendVarint(dst, 0)
}

func appendPayloadField(dst, payload []byte) []byte {
	dst = appendKey(dst, msyncPayloadField, wireBytes)
	dst = appendVarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

func readVarint(data []byte, offset int) (uint64, int, error) {
	var value uint64
	for i := 0; i < 10; i++ {
		if offset >= len(data) {
			return 0, 0, fmt.Errorf("truncated varint")
		}
		b := data[offset]
		offset++
		if i == 9 && b > 1 {
			return 0, 0, fmt.Errorf("varint overflow")
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return value, offset, nil
		}
	}
	return 0, 0, fmt.Errorf("varint overflow")
}

func appendKey(dst []byte, number uint64, wireType byte) []byte {
	return appendVarint(dst, number<<3|uint64(wireType))
}

func appendVarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}
