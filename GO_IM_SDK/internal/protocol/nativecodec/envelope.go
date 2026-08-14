package nativecodec

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	msyncCompressField = 4
	msyncPayloadField  = 9
	maxEnvelopePayload = 16 << 20
)

// decompressEnvelopePayload removes the optional zlib compression from the
// outer MSync envelope before handing the frame to the native codec.  The
// native codec owns all protocol semantics; this small wire-level adapter only
// needs the two MSync fields that describe compression and payload. Keeping it
// independent of generated Go protobuf code lets the public SDK ship only the
// native static archive and C ABI header.
func decompressEnvelopePayload(data []byte) ([]byte, error) {
	fields, err := parseWireFields(data)
	if err != nil {
		return nil, fmt.Errorf("parse msync envelope: %w", err)
	}
	compression := uint64(0)
	hasCompression := false
	var compressed []byte
	for _, field := range fields {
		switch field.number {
		case msyncCompressField:
			if field.wireType != wireVarint {
				return nil, fmt.Errorf("msync compress_algorimth has wire type %d", field.wireType)
			}
			compression = field.varint
			hasCompression = true
		case msyncPayloadField:
			if field.wireType != wireBytes {
				return nil, fmt.Errorf("msync payload has wire type %d", field.wireType)
			}
			compressed = field.bytes
		}
	}
	if !hasCompression || compression == 0 {
		return data, nil
	}
	if compression != 1 {
		return nil, fmt.Errorf("unsupported payload compression algorithm %d", compression)
	}
	if compressed == nil {
		return nil, fmt.Errorf("compressed msync envelope has no payload")
	}

	reader, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("open zlib payload: %w", err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(reader, maxEnvelopePayload+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read zlib payload: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close zlib payload: %w", closeErr)
	}
	if len(payload) > maxEnvelopePayload {
		return nil, fmt.Errorf("decompressed payload exceeds %d bytes", maxEnvelopePayload)
	}

	var out []byte
	for _, field := range fields {
		switch field.number {
		case msyncCompressField:
			// Preserve the field position and clear compression for the native
			// decoder. MSync defines this as an optional uint32 field.
			out = appendKey(out, field.number, wireVarint)
			out = appendVarint(out, 0)
		case msyncPayloadField:
			out = appendKey(out, field.number, wireBytes)
			out = appendVarint(out, uint64(len(payload)))
			out = append(out, payload...)
		default:
			out = append(out, field.raw...)
		}
	}
	return out, nil
}

const (
	wireVarint     = 0
	wireFixed64    = 1
	wireBytes      = 2
	wireStartGroup = 3
	wireEndGroup   = 4
	wireFixed32    = 5
)

type wireField struct {
	number   uint64
	wireType byte
	varint   uint64
	bytes    []byte
	raw      []byte
}

func parseWireFields(data []byte) ([]wireField, error) {
	fields := make([]wireField, 0, 8)
	for offset := 0; offset < len(data); {
		start := offset
		tag, next, err := readVarint(data, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		number := tag >> 3
		wireType := byte(tag & 7)
		if number == 0 {
			return nil, fmt.Errorf("invalid field number 0")
		}
		field := wireField{number: number, wireType: wireType}
		switch wireType {
		case wireVarint:
			value, next, err := readVarint(data, offset)
			if err != nil {
				return nil, err
			}
			field.varint = value
			offset = next
		case wireFixed64:
			if len(data)-offset < 8 {
				return nil, fmt.Errorf("truncated fixed64 field %d", number)
			}
			offset += 8
		case wireBytes:
			length, next, err := readVarint(data, offset)
			if err != nil {
				return nil, err
			}
			offset = next
			if length > uint64(len(data)-offset) {
				return nil, fmt.Errorf("truncated bytes field %d", number)
			}
			field.bytes = data[offset : offset+int(length)]
			offset += int(length)
		case wireFixed32:
			if len(data)-offset < 4 {
				return nil, fmt.Errorf("truncated fixed32 field %d", number)
			}
			offset += 4
		case wireStartGroup, wireEndGroup:
			return nil, fmt.Errorf("unsupported group wire type %d", wireType)
		default:
			return nil, fmt.Errorf("unsupported wire type %d", wireType)
		}
		field.raw = data[start:offset]
		fields = append(fields, field)
	}
	return fields, nil
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
