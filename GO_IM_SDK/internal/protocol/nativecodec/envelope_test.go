package nativecodec

import (
	"bytes"
	"compress/zlib"
	"testing"
)

func TestDecompressEnvelopePayload(t *testing.T) {
	originalPayload := []byte("uncompressed msync payload")
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(originalPayload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	var input []byte
	input = appendKey(input, 1, wireVarint)
	input = appendVarint(input, 1)
	input = appendKey(input, msyncCompressField, wireVarint)
	input = appendVarint(input, 1)
	input = appendKey(input, msyncPayloadField, wireBytes)
	input = appendVarint(input, uint64(compressed.Len()))
	input = append(input, compressed.Bytes()...)
	input = appendKey(input, 20, wireVarint)
	input = appendVarint(input, 42)

	decoded, err := decompressEnvelopePayload(input)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := parseWireFields(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var gotCompression uint64
	var gotPayload []byte
	var unknownRaw []byte
	for _, field := range fields {
		switch field.number {
		case msyncCompressField:
			gotCompression = field.varint
		case msyncPayloadField:
			gotPayload = field.bytes
		case 20:
			unknownRaw = field.raw
		}
	}
	if gotCompression != 0 {
		t.Fatalf("compression field = %d, want 0", gotCompression)
	}
	if !bytes.Equal(gotPayload, originalPayload) {
		t.Fatalf("payload = %q, want %q", gotPayload, originalPayload)
	}
	if len(unknownRaw) == 0 {
		t.Fatal("unknown envelope field was not preserved")
	}
}

func TestDecompressEnvelopePayloadLeavesUncompressedDataUntouched(t *testing.T) {
	input := []byte{0x08, 0x01, 0x20, 0x00}
	decoded, err := decompressEnvelopePayload(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, input) {
		t.Fatalf("decoded = %x, want %x", decoded, input)
	}
}

func TestParseWireFieldsRejectsMalformedInput(t *testing.T) {
	for _, input := range [][]byte{{0x80}, {0x00}, {0x0a, 0x02, 0x01}} {
		if _, err := parseWireFields(input); err == nil {
			t.Fatalf("parseWireFields(%x) unexpectedly succeeded", input)
		}
	}
}
