package nativecodec

import (
	"bytes"
	"compress/zlib"
	"errors"
	"testing"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

func TestDecompressEnvelopePayload(t *testing.T) {
	originalPayload := []byte("uncompressed msync payload")
	compressed := compressForTest(t, originalPayload)

	// Use non-canonical varints and different wire types around the two fields
	// to prove that unknown fields remain byte-for-byte intact and ordered.
	unknownBefore := []byte{0x88, 0x00, 0x81, 0x00}
	unknownMiddle := []byte{0x15, 0x01, 0x02, 0x03, 0x04}
	unknownAfter := []byte{0xa2, 0x01, 0x03, 'e', 'n', 'd'}
	var input []byte
	input = append(input, unknownBefore...)
	input = appendCompressionForTest(input, 1)
	input = append(input, unknownMiddle...)
	input = appendBytesFieldForTest(input, msyncPayloadField, compressed)
	input = append(input, unknownAfter...)

	decoded, err := decompressEnvelopePayload(input)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	want = append(want, unknownBefore...)
	want = appendCompressionForTest(want, 0)
	want = append(want, unknownMiddle...)
	want = appendBytesFieldForTest(want, msyncPayloadField, originalPayload)
	want = append(want, unknownAfter...)
	if !bytes.Equal(decoded, want) {
		t.Fatalf("decoded envelope mismatch\n got: %x\nwant: %x", decoded, want)
	}
}

func TestDecompressEnvelopePayloadPreservesPayloadBeforeCompressionOrder(t *testing.T) {
	payload := []byte("payload before compression")
	var input []byte
	input = appendBytesFieldForTest(input, msyncPayloadField, compressForTest(t, payload))
	input = appendVarintFieldForTest(input, 20, 42)
	input = appendCompressionForTest(input, 1)

	decoded, err := decompressEnvelopePayload(input)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	want = appendBytesFieldForTest(want, msyncPayloadField, payload)
	want = appendVarintFieldForTest(want, 20, 42)
	want = appendCompressionForTest(want, 0)
	if !bytes.Equal(decoded, want) {
		t.Fatalf("decoded envelope mismatch\n got: %x\nwant: %x", decoded, want)
	}
}

func TestDecompressEnvelopePayloadLeavesUncompressedDataUntouchedWithoutAllocating(t *testing.T) {
	input := []byte{0x08, 0x01, 0x20, 0x00, 0x4a, 0x01, 0xff}
	decoded, err := decompressEnvelopePayload(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, input) || &decoded[0] != &input[0] {
		t.Fatalf("decoded = %x, want original input slice %x", decoded, input)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		got, gotErr := decompressEnvelopePayload(input)
		if gotErr != nil || len(got) != len(input) {
			panic("unexpected uncompressed envelope result")
		}
	}); allocs != 0 {
		t.Fatalf("uncompressed envelope allocated %.2f objects/run, want 0", allocs)
	}
}

func TestDecompressEnvelopePayloadRejectsDuplicateSingularFields(t *testing.T) {
	compressed := compressForTest(t, []byte("valid"))
	tests := map[string][]byte{
		"payload": func() []byte {
			var input []byte
			input = appendCompressionForTest(input, 1)
			input = appendBytesFieldForTest(input, msyncPayloadField, nil)
			return appendBytesFieldForTest(input, msyncPayloadField, compressed)
		}(),
		"compression": func() []byte {
			var input []byte
			input = appendCompressionForTest(input, 0)
			input = appendCompressionForTest(input, 1)
			return appendBytesFieldForTest(input, msyncPayloadField, compressed)
		}(),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			decoded, err := decompressEnvelopePayload(input)
			if decoded != nil || !errors.Is(err, protocol.ErrLimitExceeded) {
				t.Fatalf("decompressEnvelopePayload() = (%x, %v), want nil ErrLimitExceeded", decoded, err)
			}
		})
	}
}

func TestDecompressEnvelopePayloadRejectsFieldStormWithBoundedAllocation(t *testing.T) {
	const fields = 1_000_000
	input := make([]byte, 0, fields*2)
	for i := 0; i < fields; i++ {
		input = append(input, 0x08, 0x00)
	}
	if _, err := decompressEnvelopePayload(input); !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("decompressEnvelopePayload() error = %v, want ErrLimitExceeded", err)
	}

	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := decompressEnvelopePayload(input); !errors.Is(err, protocol.ErrLimitExceeded) {
				b.Fatalf("decompressEnvelopePayload() error = %v, want ErrLimitExceeded", err)
			}
		}
	})
	if allocated := result.AllocedBytesPerOp(); allocated >= 256<<10 {
		t.Fatalf("field storm allocated %d bytes/op, want < %d", allocated, 256<<10)
	}
}

func TestDecompressEnvelopePayloadRebuiltSizeBoundary(t *testing.T) {
	const rebuiltOverhead = 7 // field 4 + field 9 key + four-byte payload length
	exactPayloadLen := protocol.MaxCodecInputBytes - rebuiltOverhead
	allZeros := make([]byte, protocol.MaxCodecInputBytes+1)

	tests := []struct {
		name      string
		payload   []byte
		unknown   []byte
		wantLen   int
		wantLimit bool
	}{
		{name: "exactly maximum", payload: allZeros[:exactPayloadLen], wantLen: protocol.MaxCodecInputBytes},
		{name: "one byte over", payload: allZeros[:exactPayloadLen+1], wantLimit: true},
		{name: "payload itself maximum", payload: allZeros[:protocol.MaxCodecInputBytes], wantLimit: true},
		{name: "decompressed payload over maximum", payload: allZeros, wantLimit: true},
		{name: "unknown fields push total over", payload: allZeros[:exactPayloadLen], unknown: []byte{0x08, 0x00}, wantLimit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input []byte
			input = append(input, tt.unknown...)
			input = appendCompressionForTest(input, 1)
			input = appendBytesFieldForTest(input, msyncPayloadField, compressForTest(t, tt.payload))
			decoded, err := decompressEnvelopePayload(input)
			if tt.wantLimit {
				if decoded != nil || !errors.Is(err, protocol.ErrLimitExceeded) {
					t.Fatalf("decompressEnvelopePayload() = (%d bytes, %v), want nil ErrLimitExceeded", len(decoded), err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(decoded) != tt.wantLen {
				t.Fatalf("decoded length = %d, want %d", len(decoded), tt.wantLen)
			}
		})
	}
}

func TestDecompressEnvelopePayloadRejectsOversizedUncompressedEnvelope(t *testing.T) {
	input := make([]byte, protocol.MaxCodecInputBytes+1)
	if decoded, err := decompressEnvelopePayload(input); decoded != nil || !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("decompressEnvelopePayload() = (%d bytes, %v), want nil ErrLimitExceeded", len(decoded), err)
	}
}

func TestDecompressEnvelopePayloadRejectsMalformedInput(t *testing.T) {
	var overflowValue = []byte{0x08}
	overflowValue = append(overflowValue, bytes.Repeat([]byte{0x80}, 9)...)
	overflowValue = append(overflowValue, 0x02)
	tests := [][]byte{
		{0x80},
		{0x00},
		{0x0a, 0x02, 0x01},
		{0x0b},
		{0x0e},
		bytes.Repeat([]byte{0xff}, 10),
		overflowValue,
		{0x22, 0x00},
		{0x48, 0x00},
	}
	for _, input := range tests {
		if _, err := decompressEnvelopePayload(input); err == nil {
			t.Fatalf("decompressEnvelopePayload(%x) unexpectedly succeeded", input)
		} else if errors.Is(err, protocol.ErrLimitExceeded) {
			t.Fatalf("malformed input %x was misclassified as a limit error: %v", input, err)
		}
	}
}

func TestDecompressEnvelopePayloadAcceptsOverlongVarints(t *testing.T) {
	input := []byte{0x88, 0x00, 0x80, 0x00}
	decoded, err := decompressEnvelopePayload(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, input) {
		t.Fatalf("decoded = %x, want %x", decoded, input)
	}
}

func TestDecompressEnvelopePayloadCompressionErrors(t *testing.T) {
	tests := [][]byte{
		appendCompressionForTest(nil, 2),
		appendCompressionForTest(nil, 1),
		appendBytesFieldForTest(appendCompressionForTest(nil, 1), msyncPayloadField, []byte("not zlib")),
	}
	for _, input := range tests {
		if _, err := decompressEnvelopePayload(input); err == nil {
			t.Fatalf("decompressEnvelopePayload(%x) unexpectedly succeeded", input)
		} else if errors.Is(err, protocol.ErrLimitExceeded) {
			t.Fatalf("compression error %x was misclassified as a limit error: %v", input, err)
		}
	}
}

func TestEstimateDecodeAdmission(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  protocol.DecodeAdmissionClass
	}{
		{name: "tiny boundary", input: envelopeWithSizeForTest(t, 64<<10), want: protocol.DecodeAdmissionTiny},
		{name: "small lower boundary", input: envelopeWithSizeForTest(t, (64<<10)+1), want: protocol.DecodeAdmissionSmall},
		{name: "small upper boundary", input: envelopeWithSizeForTest(t, 1<<20), want: protocol.DecodeAdmissionSmall},
		{name: "large lower boundary", input: envelopeWithSizeForTest(t, (1<<20)+1), want: protocol.DecodeAdmissionLarge},
		{name: "large upper boundary", input: envelopeWithSizeForTest(t, 4<<20), want: protocol.DecodeAdmissionLarge},
		{name: "above websocket limit", input: envelopeWithSizeForTest(t, (4<<20)+1), want: protocol.DecodeAdmissionAmbiguous},
		{name: "compressed", input: appendBytesFieldForTest(appendCompressionForTest(nil, 1), msyncPayloadField, []byte{1}), want: protocol.DecodeAdmissionCompressed},
		{name: "unsupported compression", input: appendCompressionForTest(nil, 2), want: protocol.DecodeAdmissionAmbiguous},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := estimateDecodeAdmission(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("estimateDecodeAdmission() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEstimateDecodeAdmissionRejectsInvalidPreflight(t *testing.T) {
	duplicate := appendCompressionForTest(appendCompressionForTest(nil, 0), 0)
	if _, err := estimateDecodeAdmission(duplicate); !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("duplicate compression error = %v, want ErrLimitExceeded", err)
	}
	if _, err := estimateDecodeAdmission([]byte{0x80}); err == nil || errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("malformed wire error = %v, want ordinary malformed error", err)
	}
	if _, err := estimateDecodeAdmission(appendCompressionForTest(nil, 1)); err == nil {
		t.Fatal("compressed envelope without payload unexpectedly classified")
	}
}

func FuzzDecompressEnvelopePayload(f *testing.F) {
	plain := appendCompressionForTest(appendVarintFieldForTest(nil, 1, 1), 0)
	f.Add(plain)
	f.Add(appendCompressionForTest(appendCompressionForTest(nil, 0), 1))
	f.Add(appendBytesFieldForTest(appendBytesFieldForTest(nil, msyncPayloadField, nil), msyncPayloadField, nil))
	storm := make([]byte, 0, (maxEnvelopeFields+1)*2)
	for i := 0; i <= maxEnvelopeFields; i++ {
		storm = append(storm, 0x08, 0x00)
	}
	f.Add(storm)
	bomb := compressForFuzz(bytes.Repeat([]byte{0}, 256<<10))
	f.Add(appendBytesFieldForTest(appendCompressionForTest(nil, 1), msyncPayloadField, bomb))
	ordered := appendBytesFieldForTest(nil, msyncPayloadField, compressForFuzz([]byte("order")))
	ordered = appendCompressionForTest(ordered, 1)
	f.Add(ordered)

	f.Fuzz(func(t *testing.T, input []byte) {
		decoded, err := decompressEnvelopePayload(input)
		if err != nil {
			return
		}
		if len(decoded) > protocol.MaxCodecInputBytes {
			t.Fatalf("successful output length = %d, want <= %d", len(decoded), protocol.MaxCodecInputBytes)
		}
		scan, err := scanEnvelope(decoded)
		if err != nil {
			t.Fatalf("successful output did not rescan: %v", err)
		}
		if scan.hasCompression && scan.compression != 0 {
			t.Fatalf("successful output retained compression algorithm %d", scan.compression)
		}
	})
}

func compressForTest(t *testing.T, payload []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func compressForFuzz(payload []byte) []byte {
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	_, _ = writer.Write(payload)
	_ = writer.Close()
	return compressed.Bytes()
}

func appendCompressionForTest(dst []byte, algorithm uint64) []byte {
	return appendVarintFieldForTest(dst, msyncCompressField, algorithm)
}

func appendVarintFieldForTest(dst []byte, number uint64, value uint64) []byte {
	dst = appendKey(dst, number, wireVarint)
	return appendVarint(dst, value)
}

func appendBytesFieldForTest(dst []byte, number uint64, value []byte) []byte {
	dst = appendKey(dst, number, wireBytes)
	dst = appendVarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func envelopeWithSizeForTest(t *testing.T, size int) []byte {
	t.Helper()
	for payloadSize := size - 2; payloadSize >= 0; payloadSize-- {
		if 1+varintSize(uint64(payloadSize))+payloadSize == size {
			return appendBytesFieldForTest(nil, 1, make([]byte, payloadSize))
		}
	}
	t.Fatalf("cannot construct %d-byte envelope", size)
	return nil
}
