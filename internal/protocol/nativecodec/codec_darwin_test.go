//go:build nativecodecdev && darwin && cgo

package nativecodec

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

func TestNativeCodecLimitConstantsAndTrackers(t *testing.T) {
	if got := nativeCodecMaxInputBytes(); got != protocol.MaxCodecInputBytes {
		t.Fatalf("native max input bytes = %d, Go limit = %d", got, protocol.MaxCodecInputBytes)
	}

	tracker := &decodeTracker{}
	if err := tracker.addPayload(protocol.MaxSyncPayloadBytes); err != nil {
		t.Fatal(err)
	}
	if err := tracker.addPayload(1); !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("payload overflow error = %v, want ErrLimitExceeded", err)
	}
	if err := tracker.addDirectedUsers(protocol.MaxSyncDirectedUsers); err != nil {
		t.Fatal(err)
	}
	if err := tracker.addDirectedUsers(1); !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("directed user overflow error = %v, want ErrLimitExceeded", err)
	}
}

func TestNativeCodecBoundedCStringCopy(t *testing.T) {
	exact := append(bytes.Repeat([]byte{'x'}, protocol.MaxSyncStringBytes), 0)
	tracker := &decodeTracker{}
	got, err := tracker.readStringPointer(unsafe.Pointer(&exact[0]))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != protocol.MaxSyncStringBytes || tracker.stringBytes != protocol.MaxSyncStringBytes {
		t.Fatalf("copied length/tracked = %d/%d", len(got), tracker.stringBytes)
	}

	over := append(bytes.Repeat([]byte{'x'}, protocol.MaxSyncStringBytes+1), 0)
	tracker = &decodeTracker{}
	got, err = tracker.readStringPointer(unsafe.Pointer(&over[0]))
	if got != "" || !errors.Is(err, protocol.ErrLimitExceeded) || tracker.stringBytes != 0 {
		t.Fatalf("over-limit read = (%d bytes, %v, tracked=%d), want no copy ErrLimitExceeded", len(got), err, tracker.stringBytes)
	}

	half := append([]byte(strings.Repeat("y", protocol.MaxSyncStringBytes/2)), 0)
	tracker = &decodeTracker{}
	for i := 0; i < 2; i++ {
		if _, err = tracker.readStringPointer(unsafe.Pointer(&half[0])); err != nil {
			t.Fatal(err)
		}
	}
	one := []byte{'z', 0}
	if _, err = tracker.readStringPointer(unsafe.Pointer(&one[0])); !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("shared string overflow error = %v, want ErrLimitExceeded", err)
	}
}

func TestNativeCodecJIDBoundsBeforeCopy(t *testing.T) {
	component := append(bytes.Repeat([]byte{'j'}, protocol.MaxJIDComponentBytes), 0)
	p := unsafe.Pointer(&component[0])
	tracker := &decodeTracker{}
	jid, err := readJIDPointers([4]unsafe.Pointer{p, p, p, p}, tracker)
	if err != nil {
		t.Fatal(err)
	}
	if len(jid.AppKey)+len(jid.Name)+len(jid.Domain)+len(jid.ClientResource) != protocol.MaxJIDBytes {
		t.Fatalf("JID copied length = %d, want %d", len(jid.AppKey)+len(jid.Name)+len(jid.Domain)+len(jid.ClientResource), protocol.MaxJIDBytes)
	}

	over := append(bytes.Repeat([]byte{'j'}, protocol.MaxJIDComponentBytes+1), 0)
	tracker = &decodeTracker{}
	jid, err = readJIDPointers([4]unsafe.Pointer{unsafe.Pointer(&over[0])}, tracker)
	if jid != (protocol.JID{}) || !errors.Is(err, protocol.ErrLimitExceeded) || tracker.stringBytes != 0 {
		t.Fatalf("over-limit JID = (%+v, %v, tracked=%d), want no copy ErrLimitExceeded", jid, err, tracker.stringBytes)
	}
}

func TestNativeCodecReadBytesReportsErrors(t *testing.T) {
	if got, err := readBytes(nil, 1); got != nil || err == nil || errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("nil readBytes = (%v, %v), want ordinary error", got, err)
	}
	if got, err := readBytes(nil, protocol.MaxCodecInputBytes+1); got != nil || !errors.Is(err, protocol.ErrLimitExceeded) {
		t.Fatalf("oversized readBytes = (%v, %v), want ErrLimitExceeded", got, err)
	}
	input := []byte{1, 2, 3}
	got, err := readBytes(unsafe.Pointer(&input[0]), uint64(len(input)))
	if err != nil || !bytes.Equal(got, input) {
		t.Fatalf("readBytes = (%v, %v), want %v", got, err, input)
	}
	input[0] = 9
	if got[0] != 1 {
		t.Fatal("readBytes did not return an owned copy")
	}
}

func TestNativeCodecSemanticRoundTrip(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	body := protocol.MessageBody{Kind: protocol.MessageGroupChat, From: protocol.JID{AppKey: "o#a", Name: "alice", Domain: "easemob.com", ClientResource: "go"}, To: protocol.JID{AppKey: "o#a", Name: "group", Domain: "easemob.com"}, Ext: []protocol.KeyValue{
		{Key: "bool", Kind: protocol.KeyValueBool, Bool: true},
		{Key: "int", Kind: protocol.KeyValueInt, Int64: -7},
		{Key: "uint", Kind: protocol.KeyValueUint, Uint64: math.MaxUint64},
		{Key: "long", Kind: protocol.KeyValueLong, Int64: math.MinInt64},
		{Key: "float", Kind: protocol.KeyValueFloat, Float: 1.25},
		{Key: "double", Kind: protocol.KeyValueDouble, Double: 2.5},
		{Key: "string", Kind: protocol.KeyValueString, String: "value"},
		{Key: "json", Kind: protocol.KeyValueJSONString, String: `{"a":1}`},
	}, Contents: []protocol.Content{
		{Kind: protocol.ContentCommand, Action: "run", Params: []protocol.KeyValue{{Key: "max", Kind: protocol.KeyValueLong, Int64: 1 << 60}, {Key: "json", Kind: protocol.KeyValueJSONString, String: `{"a":1}`}}},
		{Kind: protocol.ContentCustom, Event: "order-status", CustomExts: []protocol.KeyValue{{Key: "status", Kind: protocol.KeyValueString, String: "paid"}}},
	}}
	payload, err := c.EncodeMessageBody(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.DecodeMessageBody(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != body.Kind || decoded.From.Name != "alice" || decoded.Contents[0].Action != "run" || decoded.Contents[0].Params[0].Int64 != 1<<60 || decoded.Contents[1].Event != "order-status" || decoded.Contents[1].CustomExts[0].String != "paid" {
		t.Fatalf("decoded=%+v", decoded)
	}
	if len(decoded.Ext) != len(body.Ext) {
		t.Fatalf("decoded ext=%#v", decoded.Ext)
	}
	for i, want := range body.Ext {
		got := decoded.Ext[i]
		matches := got.Key == want.Key && got.Kind == want.Kind
		switch want.Kind {
		case protocol.KeyValueBool:
			matches = matches && got.Bool == want.Bool
		case protocol.KeyValueInt, protocol.KeyValueLong:
			matches = matches && got.Int64 == want.Int64
		case protocol.KeyValueUint:
			matches = matches && got.Uint64 == want.Uint64
		case protocol.KeyValueFloat:
			matches = matches && got.Float == want.Float
		case protocol.KeyValueDouble:
			matches = matches && got.Double == want.Double
		case protocol.KeyValueString, protocol.KeyValueJSONString:
			matches = matches && got.String == want.String
		}
		if !matches {
			t.Fatalf("decoded ext[%d]=%#v want %#v", i, got, want)
		}
	}
	frame, err := c.EncodeSync(protocol.SyncRequest{Meta: &protocol.Meta{ID: 42, From: body.From, To: body.To, Namespace: protocol.NamespaceChat, Route: protocol.RouteDirect, Payload: payload, DirectedUsers: []string{"bob"}}})
	if err != nil {
		t.Fatal(err)
	}
	// EncodeSync creates an uplink frame; DecodeFrame intentionally decodes downlink payloads only.
	if len(frame) == 0 {
		t.Fatal("empty sync frame")
	}
	provision, err := c.EncodeProvision(protocol.ProvisionRequest{User: body.From, SDKVersion: "4.0.0-go", Resource: "go", AuthToken: []byte(`{"token":"t"}`)})
	if err != nil || len(provision) == 0 {
		t.Fatalf("provision len=%d err=%v", len(provision), err)
	}
	envelope, err := scanEnvelope(provision)
	if err != nil {
		t.Fatal(err)
	}
	if version, ok := stringFieldForTest(envelope.payload, provisionActionVersionField); !ok || version != protocol.ActionVersion {
		t.Fatalf("provision action_version=%q present=%v, want %q", version, ok, protocol.ActionVersion)
	}
}

func TestNativeCodecEmptyMessageExtHasCompatibleWireEncoding(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	base := protocol.MessageBody{
		Kind: protocol.MessageChat, From: protocol.JID{Name: "alice"}, To: protocol.JID{Name: "bob"},
		Contents: []protocol.Content{{Kind: protocol.ContentText, Text: "hello"}},
	}
	nilPayload, err := c.EncodeMessageBody(base)
	if err != nil {
		t.Fatal(err)
	}
	base.Ext = []protocol.KeyValue{}
	emptyPayload, err := c.EncodeMessageBody(base)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(nilPayload, emptyPayload) {
		t.Fatalf("nil and empty ext changed wire encoding: nil=%x empty=%x", nilPayload, emptyPayload)
	}
	decoded, err := c.DecodeMessageBody(emptyPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Ext) != 0 {
		t.Fatalf("decoded ext=%#v", decoded.Ext)
	}
}

func TestMakeRequestDoesNotAllocateEmptyMessageExt(t *testing.T) {
	for name, ext := range map[string][]protocol.KeyValue{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			memory, err := makeRequest(protocol.MessageBody{Ext: ext})
			if err != nil {
				t.Fatal(err)
			}
			defer memory.free()
			if memory.req.extensions != nil || memory.req.extension_count != 0 {
				t.Fatalf("extensions=%p count=%d", memory.req.extensions, memory.req.extension_count)
			}
		})
	}
}
