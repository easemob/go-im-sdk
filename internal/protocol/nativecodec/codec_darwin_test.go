//go:build nativecodecdev && darwin && cgo

package nativecodec

import (
	"bytes"
	"math"
	"testing"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

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
