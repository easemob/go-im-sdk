package sdk

import (
	"encoding/json"
	"math"
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

func TestMessageToJSONEmitsProtocolDefaults(t *testing.T) {
	msg := &Message{From: "alice", To: "bob"}
	raw, err := msg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"id", "from", "to", "timestamp", "ns", "routetype", "ext", "meta",
		"directed_users", "expire_time", "local_timestamp", "env", "payload",
	} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("missing %s in %s", key, raw)
		}
	}
	var from protocolJIDJSON
	if err := json.Unmarshal(doc["from"], &from); err != nil {
		t.Fatal(err)
	}
	if from.Name != "alice" || from.AppKey != "" || from.Domain != "" || from.ClientResource != "" {
		t.Fatalf("from=%+v", from)
	}
	if string(doc["meta"]) != "{}" {
		t.Fatalf("meta=%s", doc["meta"])
	}
	if string(doc["directed_users"]) != "[]" || string(doc["ext"]) != "[]" {
		t.Fatalf("empty collections=%s", raw)
	}
}

func TestMessageToJSONFromWireKeepsAllDecodedFields(t *testing.T) {
	codec := &messageTestCodec{decoded: &internalprotocol.MessageBody{
		Kind: internalprotocol.MessageGroupChat,
		From: internalprotocol.JID{AppKey: "o#a", Name: "alice", Domain: "easemob.com", ClientResource: "go"},
		To:   internalprotocol.JID{AppKey: "o#a", Name: "group", Domain: "conference.easemob.com"},
		Ext:  []internalprotocol.KeyValue{{Key: "trace", Kind: internalprotocol.KeyValueString, String: "t1"}},
		Contents: []internalprotocol.Content{
			{Kind: internalprotocol.ContentText, Text: "hello"},
			{Kind: internalprotocol.ContentCustom, Event: "ping", CustomExts: []internalprotocol.KeyValue{{Key: "k", Kind: internalprotocol.KeyValueInt, Int64: 2}}},
		},
	}}
	msg, err := parseIncomingMessage(codec, internalprotocol.Meta{
		ID: 99, Timestamp: 1234,
		From:          internalprotocol.JID{AppKey: "o#a", Name: "alice", Domain: "easemob.com"},
		To:            internalprotocol.JID{AppKey: "o#a", Name: "group", Domain: "conference.easemob.com"},
		Namespace:     internalprotocol.NamespaceChat,
		Route:         internalprotocol.RouteDirect,
		DirectedUsers: []string{"bob"},
		Attributes:    []byte(`{"is_online":0}`),
		ExpireTime:    7,
		Environment:   "prod",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := msg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var snap messageProtocol
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.ID != 99 || snap.Timestamp != 1234 || snap.NS != 1 || snap.RouteType != 2 {
		t.Fatalf("meta header=%+v", snap)
	}
	if snap.From.Name != "alice" || snap.To.Domain != "conference.easemob.com" {
		t.Fatalf("jids from=%+v to=%+v", snap.From, snap.To)
	}
	if len(snap.DirectedUsers) != 1 || snap.DirectedUsers[0] != "bob" {
		t.Fatalf("directed=%v", snap.DirectedUsers)
	}
	if snap.ExpireTime != 7 || snap.Env != "prod" || string(snap.Meta) != `{"is_online":0}` {
		t.Fatalf("envelope meta=%s expire=%d env=%q", snap.Meta, snap.ExpireTime, snap.Env)
	}
	if snap.Payload.Type != 2 || len(snap.Payload.Bodies) != 2 {
		t.Fatalf("payload=%+v", snap.Payload)
	}
	if snap.Payload.Bodies[0].Text != "hello" || snap.Payload.Bodies[1].CustomEvent != "ping" {
		t.Fatalf("bodies=%+v", snap.Payload.Bodies)
	}
	if len(snap.Payload.Ext) != 1 || snap.Payload.Ext[0].StringValue != "t1" || snap.Payload.Ext[0].Type != 7 {
		t.Fatalf("payload ext=%+v", snap.Payload.Ext)
	}
	// Public view stays trimmed to the first body.
	if msg.Body == nil || msg.Body.Type != MessageBodyText || msg.From != "alice" {
		t.Fatalf("public message=%+v body=%+v", msg, msg.Body)
	}
}

func TestNewOutgoingMessageToJSONMatchesSendShape(t *testing.T) {
	msg, err := NewOutgoingMessage("o#a", "lxm", "easemob.com", SendRequest{
		ClientMessageID: 42,
		To:              "room1",
		IsGroup:         true,
		DirectedUsers:   []string{"lxm2"},
		Ext:             map[string]KeyValue{"trace_id": {Type: KeyValueString, Value: "out"}},
		Body:            MessageBody{Type: MessageBodyText, Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := msg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var snap messageProtocol
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.ID != 42 || snap.RouteType != 2 || snap.NS != 1 {
		t.Fatalf("header=%+v", snap)
	}
	if snap.To.AppKey != "o#a" || snap.To.Name != "room1" || snap.To.Domain != "conference.easemob.com" {
		t.Fatalf("to=%+v", snap.To)
	}
	if snap.Payload.Type != 2 || snap.Payload.From.Name != "lxm" || snap.Payload.Bodies[0].Text != "hello" {
		t.Fatalf("payload=%+v", snap.Payload)
	}
	if len(snap.DirectedUsers) != 1 || snap.DirectedUsers[0] != "lxm2" {
		t.Fatalf("directed=%v", snap.DirectedUsers)
	}
}

func TestMessageToJSONUintUsesStringValue(t *testing.T) {
	codec := &messageTestCodec{decoded: &internalprotocol.MessageBody{
		Kind:     internalprotocol.MessageChat,
		Ext:      []internalprotocol.KeyValue{{Key: "n", Kind: internalprotocol.KeyValueUint, Uint64: math.MaxUint64}},
		Contents: []internalprotocol.Content{{Kind: internalprotocol.ContentText, Text: "x"}},
	}}
	msg, err := parseIncomingMessage(codec, internalprotocol.Meta{Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := msg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var snap messageProtocol
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Payload.Ext) != 1 || snap.Payload.Ext[0].StringValue != "18446744073709551615" {
		t.Fatalf("uint ext=%+v", snap.Payload.Ext)
	}
}

func TestMessageToJSONDoesNotChangePublicMarshal(t *testing.T) {
	msg := &Message{From: "alice", To: "bob"}
	public, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(public, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["routetype"]; ok {
		t.Fatalf("public marshal leaked protocol field: %s", public)
	}
	if _, ok := fields["online_state"]; ok {
		t.Fatalf("unknown online_state should stay omitted: %s", public)
	}
}
