package sdk

import (
	"encoding/json"
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

func TestParseOnlineStateKeepsUnknownDistinctFromOffline(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       MessageOnlineState
	}{
		{name: "absent blob", attributes: "", want: MessageOnlineStateUnknown},
		{name: "empty object", attributes: `{}`, want: MessageOnlineStateUnknown},
		{name: "zero is offline", attributes: `{"is_online":0}`, want: MessageOnlineStateOffline},
		{name: "one is online", attributes: `{"is_online":1}`, want: MessageOnlineStateOnline},
		{name: "any non-zero is online", attributes: `{"is_online":2}`, want: MessageOnlineStateOnline},
		{name: "bool true", attributes: `{"is_online":true}`, want: MessageOnlineStateOnline},
		{name: "bool false", attributes: `{"is_online":false}`, want: MessageOnlineStateOffline},
		{name: "other keys ignored", attributes: `{"other":1}`, want: MessageOnlineStateUnknown},
		{name: "null value", attributes: `{"is_online":null}`, want: MessageOnlineStateUnknown},
		{name: "string value", attributes: `{"is_online":"1"}`, want: MessageOnlineStateUnknown},
		{name: "malformed json", attributes: `{"is_online":`, want: MessageOnlineStateUnknown},
		{name: "not an object", attributes: `[1,2]`, want: MessageOnlineStateUnknown},
		// The peer SDKs persist the decoded value under "online_state"; that
		// spelling must not be mistaken for the wire key.
		{name: "persistence spelling is not the wire key", attributes: `{"online_state":false}`, want: MessageOnlineStateUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseOnlineState([]byte(tt.attributes)); got != tt.want {
				t.Fatalf("parseOnlineState(%q) = %q, want %q", tt.attributes, got, tt.want)
			}
		})
	}
}

func TestParseIncomingMessageSurfacesOnlineStateWithoutTouchingExt(t *testing.T) {
	codec := &messageTestCodec{decoded: &internalprotocol.MessageBody{
		Kind: internalprotocol.MessageChat,
		From: internalprotocol.JID{Name: "alice"}, To: internalprotocol.JID{Name: "bob"},
		Ext:      []internalprotocol.KeyValue{{Key: "app", Kind: internalprotocol.KeyValueString, String: "value"}},
		Contents: []internalprotocol.Content{{Kind: internalprotocol.ContentText, Text: "hello"}},
	}}
	meta := internalprotocol.Meta{ID: 7, Timestamp: 99, Payload: []byte("payload"), Attributes: []byte(`{"is_online":0}`)}
	got, err := parseIncomingMessage(codec, meta)
	if err != nil {
		t.Fatal(err)
	}
	if got.OnlineState != MessageOnlineStateOffline {
		t.Fatalf("OnlineState=%q, want %q", got.OnlineState, MessageOnlineStateOffline)
	}
	// The attribute blob is SDK-facing delivery metadata and must never leak
	// into the user's own extension map.
	if len(got.Ext) != 1 {
		t.Fatalf("Ext=%v, want only the application key", got.Ext)
	}
	if _, exists := got.Ext[messageOnlineStateKey]; exists {
		t.Fatalf("attribute blob leaked into Ext: %v", got.Ext)
	}
	if got.Body == nil || got.Body.Text != "hello" {
		t.Fatalf("body=%+v", got.Body)
	}
}

func TestParseIncomingMessageWithoutAttributesReportsUnknown(t *testing.T) {
	codec := &messageTestCodec{decoded: &internalprotocol.MessageBody{
		Kind: internalprotocol.MessageChat,
		From: internalprotocol.JID{Name: "alice"}, To: internalprotocol.JID{Name: "bob"},
		Contents: []internalprotocol.Content{{Kind: internalprotocol.ContentText, Text: "hello"}},
	}}
	got, err := parseIncomingMessage(codec, internalprotocol.Meta{Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if got.OnlineState != MessageOnlineStateUnknown {
		t.Fatalf("OnlineState=%q, want unknown", got.OnlineState)
	}
}

func TestMessageOnlineStateJSONOmitsUnknown(t *testing.T) {
	encoded, err := json.Marshal(&Message{From: "alice", To: "bob"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["online_state"]; exists {
		t.Fatalf("unknown state must not be serialized: %s", encoded)
	}

	encoded, err = json.Marshal(&Message{From: "alice", To: "bob", OnlineState: MessageOnlineStateOffline})
	if err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.OnlineState != MessageOnlineStateOffline {
		t.Fatalf("round-tripped OnlineState=%q, want offline", decoded.OnlineState)
	}
}
