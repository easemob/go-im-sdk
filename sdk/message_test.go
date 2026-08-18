package sdk

import (
	"encoding/json"
	"math"
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

type messageTestCodec struct {
	encoded internalprotocol.MessageBody
	decoded *internalprotocol.MessageBody
}

func (c *messageTestCodec) EncodeProvision(internalprotocol.ProvisionRequest) ([]byte, error) {
	return nil, nil
}
func (c *messageTestCodec) EncodeUnread() ([]byte, error) { return nil, nil }
func (c *messageTestCodec) EncodeSync(internalprotocol.SyncRequest) ([]byte, error) {
	return nil, nil
}
func (c *messageTestCodec) EncodeLogout(internalprotocol.LogoutRequest) ([]byte, error) {
	return nil, nil
}
func (c *messageTestCodec) DecodeFrame([]byte) (*internalprotocol.Frame, error) { return nil, nil }
func (c *messageTestCodec) EncodeMessageBody(body internalprotocol.MessageBody) ([]byte, error) {
	c.encoded = body
	return []byte("test-payload"), nil
}
func (c *messageTestCodec) DecodeMessageBody([]byte) (*internalprotocol.MessageBody, error) {
	return c.decoded, nil
}
func (c *messageTestCodec) DecodeStatistic([]byte) (*internalprotocol.Statistic, error) {
	return nil, nil
}

var _ internalprotocol.Codec = (*messageTestCodec)(nil)

func TestBuildOutgoingMeta(t *testing.T) {
	tests := []struct {
		name       string
		req        SendRequest
		wantKind   internalprotocol.MessageKind
		wantRoute  internalprotocol.RouteType
		wantDomain string
	}{
		{
			name: "chat",
			req: SendRequest{ClientMessageID: 42, To: "bob", Body: MessageBody{
				Type: MessageBodyText, Text: "hello",
			}},
			wantKind: internalprotocol.MessageChat, wantRoute: internalprotocol.RouteAll,
		},
		{
			name: "group directed",
			req: SendRequest{To: "group-1", IsGroup: true, DirectedUsers: []string{"bob", "cara"}, Body: MessageBody{
				Type: MessageBodyCustom, Event: "alert",
				CustomExts: map[string]KeyValue{"color": {Type: KeyValueString, Value: "red"}},
			}},
			wantKind: internalprotocol.MessageGroupChat, wantRoute: internalprotocol.RouteDirect,
			wantDomain: "conference.easemob.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := &messageTestCodec{}
			meta, err := buildOutgoingMeta(codec, "org#app", "alice", "easemob.com", "go", tt.req)
			if err != nil {
				t.Fatal(err)
			}
			if tt.req.ClientMessageID != 0 && meta.ID != tt.req.ClientMessageID {
				t.Fatalf("id=%d", meta.ID)
			}
			if meta.ID == 0 {
				t.Fatal("generated id is zero")
			}
			if meta.Namespace != internalprotocol.NamespaceChat || meta.Route != tt.wantRoute {
				t.Fatalf("routing: %+v", meta)
			}
			if meta.To.Name != tt.req.To || meta.To.AppKey != "org#app" || meta.To.Domain != tt.wantDomain {
				t.Fatalf("meta.to=%+v want domain=%q", meta.To, tt.wantDomain)
			}
			if got := meta.DirectedUsers; len(got) != len(tt.req.DirectedUsers) {
				t.Fatalf("directed users=%v", got)
			}
			if codec.encoded.Kind != tt.wantKind || codec.encoded.To.Name != tt.req.To {
				t.Fatalf("body=%+v", codec.encoded)
			}
			if codec.encoded.From.Name != "alice" {
				t.Fatalf("body.from=%+v", codec.encoded.From)
			}
		})
	}
}

func TestBuildOutgoingMetaValidation(t *testing.T) {
	codec := &messageTestCodec{}
	if _, err := buildOutgoingMeta(codec, "a", "u", "d", "r", SendRequest{
		To: "g", DirectedUsers: []string{"u"}, Body: MessageBody{Type: MessageBodyText},
	}); err == nil {
		t.Fatal("expected directed non-group error")
	}
	if _, err := buildOutgoingMeta(codec, "a", "u", "d", "r", SendRequest{
		To: "u", Body: MessageBody{Type: MessageBodyCustom, CustomExts: map[string]KeyValue{
			"n": {Type: KeyValueInt, Value: int64(1)},
		}},
	}); err == nil {
		t.Fatal("expected non-string outgoing KeyValue error")
	}
}

func TestBuildOutgoingMetaEncodesTypedMessageExtInStableOrder(t *testing.T) {
	codec := &messageTestCodec{}
	req := SendRequest{
		To: "bob",
		Ext: map[string]KeyValue{
			"h_json":   {Type: KeyValueJSONString, Value: `{"order_id":"123"}`},
			"c_uint":   {Type: KeyValueUint, Value: uint64(math.MaxUint64)},
			"a_bool":   {Type: KeyValueBool, Value: true},
			"f_double": {Type: KeyValueDouble, Value: 2.5},
			"e_float":  {Type: KeyValueFloat, Value: float32(1.25)},
			"d_long":   {Type: KeyValueLong, Value: int64(math.MinInt64)},
			"b_int":    {Type: KeyValueInt, Value: int(-7)},
			"g_string": {Type: KeyValueString, Value: "value"},
		},
		Body: MessageBody{Type: MessageBodyText, Text: "hello"},
	}
	if _, err := buildOutgoingMeta(codec, "org#app", "alice", "easemob.com", "resource", req); err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"a_bool", "b_int", "c_uint", "d_long", "e_float", "f_double", "g_string", "h_json"}
	if len(codec.encoded.Ext) != len(wantKeys) {
		t.Fatalf("ext=%#v", codec.encoded.Ext)
	}
	for i, key := range wantKeys {
		if codec.encoded.Ext[i].Key != key {
			t.Fatalf("ext order=%#v", codec.encoded.Ext)
		}
	}
	if got := codec.encoded.Ext; got[0].Kind != internalprotocol.KeyValueBool || !got[0].Bool ||
		got[1].Kind != internalprotocol.KeyValueInt || got[1].Int64 != -7 ||
		got[2].Kind != internalprotocol.KeyValueUint || got[2].Uint64 != math.MaxUint64 ||
		got[3].Kind != internalprotocol.KeyValueLong || got[3].Int64 != math.MinInt64 ||
		got[4].Kind != internalprotocol.KeyValueFloat || got[4].Float != 1.25 ||
		got[5].Kind != internalprotocol.KeyValueDouble || got[5].Double != 2.5 ||
		got[6].Kind != internalprotocol.KeyValueString || got[6].String != "value" ||
		got[7].Kind != internalprotocol.KeyValueJSONString || got[7].String != `{"order_id":"123"}` {
		t.Fatalf("typed ext=%#v", got)
	}
}

func TestBuildOutgoingMetaEmptyExtPreservesWireShape(t *testing.T) {
	for name, ext := range map[string]map[string]KeyValue{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			codec := &messageTestCodec{}
			_, err := buildOutgoingMeta(codec, "org#app", "alice", "easemob.com", "resource", SendRequest{
				To: "bob", Ext: ext, Body: MessageBody{Type: MessageBodyText, Text: "hello"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if codec.encoded.Ext != nil {
				t.Fatalf("empty ext encoded as %#v", codec.encoded.Ext)
			}
		})
	}
}

func TestBuildOutgoingMetaRejectsInvalidMessageExt(t *testing.T) {
	for name, value := range map[string]KeyValue{
		"bool":    {Type: KeyValueBool, Value: "true"},
		"int":     {Type: KeyValueInt, Value: 1.5},
		"uint":    {Type: KeyValueUint, Value: -1},
		"float":   {Type: KeyValueFloat, Value: 1},
		"string":  {Type: KeyValueString, Value: 1},
		"unknown": {Type: KeyValueType("future"), Value: "x"},
	} {
		t.Run(name, func(t *testing.T) {
			codec := &messageTestCodec{}
			_, err := buildOutgoingMeta(codec, "org#app", "alice", "easemob.com", "resource", SendRequest{
				To: "bob", Ext: map[string]KeyValue{"bad": value}, Body: MessageBody{Type: MessageBodyText},
			})
			if err == nil {
				t.Fatal("expected message ext validation error")
			}
		})
	}
}

func TestParseIncomingMessagePreservesTypedKeyValues(t *testing.T) {
	values := []internalprotocol.KeyValue{
		{Key: "bool", Kind: internalprotocol.KeyValueBool, Bool: true},
		{Key: "int", Kind: internalprotocol.KeyValueInt, Int64: -7},
		{Key: "uint", Kind: internalprotocol.KeyValueUint, Uint64: math.MaxUint64},
		{Key: "long", Kind: internalprotocol.KeyValueLong, Int64: math.MaxInt64},
		{Key: "float", Kind: internalprotocol.KeyValueFloat, Float: 1.25},
		{Key: "double", Kind: internalprotocol.KeyValueDouble, Double: 2.5},
		{Key: "string", Kind: internalprotocol.KeyValueString, String: "value"},
		{Key: "json", Kind: internalprotocol.KeyValueJSONString, String: `{"a":1}`},
	}
	codec := &messageTestCodec{decoded: &internalprotocol.MessageBody{
		Kind: internalprotocol.MessageGroupChat,
		From: internalprotocol.JID{Name: "alice"}, To: internalprotocol.JID{Name: "group"}, Ext: values,
		Contents: []internalprotocol.Content{{Kind: internalprotocol.ContentCommand, Action: "run"}},
	}}
	got, err := parseIncomingMessage(codec, internalprotocol.Meta{ID: 99, Timestamp: 1234, Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsGroup || got.From != "alice" || got.To != "group" || got.MetaID != 99 {
		t.Fatalf("message=%+v", got)
	}
	if got.Ext["bool"].Value != true || got.Ext["uint"].Value != uint64(math.MaxUint64) || got.Ext["long"].Value != int64(math.MaxInt64) {
		t.Fatalf("ext=%#v", got.Ext)
	}
	if got.Ext["int"].Value != int64(-7) || got.Ext["float"].Value != float64(float32(1.25)) ||
		got.Ext["double"].Value != 2.5 || got.Ext["string"].Value != "value" || got.Ext["json"].Value != `{"a":1}` {
		t.Fatalf("typed ext=%#v", got.Ext)
	}
	if got.Body == nil || got.Body.Type != MessageBodyCommand || got.Body.Action != "run" {
		t.Fatalf("body=%+v", got.Body)
	}
}

func TestKeyValueJSON64BitIntegersAreStrings(t *testing.T) {
	original := map[string]KeyValue{
		"long": {Type: KeyValueLong, Value: int64(math.MinInt64)},
		"uint": {Type: KeyValueUint, Value: uint64(math.MaxUint64)},
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"long":{"type":"llint","value":"-9223372036854775808"},"uint":{"type":"uint","value":"18446744073709551615"}}`
	if string(data) != want {
		t.Fatalf("json=%s", data)
	}
	var decoded map[string]KeyValue
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["long"].Value != int64(math.MinInt64) || decoded["uint"].Value != uint64(math.MaxUint64) {
		t.Fatalf("decoded=%#v", decoded)
	}
}

func TestUnknownBodyDoesNotFailMessage(t *testing.T) {
	codec := &messageTestCodec{decoded: &internalprotocol.MessageBody{
		Kind:     internalprotocol.MessageChat,
		Contents: []internalprotocol.Content{{Kind: internalprotocol.ContentKind(99), RawPayload: []byte("future")}},
	}}
	got, err := parseIncomingMessage(codec, internalprotocol.Meta{Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if got.Body == nil || got.Body.Type != MessageBodyUnknown || got.Body.RawType != 99 || len(got.Body.RawPayload) == 0 {
		t.Fatalf("body=%+v", got.Body)
	}
}

func TestGeneratedClientMessageIDsAreUnique(t *testing.T) {
	seen := make(map[uint64]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := nextClientMessageID()
		if id == 0 {
			t.Fatal("zero id")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}
}
