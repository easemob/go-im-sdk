package sdk

import (
	"encoding/json"
	"math"
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/internal/protocol/gopb"
	"github.com/easemob/go-im-sdk/pb"
	"google.golang.org/protobuf/proto"
)

func TestBuildOutgoingMeta(t *testing.T) {
	tests := []struct {
		name       string
		req        SendRequest
		wantType   pb.MessageBody_Type
		wantRoute  pb.Meta_RouteType
		wantDomain string
	}{
		{"chat", SendRequest{ClientMessageID: 42, To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"}}, pb.MessageBody_CHAT, pb.Meta_ROUTE_ALL, ""},
		{"group directed", SendRequest{To: "group-1", IsGroup: true, DirectedUsers: []string{"bob", "cara"}, Body: MessageBody{Type: MessageBodyCustom, Event: "alert", CustomExts: map[string]KeyValue{"color": {Type: KeyValueString, Value: "red"}}}}, pb.MessageBody_GROUPCHAT, pb.Meta_ROUTE_DIRECT, "conference.easemob.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := gopb.New()
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
			if meta.Namespace != internalprotocol.NamespaceChat || int32(meta.Route) != int32(tt.wantRoute) {
				t.Fatalf("routing: %+v", meta)
			}
			// Meta.to：携带 appkey；单聊无 domain，群聊带 conference.<domain>。
			if meta.To.Name != tt.req.To || meta.To.AppKey != "org#app" || meta.To.Domain != tt.wantDomain {
				t.Fatalf("meta.to=%+v want domain=%q", meta.To, tt.wantDomain)
			}
			if got := meta.DirectedUsers; len(got) != len(tt.req.DirectedUsers) {
				t.Fatalf("directed users=%v", got)
			}
			var body pb.MessageBody
			if err := proto.Unmarshal(meta.Payload, &body); err != nil {
				t.Fatal(err)
			}
			if body.GetType() != tt.wantType || body.GetTo().GetName() != tt.req.To {
				t.Fatalf("body=%+v", &body)
			}
			if body.UserinfoUpdateTime != nil {
				t.Fatal("userinfo_update_time must not be sent")
			}
		})
	}
}

func TestBuildOutgoingMetaValidation(t *testing.T) {
	codec := gopb.New()
	if _, err := buildOutgoingMeta(codec, "a", "u", "d", "r", SendRequest{To: "g", DirectedUsers: []string{"u"}, Body: MessageBody{Type: MessageBodyText}}); err == nil {
		t.Fatal("expected directed non-group error")
	}
	if _, err := buildOutgoingMeta(codec, "a", "u", "d", "r", SendRequest{To: "u", Body: MessageBody{Type: MessageBodyCommand, Params: map[string]KeyValue{"n": {Type: KeyValueInt, Value: int64(1)}}}}); err == nil {
		t.Fatal("expected non-string outgoing KeyValue error")
	}
}

func TestParseIncomingMessagePreservesTypedKeyValues(t *testing.T) {
	command := pb.MessageBody_Content_COMMAND
	messageType := pb.MessageBody_GROUPCHAT
	values := []*pb.KeyValue{
		wireVarint("bool", pb.KeyValue_BOOL, 1),
		wireVarint("int", pb.KeyValue_INT, -7),
		wireVarint("uint", pb.KeyValue_UINT, -1),
		wireVarint("long", pb.KeyValue_LLINT, math.MaxInt64),
		wireFloat("float", 1.25),
		wireDouble("double", 2.5),
		wireString("string", pb.KeyValue_STRING, "value"),
		wireString("json", pb.KeyValue_JSON_STRING, `{"a":1}`),
	}
	body := &pb.MessageBody{Type: &messageType, From: &pb.JID{Name: proto.String("alice")}, To: &pb.JID{Name: proto.String("group")}, Contents: []*pb.MessageBody_Content{{Type: &command, Action: proto.String("run"), Params: values}}, Ext: values}
	payload, err := proto.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	meta := internalprotocol.Meta{ID: 99, Timestamp: 1234, Payload: payload}
	got, err := parseIncomingMessage(gopb.New(), meta)
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsGroup || got.From != "alice" || got.To != "group" || got.MetaID != 99 {
		t.Fatalf("message=%+v", got)
	}
	if got.Ext["bool"].Value != true || got.Ext["uint"].Value != uint64(math.MaxUint64) || got.Ext["long"].Value != int64(math.MaxInt64) {
		t.Fatalf("ext=%#v", got.Ext)
	}
	if got.Bodies[0].Type != MessageBodyCommand || got.Bodies[0].Action != "run" {
		t.Fatalf("body=%+v", got.Bodies[0])
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
	unknown := pb.MessageBody_Content_Type(99)
	messageType := pb.MessageBody_CHAT
	body := &pb.MessageBody{Type: &messageType, Contents: []*pb.MessageBody_Content{{Type: &unknown, Text: proto.String("future")}}}
	payload, _ := proto.Marshal(body)
	got, err := parseIncomingMessage(gopb.New(), internalprotocol.Meta{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Bodies) != 1 || got.Bodies[0].Type != MessageBodyUnknown || got.Bodies[0].RawType != 99 || len(got.Bodies[0].RawPayload) == 0 {
		t.Fatalf("body=%+v", got.Bodies)
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

func wireVarint(key string, typ pb.KeyValue_ValueType, value int64) *pb.KeyValue {
	return &pb.KeyValue{Key: proto.String(key), Type: &typ, Value: &pb.KeyValue_VarintValue{VarintValue: value}}
}
func wireFloat(key string, value float32) *pb.KeyValue {
	typ := pb.KeyValue_FLOAT
	return &pb.KeyValue{Key: proto.String(key), Type: &typ, Value: &pb.KeyValue_FloatValue{FloatValue: value}}
}
func wireDouble(key string, value float64) *pb.KeyValue {
	typ := pb.KeyValue_DOUBLE
	return &pb.KeyValue{Key: proto.String(key), Type: &typ, Value: &pb.KeyValue_DoubleValue{DoubleValue: value}}
}
func wireString(key string, typ pb.KeyValue_ValueType, value string) *pb.KeyValue {
	return &pb.KeyValue{Key: proto.String(key), Type: &typ, Value: &pb.KeyValue_StringValue{StringValue: value}}
}
