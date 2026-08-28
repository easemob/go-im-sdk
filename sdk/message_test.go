package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

type messageTestCodec struct {
	encoded         internalprotocol.MessageBody
	decoded         *internalprotocol.MessageBody
	encodeSyncCalls atomic.Int32
	encodeBodyCalls atomic.Int32
}

func (c *messageTestCodec) EncodeProvision(internalprotocol.ProvisionRequest) ([]byte, error) {
	return nil, nil
}
func (c *messageTestCodec) EncodeUnread() ([]byte, error) { return nil, nil }
func (c *messageTestCodec) EncodeSync(internalprotocol.SyncRequest) ([]byte, error) {
	c.encodeSyncCalls.Add(1)
	return nil, nil
}
func (c *messageTestCodec) EncodeLogout(internalprotocol.LogoutRequest) ([]byte, error) {
	return nil, nil
}
func (c *messageTestCodec) DecodeFrame([]byte) (*internalprotocol.Frame, error) { return nil, nil }
func (c *messageTestCodec) EncodeMessageBody(body internalprotocol.MessageBody) ([]byte, error) {
	c.encodeBodyCalls.Add(1)
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
			req: SendRequest{ClientMessageID: 43, To: "group-1", IsGroup: true, DirectedUsers: []string{"bob", "cara"}, Body: MessageBody{
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

type encodeErrorCodec struct {
	messageTestCodec
	err error
}

func (c *encodeErrorCodec) EncodeMessageBody(internalprotocol.MessageBody) ([]byte, error) {
	return nil, c.err
}

func TestBuildOutgoingMetaWrapsEncodeErrors(t *testing.T) {
	req := SendRequest{ClientMessageID: 1, To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"}}
	t.Run("limit", func(t *testing.T) {
		_, err := buildOutgoingMeta(&encodeErrorCodec{err: fmt.Errorf("native: %w", internalprotocol.ErrLimitExceeded)}, "org#app", "alice", "easemob.com", "go", req)
		if errorCode(err) != ErrProtocolLimit || !errors.Is(err, internalprotocol.ErrLimitExceeded) {
			t.Fatalf("err=%v code=%s", err, errorCode(err))
		}
	})
	t.Run("invalid", func(t *testing.T) {
		_, err := buildOutgoingMeta(&encodeErrorCodec{err: errors.New("native codec error 1")}, "org#app", "alice", "easemob.com", "go", req)
		if errorCode(err) != ErrProtocol {
			t.Fatalf("err=%v code=%s", err, errorCode(err))
		}
	})
}

func TestBuildOutgoingMetaValidation(t *testing.T) {
	codec := &messageTestCodec{}
	if _, err := buildOutgoingMeta(codec, "a", "u", "d", "r", SendRequest{
		ClientMessageID: 1, To: "g", DirectedUsers: []string{"u"}, Body: MessageBody{Type: MessageBodyText},
	}); err == nil {
		t.Fatal("expected directed non-group error")
	}
	if _, err := buildOutgoingMeta(codec, "a", "u", "d", "r", SendRequest{
		ClientMessageID: 1, To: "u", Body: MessageBody{Type: MessageBodyCustom, CustomExts: map[string]KeyValue{
			"n": {Type: KeyValueInt, Value: int64(1)},
		}},
	}); err == nil {
		t.Fatal("expected non-string outgoing KeyValue error")
	}
}

func TestBuildOutgoingMetaEncodesTypedMessageExtInStableOrder(t *testing.T) {
	codec := &messageTestCodec{}
	req := SendRequest{
		ClientMessageID: 1,
		To:              "bob",
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
				ClientMessageID: 1, To: "bob", Ext: ext, Body: MessageBody{Type: MessageBodyText, Text: "hello"},
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
				ClientMessageID: 1, To: "bob", Ext: map[string]KeyValue{"bad": value}, Body: MessageBody{Type: MessageBodyText},
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

func TestBuildOutgoingMetaRequiresNonZeroClientMessageID(t *testing.T) {
	codec := &messageTestCodec{}
	_, err := buildOutgoingMeta(codec, "org#app", "alice", "easemob.com", "resource", SendRequest{
		To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"},
	})
	if err == nil {
		t.Fatal("expected zero ClientMessageID to be rejected")
	}
	if codec.encodeBodyCalls.Load() != 0 {
		t.Fatal("zero ClientMessageID reached message body encoding")
	}
}

func TestClientMessageIDCrossesOldUint32Boundary(t *testing.T) {
	c := &Client{}
	c.idCounter.Store(uint64(math.MaxUint32) - 1)
	first, err := c.nextMessageID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.nextMessageID()
	if err != nil {
		t.Fatal(err)
	}
	if first != math.MaxUint32 || second != uint64(math.MaxUint32)+1 {
		t.Fatalf("ids=(%d,%d), want (%d,%d)", first, second, uint64(math.MaxUint32), uint64(math.MaxUint32)+1)
	}
}

func TestClientMessageIDInitialCounterLayout(t *testing.T) {
	initial := initialMessageIDCounter([4]byte{0xff, 0xff, 0xff, 0xff})
	if initial>>63 != 0 {
		t.Fatalf("highest bit set in initial counter %#x", initial)
	}
	if initial&((uint64(1)<<31)-1) != 0 {
		t.Fatalf("low 31 bits not clear in initial counter %#x", initial)
	}
	c := &Client{}
	c.idCounter.Store(initial)
	id, err := c.nextMessageID()
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 || id != initial+1 {
		t.Fatalf("first id=%d initial=%d", id, initial)
	}
}

func TestClientMessageIDExhaustionIsStable(t *testing.T) {
	c := &Client{}
	c.idCounter.Store(math.MaxUint64 - 1)
	last, err := c.nextMessageID()
	if err != nil || last != math.MaxUint64 {
		t.Fatalf("last id=%d err=%v", last, err)
	}
	for i := 0; i < 3; i++ {
		id, err := c.nextMessageID()
		if id != 0 || errorCode(err) != ErrMessageIDExhausted {
			t.Fatalf("attempt %d: id=%d err=%v", i, id, err)
		}
	}
	if got := c.idCounter.Load(); got != math.MaxUint64 {
		t.Fatalf("counter wrapped to %d", got)
	}
}

func TestConcurrentClientMessageIDsAreUnique(t *testing.T) {
	const goroutines = 16
	const perGoroutine = 1_000
	c := &Client{}
	ids := make(chan uint64, goroutines*perGoroutine)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				id, err := c.nextMessageID()
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[uint64]struct{}, goroutines*perGoroutine)
	for id := range ids {
		if id == 0 {
			t.Fatal("generated zero ID")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("got %d IDs, want %d", len(seen), goroutines*perGoroutine)
	}
}

func TestSendExhaustedAutomaticIDHasNoSideEffects(t *testing.T) {
	codec := &messageTestCodec{}
	run := &connectionRun{pending: make(map[uint64]chan ackResult), done: make(chan struct{})}
	c := &Client{state: LoginStateLoggedIn, connState: ConnStateConnected, run: run, codec: codec}
	run.client = c
	c.idCounter.Store(math.MaxUint64)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result, err := c.Send(ctx, SendRequest{
		To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"},
	})
	if result != nil || errorCode(err) != ErrMessageIDExhausted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if codec.encodeBodyCalls.Load() != 0 || codec.encodeSyncCalls.Load() != 0 {
		t.Fatalf("encoding side effects: body=%d sync=%d", codec.encodeBodyCalls.Load(), codec.encodeSyncCalls.Load())
	}
	run.pendingMu.Lock()
	pending := len(run.pending)
	run.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("registered %d pending sends", pending)
	}
}

func TestExplicitClientMessageIDWorksAfterAutomaticExhaustion(t *testing.T) {
	const explicitID = uint64(42)
	codec := &messageTestCodec{}
	run := &connectionRun{
		pending: make(map[uint64]chan ackResult), writes: make(chan writeRequest, 1), done: make(chan struct{}),
	}
	c := &Client{
		cfg:   Config{AppKey: "org#app", Domain: "easemob.com", Resource: "resource"},
		state: LoginStateLoggedIn, connState: ConnStateConnected, run: run, codec: codec, userID: "alice",
	}
	run.client = c
	c.idCounter.Store(math.MaxUint64)
	go func() {
		req := <-run.writes
		req.done <- nil
		run.completeACK(&internalprotocol.Sync{MetaID: explicitID, ServerID: 99, Status: &internalprotocol.Status{Code: internalprotocol.StatusOK}})
	}()

	result, err := c.Send(context.Background(), SendRequest{
		ClientMessageID: explicitID, To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.ClientMessageID != explicitID || result.MessageID != 99 {
		t.Fatalf("result=%#v", result)
	}
	if got := c.idCounter.Load(); got != math.MaxUint64 {
		t.Fatalf("explicit ID changed automatic counter to %d", got)
	}
}
