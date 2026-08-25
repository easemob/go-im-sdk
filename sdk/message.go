package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

// Message is the stable, JSON-serializable representation delivered to users.
type Message struct {
	From    string `json:"from"`
	To      string `json:"to"`
	IsGroup bool   `json:"is_group"`
	// MetaID is the authoritative server-assigned message ID for a received
	// message. For a message sent by this SDK, it equals SendResult.MessageID.
	MetaID    uint64              `json:"meta_id"`
	Timestamp uint64              `json:"timestamp"`
	Body      *MessageBody        `json:"body"`
	Ext       map[string]KeyValue `json:"ext,omitempty"`
	// OnlineState reports whether the server delivered this message while the
	// recipient was online. It is derived from the server-populated msync Meta
	// attribute blob and requires the feature to be enabled server-side, so it
	// is MessageOnlineStateUnknown whenever the server stays silent. Treat it
	// as a hint: it is not a delivery guarantee, and it is never inferred from
	// which pull the message arrived on.
	OnlineState MessageOnlineState `json:"online_state,omitempty"`
}

// MessageOnlineState distinguishes a real-time delivery from an offline one
// without collapsing the "server did not tell us" case into either answer.
type MessageOnlineState string

const (
	// MessageOnlineStateUnknown is the zero value: the server did not report
	// a state, so no claim is made either way.
	MessageOnlineStateUnknown MessageOnlineState = ""
	MessageOnlineStateOnline  MessageOnlineState = "online"
	MessageOnlineStateOffline MessageOnlineState = "offline"
)

// messageOnlineStateKey is the server's wire spelling inside the Meta
// attribute blob. Other Easemob SDKs persist the decoded value under the
// different name "online_state"; only the wire name is read here.
const messageOnlineStateKey = "is_online"

// parseOnlineState reads is_online from the Meta attribute blob. Every failure
// mode (absent blob, malformed JSON, absent or non-numeric key) yields Unknown
// rather than an error: the attribute is advisory, and a message must never be
// dropped because optional delivery metadata could not be understood.
func parseOnlineState(attributes []byte) MessageOnlineState {
	if len(attributes) == 0 {
		return MessageOnlineStateUnknown
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(attributes, &decoded); err != nil {
		return MessageOnlineStateUnknown
	}
	raw, ok := decoded[messageOnlineStateKey]
	if !ok {
		return MessageOnlineStateUnknown
	}
	// The server sends an integer, but accept a bool too so that a future
	// server-side type change degrades to a correct read instead of Unknown.
	// Both are decoded through pointers because unmarshalling a JSON null into
	// a bare int64 succeeds and leaves a zero behind, which would silently
	// report "offline" for a value the server never actually sent.
	var number *int64
	if err := json.Unmarshal(raw, &number); err == nil {
		if number == nil {
			return MessageOnlineStateUnknown
		}
		if *number == 0 {
			return MessageOnlineStateOffline
		}
		return MessageOnlineStateOnline
	}
	var flag *bool
	if err := json.Unmarshal(raw, &flag); err != nil || flag == nil {
		return MessageOnlineStateUnknown
	}
	if *flag {
		return MessageOnlineStateOnline
	}
	return MessageOnlineStateOffline
}

type MessageBodyType string

const (
	MessageBodyText    MessageBodyType = "text"
	MessageBodyCommand MessageBodyType = "command"
	MessageBodyCustom  MessageBodyType = "custom"
	MessageBodyUnknown MessageBodyType = "unknown"
)

type MessageBody struct {
	Type       MessageBodyType     `json:"type"`
	Text       string              `json:"text,omitempty"`
	Action     string              `json:"action,omitempty"`
	Event      string              `json:"event,omitempty"`
	CustomExts map[string]KeyValue `json:"custom_exts,omitempty"`
	RawType    int32               `json:"raw_type,omitempty"`
	RawPayload []byte              `json:"raw_payload,omitempty"`
}

type KeyValueType string

const (
	KeyValueBool       KeyValueType = "bool"
	KeyValueInt        KeyValueType = "int"
	KeyValueUint       KeyValueType = "uint"
	KeyValueLong       KeyValueType = "llint"
	KeyValueFloat      KeyValueType = "float"
	KeyValueDouble     KeyValueType = "double"
	KeyValueString     KeyValueType = "string"
	KeyValueJSONString KeyValueType = "json_string"
)

type KeyValue struct {
	Type  KeyValueType `json:"type"`
	Value any          `json:"value"`
}

// MarshalJSON writes all 64-bit integer variants as decimal strings so that a
// JavaScript consumer cannot silently round values above 2^53.
func (v KeyValue) MarshalJSON() ([]byte, error) {
	var value any = v.Value
	switch v.Type {
	case KeyValueLong:
		n, ok := asInt64(v.Value)
		if !ok {
			return nil, fmt.Errorf("%s KeyValue requires int64", v.Type)
		}
		value = strconv.FormatInt(n, 10)
	case KeyValueUint:
		n, ok := asUint64(v.Value)
		if !ok {
			return nil, fmt.Errorf("%s KeyValue requires uint64", v.Type)
		}
		value = strconv.FormatUint(n, 10)
	}
	return json.Marshal(struct {
		Type  KeyValueType `json:"type"`
		Value any          `json:"value"`
	}{v.Type, value})
}

func (v *KeyValue) UnmarshalJSON(data []byte) error {
	var wire struct {
		Type  KeyValueType    `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	v.Type = wire.Type
	switch wire.Type {
	case KeyValueLong:
		var s string
		if err := json.Unmarshal(wire.Value, &s); err != nil {
			return errors.New("llint KeyValue JSON value must be a decimal string")
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid llint KeyValue: %w", err)
		}
		v.Value = n
	case KeyValueUint:
		var s string
		if err := json.Unmarshal(wire.Value, &s); err != nil {
			return errors.New("uint KeyValue JSON value must be a decimal string")
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid uint KeyValue: %w", err)
		}
		v.Value = n
	case KeyValueBool:
		var n bool
		if err := json.Unmarshal(wire.Value, &n); err != nil {
			return err
		}
		v.Value = n
	case KeyValueString, KeyValueJSONString:
		var s string
		if err := json.Unmarshal(wire.Value, &s); err != nil {
			return err
		}
		v.Value = s
	case KeyValueInt:
		var n int64
		if err := json.Unmarshal(wire.Value, &n); err != nil {
			return err
		}
		v.Value = n
	case KeyValueFloat, KeyValueDouble:
		var n float64
		if err := json.Unmarshal(wire.Value, &n); err != nil {
			return err
		}
		v.Value = n
	default:
		return fmt.Errorf("unknown KeyValue type %q", wire.Type)
	}
	return nil
}

type SendRequest struct {
	ClientMessageID uint64
	To              string
	IsGroup         bool
	DirectedUsers   []string
	Ext             map[string]KeyValue
	Body            MessageBody
}

type SendResult struct {
	// MessageID is the authoritative message ID assigned by the server after
	// a successful ACK. It is the send-side equivalent of Message.MetaID on
	// the receiving side.
	MessageID uint64
	// ClientMessageID is only the client-generated correlation/idempotency ID
	// used to match the ACK and safely retry an outcome-unknown send.
	ClientMessageID uint64
	// ServerMessageID is kept as a compatibility alias for MessageID.
	// Deprecated: use MessageID.
	ServerMessageID uint64
	ServerTimestamp uint64
}

func buildOutgoingMeta(codec internalprotocol.Codec, appKey, userID, domain, resource string, req SendRequest) (internalprotocol.Meta, error) {
	if req.ClientMessageID == 0 {
		return internalprotocol.Meta{}, errors.New("ClientMessageID is required")
	}
	if req.To == "" {
		return internalprotocol.Meta{}, errors.New("message recipient is required")
	}
	if len(req.DirectedUsers) > 0 && !req.IsGroup {
		return internalprotocol.Meta{}, errors.New("directed users require a group message")
	}
	content, err := encodeOutgoingContent(req.Body)
	if err != nil {
		return internalprotocol.Meta{}, err
	}
	ext, err := encodeMessageExt(req.Ext)
	if err != nil {
		return internalprotocol.Meta{}, fmt.Errorf("message ext: %w", err)
	}
	messageType := internalprotocol.MessageChat
	if req.IsGroup {
		messageType = internalprotocol.MessageGroupChat
	}
	// Meta.to：单聊是 bare JID；群聊带 domain=conference.<domain>。
	// C++ 客户端的生产发送路径同时携带 app_key；服务端据此解析群队列，
	// 尤其是 ROUTE_DIRECT 不能依赖服务端从 guid 隐式补全 app_key。
	to := internalprotocol.JID{Name: req.To}
	to.AppKey = appKey
	if req.IsGroup {
		to.Domain = "conference." + domain
	}
	// MessageBody.from/to 只携带 bare user ID。
	body := internalprotocol.MessageBody{Kind: messageType,
		From: internalprotocol.JID{Name: userID}, To: internalprotocol.JID{Name: req.To},
		Contents: []internalprotocol.Content{content}, Ext: ext}
	payload, err := codec.EncodeMessageBody(body)
	if err != nil {
		return internalprotocol.Meta{}, fmt.Errorf("marshal message body: %w", err)
	}
	route := internalprotocol.RouteAll
	if len(req.DirectedUsers) > 0 {
		route = internalprotocol.RouteDirect
	}
	return internalprotocol.Meta{ID: req.ClientMessageID, To: to, Namespace: internalprotocol.NamespaceChat, Payload: payload, Route: route, DirectedUsers: append([]string(nil), req.DirectedUsers...)}, nil
}

func buildSendMeta(c *Client, userID string, req SendRequest, id uint64) (internalprotocol.Meta, error) {
	req.ClientMessageID = id
	return buildOutgoingMeta(c.codec, c.cfg.AppKey, userID, c.cfg.Domain, c.cfg.Resource, req)
}

func encodeMessageExt(values map[string]KeyValue) ([]internalprotocol.KeyValue, error) {
	if len(values) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]internalprotocol.KeyValue, 0, len(values))
	for _, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("extension key is empty")
		}
		value := values[key]
		encoded := internalprotocol.KeyValue{Key: key}
		switch value.Type {
		case KeyValueBool:
			v, ok := value.Value.(bool)
			if !ok {
				return nil, fmt.Errorf("%q: bool KeyValue requires bool", key)
			}
			encoded.Kind, encoded.Bool = internalprotocol.KeyValueBool, v
		case KeyValueInt:
			v, ok := asInt64(value.Value)
			if !ok {
				return nil, fmt.Errorf("%q: int KeyValue requires an integer", key)
			}
			encoded.Kind, encoded.Int64 = internalprotocol.KeyValueInt, v
		case KeyValueUint:
			v, ok := asUint64(value.Value)
			if !ok {
				return nil, fmt.Errorf("%q: uint KeyValue requires a non-negative integer", key)
			}
			encoded.Kind, encoded.Uint64 = internalprotocol.KeyValueUint, v
		case KeyValueLong:
			v, ok := asInt64(value.Value)
			if !ok {
				return nil, fmt.Errorf("%q: llint KeyValue requires an integer", key)
			}
			encoded.Kind, encoded.Int64 = internalprotocol.KeyValueLong, v
		case KeyValueFloat:
			v, ok := asFloat64(value.Value)
			if !ok {
				return nil, fmt.Errorf("%q: float KeyValue requires float32 or float64", key)
			}
			encoded.Kind, encoded.Float = internalprotocol.KeyValueFloat, float32(v)
		case KeyValueDouble:
			v, ok := asFloat64(value.Value)
			if !ok {
				return nil, fmt.Errorf("%q: double KeyValue requires float32 or float64", key)
			}
			encoded.Kind, encoded.Double = internalprotocol.KeyValueDouble, v
		case KeyValueString, KeyValueJSONString:
			v, ok := value.Value.(string)
			if !ok {
				return nil, fmt.Errorf("%q: %s KeyValue requires string", key, value.Type)
			}
			encoded.Kind, encoded.String = internalprotocol.KeyValueString, v
			if value.Type == KeyValueJSONString {
				encoded.Kind = internalprotocol.KeyValueJSONString
			}
		default:
			return nil, fmt.Errorf("%q: unsupported KeyValue type %q", key, value.Type)
		}
		result = append(result, encoded)
	}
	return result, nil
}

func encodeOutgoingContent(body MessageBody) (internalprotocol.Content, error) {
	switch body.Type {
	case MessageBodyText:
		return internalprotocol.Content{Kind: internalprotocol.ContentText, Text: body.Text}, nil
	case MessageBodyCommand:
		// 老协议中 cmd 消息的 params 参数已废弃，不再编码。
		return internalprotocol.Content{Kind: internalprotocol.ContentCommand, Action: body.Action}, nil
	case MessageBodyCustom:
		exts, err := encodeStringKeyValues(body.CustomExts)
		if err != nil {
			return internalprotocol.Content{}, fmt.Errorf("custom extensions: %w", err)
		}
		return internalprotocol.Content{Kind: internalprotocol.ContentCustom, Event: body.Event, CustomExts: exts}, nil
	default:
		return internalprotocol.Content{}, fmt.Errorf("unsupported outgoing message body type %q", body.Type)
	}
}

func encodeStringKeyValues(values map[string]KeyValue) ([]internalprotocol.KeyValue, error) {
	result := make([]internalprotocol.KeyValue, 0, len(values))
	for key, value := range values {
		if value.Type != KeyValueString && value.Type != KeyValueJSONString {
			return nil, fmt.Errorf("%q: sending only supports string and json_string KeyValue", key)
		}
		s, ok := value.Value.(string)
		if !ok {
			return nil, fmt.Errorf("%q: KeyValue value must be string", key)
		}
		t := internalprotocol.KeyValueString
		if value.Type == KeyValueJSONString {
			t = internalprotocol.KeyValueJSONString
		}
		result = append(result, internalprotocol.KeyValue{Key: key, Kind: t, String: s})
	}
	return result, nil
}

func parseIncomingMessage(codec internalprotocol.Codec, meta internalprotocol.Meta) (*Message, error) {
	wire, err := codec.DecodeMessageBody(meta.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal message body: %w", err)
	}
	msg := &Message{From: wire.From.Name, To: wire.To.Name, IsGroup: wire.Kind == internalprotocol.MessageGroupChat, MetaID: meta.ID, Timestamp: meta.Timestamp, Ext: decodeKeyValues(wire.Ext), OnlineState: parseOnlineState(meta.Attributes)}
	// 对外只暴露单个 body：当前单条消息只对应一个 body，其它端 SDK 也尚未实现
	// 多 body。若 wire 上意外出现多个 content，只取第一个，其余丢弃。
	if len(wire.Contents) > 0 {
		msg.Body = decodeContent(wire.Contents[0])
	}
	return msg, nil
}

func decodeContent(content internalprotocol.Content) *MessageBody {
	body := &MessageBody{}
	switch content.Kind {
	case internalprotocol.ContentText:
		body.Type, body.Text = MessageBodyText, content.Text
	case internalprotocol.ContentCommand:
		// 老协议中 cmd 消息的 params 参数已废弃，不再对外暴露。
		body.Type, body.Action = MessageBodyCommand, content.Action
	case internalprotocol.ContentCustom:
		body.Type, body.Event, body.CustomExts = MessageBodyCustom, content.Event, decodeKeyValues(content.CustomExts)
	default:
		body.Type, body.RawType = MessageBodyUnknown, int32(content.Kind)
		body.RawPayload = append([]byte(nil), content.RawPayload...)
	}
	return body
}

func parseMessage(c *Client, meta internalprotocol.Meta) (*Message, error) {
	return parseIncomingMessage(c.codec, meta)
}

func decodeKeyValues(values []internalprotocol.KeyValue) map[string]KeyValue {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]KeyValue, len(values))
	for _, value := range values {
		var decoded KeyValue
		switch value.Kind {
		case internalprotocol.KeyValueBool:
			decoded = KeyValue{KeyValueBool, value.Bool}
		case internalprotocol.KeyValueInt:
			decoded = KeyValue{KeyValueInt, value.Int64}
		case internalprotocol.KeyValueUint:
			decoded = KeyValue{KeyValueUint, value.Uint64}
		case internalprotocol.KeyValueLong:
			decoded = KeyValue{KeyValueLong, value.Int64}
		case internalprotocol.KeyValueFloat:
			decoded = KeyValue{KeyValueFloat, float64(value.Float)}
		case internalprotocol.KeyValueDouble:
			decoded = KeyValue{KeyValueDouble, value.Double}
		case internalprotocol.KeyValueString:
			decoded = KeyValue{KeyValueString, value.String}
		case internalprotocol.KeyValueJSONString:
			decoded = KeyValue{KeyValueJSONString, value.String}
		default:
			continue
		}
		result[value.Key] = decoded
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

func asUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	case uint:
		return uint64(n), true
	case int8:
		if n >= 0 {
			return uint64(n), true
		}
	case int16:
		if n >= 0 {
			return uint64(n), true
		}
	case int32:
		if n >= 0 {
			return uint64(n), true
		}
	case int64:
		if n >= 0 {
			return uint64(n), true
		}
	case int:
		if n >= 0 {
			return uint64(n), true
		}
	case float64:
		if n >= 0 && n <= math.MaxUint64 && math.Trunc(n) == n {
			return uint64(n), true
		}
	}
	return 0, false
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// Equal provides a precise comparison for tests and users handling decoded
// floating-point KeyValues without exposing protobuf implementation details.
func (v KeyValue) Equal(other KeyValue) bool {
	return v.Type == other.Type && bytes.Equal(mustJSON(v), mustJSON(other))
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
