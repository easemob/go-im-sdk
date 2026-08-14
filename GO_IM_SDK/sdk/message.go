package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

// Message is the stable, JSON-serializable representation delivered to users.
type Message struct {
	From      string              `json:"from"`
	To        string              `json:"to"`
	IsGroup   bool                `json:"is_group"`
	MetaID    uint64              `json:"meta_id"`
	Timestamp uint64              `json:"timestamp"`
	Bodies    []*MessageBody      `json:"bodies"`
	Ext       map[string]KeyValue `json:"ext,omitempty"`
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
	Params     map[string]KeyValue `json:"params,omitempty"`
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
	Body            MessageBody
}

type SendResult struct {
	ClientMessageID uint64
	ServerMessageID uint64
	ServerTimestamp uint64
}

var messageIDCounter atomic.Uint64

func nextClientMessageID() uint64 {
	// A millisecond time prefix makes IDs useful in diagnostics; the atomic
	// suffix guarantees uniqueness inside the process, which is the SDK's
	// documented idempotency boundary.
	prefix := uint64(time.Now().UnixMilli()) << 20
	return prefix | (messageIDCounter.Add(1) & ((1 << 20) - 1))
}

func buildOutgoingMeta(codec internalprotocol.Codec, appKey, userID, domain, resource string, req SendRequest) (internalprotocol.Meta, error) {
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
		Contents: []internalprotocol.Content{content}}
	payload, err := codec.EncodeMessageBody(body)
	if err != nil {
		return internalprotocol.Meta{}, fmt.Errorf("marshal message body: %w", err)
	}
	id := req.ClientMessageID
	if id == 0 {
		id = nextClientMessageID()
	}
	route := internalprotocol.RouteAll
	if len(req.DirectedUsers) > 0 {
		route = internalprotocol.RouteDirect
	}
	return internalprotocol.Meta{ID: id, To: to, Namespace: internalprotocol.NamespaceChat, Payload: payload, Route: route, DirectedUsers: append([]string(nil), req.DirectedUsers...)}, nil
}

func buildSendMeta(c *Client, req SendRequest, id uint64) (internalprotocol.Meta, error) {
	req.ClientMessageID = id
	return buildOutgoingMeta(c.codec, c.cfg.AppKey, c.cfg.UserID, c.cfg.Domain, c.cfg.Resource, req)
}

func encodeOutgoingContent(body MessageBody) (internalprotocol.Content, error) {
	switch body.Type {
	case MessageBodyText:
		return internalprotocol.Content{Kind: internalprotocol.ContentText, Text: body.Text}, nil
	case MessageBodyCommand:
		params, err := encodeStringKeyValues(body.Params)
		if err != nil {
			return internalprotocol.Content{}, fmt.Errorf("command params: %w", err)
		}
		return internalprotocol.Content{Kind: internalprotocol.ContentCommand, Action: body.Action, Params: params}, nil
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
	msg := &Message{From: wire.From.Name, To: wire.To.Name, IsGroup: wire.Kind == internalprotocol.MessageGroupChat, MetaID: meta.ID, Timestamp: meta.Timestamp, Ext: decodeKeyValues(wire.Ext)}
	for _, content := range wire.Contents {
		body := &MessageBody{}
		switch content.Kind {
		case internalprotocol.ContentText:
			body.Type, body.Text = MessageBodyText, content.Text
		case internalprotocol.ContentCommand:
			body.Type, body.Action, body.Params = MessageBodyCommand, content.Action, decodeKeyValues(content.Params)
		case internalprotocol.ContentCustom:
			body.Type, body.Event, body.CustomExts = MessageBodyCustom, content.Event, decodeKeyValues(content.CustomExts)
		default:
			body.Type, body.RawType = MessageBodyUnknown, int32(content.Kind)
			body.RawPayload = append([]byte(nil), content.RawPayload...)
		}
		msg.Bodies = append(msg.Bodies, body)
	}
	return msg, nil
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
	case uint64:
		return n, true
	case uint:
		return uint64(n), true
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

// Equal provides a precise comparison for tests and users handling decoded
// floating-point KeyValues without exposing protobuf implementation details.
func (v KeyValue) Equal(other KeyValue) bool {
	return v.Type == other.Type && bytes.Equal(mustJSON(v), mustJSON(other))
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
