package sdk

import (
	"encoding/json"
	"errors"
	"strconv"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

// ToJSON serializes the protocol-level snapshot of a message for collection
// and storage. The shape follows msync Meta + MessageBody field names from
// the native protobuf (jid.proto / msync.proto / messagebody.proto / keyvalue.proto).
//
// Zero values are always written so a stored document has a stable schema:
// missing server fields appear as empty JID / 0 / "" / [] / {}.
//
// This is not protobuf JSON mapping (no .pb.go in this module). It covers the
// fields the SDK actually decodes today. Meta.payload is emitted as the
// decoded MessageBody object, not the raw bytes.
//
// json.Marshal(Message) remains the trimmed public view and is unchanged.
func (m *Message) ToJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	if m.protocol != nil {
		return json.Marshal(m.protocol)
	}
	return json.Marshal(protocolJSONFromPublic(m))
}

// NewOutgoingMessage builds the same Message a successful parse would produce
// for an outbound SendRequest, including the protocol snapshot used by ToJSON.
// ClientMessageID may be zero; the snapshot then stores id=0 until the caller
// assigns one.
func NewOutgoingMessage(appKey, userID, domain string, req SendRequest) (*Message, error) {
	if req.To == "" {
		return nil, errors.New("message recipient is required")
	}
	if len(req.DirectedUsers) > 0 && !req.IsGroup {
		return nil, errors.New("directed users require a group message")
	}
	content, err := encodeOutgoingContent(req.Body)
	if err != nil {
		return nil, err
	}
	ext, err := encodeMessageExt(req.Ext)
	if err != nil {
		return nil, err
	}
	kind := internalprotocol.MessageChat
	if req.IsGroup {
		kind = internalprotocol.MessageGroupChat
	}
	to := internalprotocol.JID{Name: req.To, AppKey: appKey}
	if req.IsGroup {
		to.Domain = "conference." + domain
	}
	route := internalprotocol.RouteAll
	if len(req.DirectedUsers) > 0 {
		route = internalprotocol.RouteDirect
	}
	body := &internalprotocol.MessageBody{
		Kind:     kind,
		From:     internalprotocol.JID{Name: userID},
		To:       internalprotocol.JID{Name: req.To},
		Contents: []internalprotocol.Content{content},
		Ext:      ext,
	}
	meta := internalprotocol.Meta{
		ID:            req.ClientMessageID,
		To:            to,
		Namespace:     internalprotocol.NamespaceChat,
		Route:         route,
		DirectedUsers: append([]string(nil), req.DirectedUsers...),
	}
	return messageFromProtocol(meta, body), nil
}

// messageProtocol is the stable collection document. Field names match the
// protobuf identifiers so stored JSON can be compared across SDK languages.
type messageProtocol struct {
	ID             uint64           `json:"id"`
	From           protocolJIDJSON  `json:"from"`
	To             protocolJIDJSON  `json:"to"`
	Timestamp      uint64           `json:"timestamp"`
	NS             int32            `json:"ns"`
	RouteType      int32            `json:"routetype"`
	Ext            []protocolKVJSON `json:"ext"`
	Meta           json.RawMessage  `json:"meta"`
	DirectedUsers  []string         `json:"directed_users"`
	ExpireTime     uint64           `json:"expire_time"`
	LocalTimestamp uint64           `json:"local_timestamp"`
	Env            string           `json:"env"`
	Payload        protocolBodyJSON `json:"payload"`
}

type protocolJIDJSON struct {
	AppKey         string `json:"app_key"`
	Name           string `json:"name"`
	Domain         string `json:"domain"`
	ClientResource string `json:"client_resource"`
}

type protocolKVJSON struct {
	Key         string  `json:"key"`
	Type        int32   `json:"type"`
	VarintValue int64   `json:"varint_value"`
	FloatValue  float32 `json:"float_value"`
	DoubleValue float64 `json:"double_value"`
	StringValue string  `json:"string_value"`
}

type protocolBodyJSON struct {
	Type   int32                 `json:"type"`
	From   protocolJIDJSON       `json:"from"`
	To     protocolJIDJSON       `json:"to"`
	Bodies []protocolContentJSON `json:"bodies"`
	Ext    []protocolKVJSON      `json:"ext"`
}

type protocolContentJSON struct {
	Type        int32            `json:"type"`
	Text        string           `json:"text"`
	Action      string           `json:"action"`
	Params      []protocolKVJSON `json:"params"`
	CustomEvent string           `json:"customEvent"`
	CustomExts  []protocolKVJSON `json:"customExts"`
}

func messageFromProtocol(meta internalprotocol.Meta, body *internalprotocol.MessageBody) *Message {
	msg := &Message{
		From:        body.From.Name,
		To:          body.To.Name,
		IsGroup:     body.Kind == internalprotocol.MessageGroupChat,
		MetaID:      meta.ID,
		Timestamp:   meta.Timestamp,
		Ext:         decodeKeyValues(body.Ext),
		OnlineState: parseOnlineState(meta.Attributes),
	}
	if len(body.Contents) > 0 {
		msg.Body = decodeContent(body.Contents[0])
	}
	msg.protocol = protocolJSONFromWire(meta, body)
	return msg
}

func protocolJSONFromWire(meta internalprotocol.Meta, body *internalprotocol.MessageBody) *messageProtocol {
	if body == nil {
		body = &internalprotocol.MessageBody{}
	}
	directed := meta.DirectedUsers
	if directed == nil {
		directed = []string{}
	}
	bodies := make([]protocolContentJSON, 0, len(body.Contents))
	for _, content := range body.Contents {
		bodies = append(bodies, protocolContentJSON{
			Type:        int32(content.Kind),
			Text:        content.Text,
			Action:      content.Action,
			Params:      protocolKVs(content.Params),
			CustomEvent: content.Event,
			CustomExts:  protocolKVs(content.CustomExts),
		})
	}
	if bodies == nil {
		bodies = []protocolContentJSON{}
	}
	return &messageProtocol{
		ID:             meta.ID,
		From:           protocolJID(meta.From),
		To:             protocolJID(meta.To),
		Timestamp:      meta.Timestamp,
		NS:             int32(meta.Namespace),
		RouteType:      int32(meta.Route),
		Ext:            protocolKVs(meta.Ext),
		Meta:           attributesJSON(meta.Attributes),
		DirectedUsers:  directed,
		ExpireTime:     meta.ExpireTime,
		LocalTimestamp: meta.LocalTimestamp,
		Env:            meta.Environment,
		Payload: protocolBodyJSON{
			Type:   int32(body.Kind),
			From:   protocolJID(body.From),
			To:     protocolJID(body.To),
			Bodies: bodies,
			Ext:    protocolKVs(body.Ext),
		},
	}
}

func protocolJSONFromPublic(m *Message) *messageProtocol {
	fromName, toName := m.From, m.To
	kind := internalprotocol.MessageChat
	if m.IsGroup {
		kind = internalprotocol.MessageGroupChat
	}
	var contents []internalprotocol.Content
	if m.Body != nil {
		if encoded, err := encodeOutgoingContent(*m.Body); err == nil {
			contents = []internalprotocol.Content{encoded}
		}
	}
	ext, _ := encodeMessageExt(m.Ext)
	body := &internalprotocol.MessageBody{Kind: kind, From: internalprotocol.JID{Name: fromName}, To: internalprotocol.JID{Name: toName}, Contents: contents, Ext: ext}
	meta := internalprotocol.Meta{ID: m.MetaID, From: internalprotocol.JID{Name: fromName}, To: internalprotocol.JID{Name: toName}, Timestamp: m.Timestamp, Namespace: internalprotocol.NamespaceChat}
	if m.OnlineState == MessageOnlineStateOnline {
		meta.Attributes = []byte(`{"is_online":1}`)
	} else if m.OnlineState == MessageOnlineStateOffline {
		meta.Attributes = []byte(`{"is_online":0}`)
	}
	return protocolJSONFromWire(meta, body)
}

func protocolJID(j internalprotocol.JID) protocolJIDJSON {
	return protocolJIDJSON{AppKey: j.AppKey, Name: j.Name, Domain: j.Domain, ClientResource: j.ClientResource}
}

func protocolKVs(values []internalprotocol.KeyValue) []protocolKVJSON {
	out := make([]protocolKVJSON, 0, len(values))
	for _, value := range values {
		item := protocolKVJSON{Key: value.Key, Type: int32(value.Kind)}
		switch value.Kind {
		case internalprotocol.KeyValueBool:
			if value.Bool {
				item.VarintValue = 1
			}
		case internalprotocol.KeyValueInt, internalprotocol.KeyValueLong:
			item.VarintValue = value.Int64
		case internalprotocol.KeyValueUint:
			item.StringValue = strconv.FormatUint(value.Uint64, 10)
		case internalprotocol.KeyValueFloat:
			item.FloatValue = value.Float
		case internalprotocol.KeyValueDouble:
			item.DoubleValue = value.Double
		case internalprotocol.KeyValueString, internalprotocol.KeyValueJSONString:
			item.StringValue = value.String
		}
		out = append(out, item)
	}
	if out == nil {
		return []protocolKVJSON{}
	}
	return out
}

func attributesJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	if json.Valid(raw) && raw[0] == '{' {
		return append(json.RawMessage(nil), raw...)
	}
	return json.RawMessage("{}")
}
