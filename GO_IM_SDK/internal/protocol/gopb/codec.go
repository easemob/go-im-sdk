// Package gopb implements protocol.Codec with the generated Go protobuf. It is
// a migration adapter and is not part of the eventual customer distribution.
package gopb

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/pb"
	"google.golang.org/protobuf/proto"
)

type Codec struct{}

var traceSequence atomic.Uint64

var _ protocol.Codec = Codec{}

func New() protocol.Codec { return Codec{} }

func (Codec) EncodeProvision(v protocol.ProvisionRequest) ([]byte, error) {
	os := pb.Provision_OS_GO
	none := pb.Provision_COMPRESS_NONE
	direction := pb.Provision_BI
	payload, err := proto.Marshal(&pb.Provision{OsType: &os, Version: proto.String(v.SDKVersion), Resource: proto.String(v.Resource), AuthToken: clone(v.AuthToken), CompressType: []pb.Provision_CompressType{none}, ProtocolCompressType: []pb.Provision_CompressType{none}, ProtocolCompressDirection: &direction})
	if err != nil {
		return nil, fmt.Errorf("marshal provision: %w", err)
	}
	return envelope(pb.MSync_PROVISION, toPBJID(v.User), v.SDKVersion, payload)
}

func (Codec) EncodeUnread() ([]byte, error) {
	payload, err := proto.Marshal(&pb.CommUnreadUL{})
	if err != nil {
		return nil, fmt.Errorf("marshal unread: %w", err)
	}
	return envelope(pb.MSync_UNREAD, nil, "", payload)
}

func (Codec) EncodeSync(v protocol.SyncRequest) ([]byte, error) {
	w := &pb.CommSyncUL{}
	if v.Key != 0 {
		w.Key = proto.Uint64(v.Key)
	}
	if v.Queue != nil {
		w.Queue = toPBJID(*v.Queue)
	}
	if v.Meta != nil {
		w.Meta = toPBMeta(*v.Meta)
	}
	payload, err := proto.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("marshal sync: %w", err)
	}
	return envelope(pb.MSync_SYNC, nil, "", payload)
}

func (Codec) EncodeLogout(v protocol.LogoutRequest) ([]byte, error) {
	payload, err := proto.Marshal(&pb.Logout{SessionId: proto.String(v.SessionID), Reason: proto.String(v.Reason)})
	if err != nil {
		return nil, fmt.Errorf("marshal logout: %w", err)
	}
	return envelope(pb.MSync_LOGOUT, nil, "", payload)
}

func envelope(command pb.MSync_Command, jid *pb.JID, agent string, payload []byte) ([]byte, error) {
	version := pb.MSync_MSYNC_V1
	compress := uint32(0)
	encrypt := pb.EncryptType_ENCRYPT_NONE
	traceID := uint64(time.Now().UnixMilli())<<22 | (traceSequence.Add(1) & ((1 << 22) - 1))
	b, err := proto.Marshal(&pb.MSync{Version: &version, Command: &command, Guid: jid,
		CompressAlgorimth: &compress, EncryptType: []pb.EncryptType{encrypt}, TraceId: &traceID,
		UserAgent: optional(agent), Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	return b, nil
}

func (Codec) DecodeFrame(data []byte) (*protocol.Frame, error) {
	data, err := decompressEnvelopePayload(data)
	if err != nil {
		return nil, err
	}
	var m pb.MSync
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	out := &protocol.Frame{Command: protocol.Command(m.GetCommand()), TraceID: m.GetTraceId()}
	switch m.GetCommand() {
	case pb.MSync_PROVISION:
		var v pb.Provision
		if err := proto.Unmarshal(m.GetPayload(), &v); err != nil {
			return nil, err
		}
		out.Provision = &protocol.Provision{Status: fromStatus(v.GetStatus()), SessionID: v.GetSessionId(), AuthToken: clone(v.GetAuthToken())}
	case pb.MSync_UNREAD:
		var v pb.CommUnreadDL
		if err := proto.Unmarshal(m.GetPayload(), &v); err != nil {
			return nil, err
		}
		u := &protocol.Unread{Status: fromStatus(v.GetStatus()), Timestamp: v.GetTimestamp()}
		for _, q := range v.GetUnread() {
			if q != nil && q.GetQueue() != nil {
				u.Queues = append(u.Queues, fromPBJID(q.GetQueue()))
			}
		}
		out.Unread = u
	case pb.MSync_NOTICE:
		var v pb.CommNotice
		if err := proto.Unmarshal(m.GetPayload(), &v); err != nil {
			return nil, err
		}
		if v.GetQueue() != nil {
			q := fromPBJID(v.GetQueue())
			out.Notice = &q
		}
	case pb.MSync_SYNC:
		var v pb.CommSyncDL
		if err := proto.Unmarshal(m.GetPayload(), &v); err != nil {
			return nil, err
		}
		s := &protocol.Sync{Status: fromStatus(v.GetStatus()), MetaID: v.GetMetaId(), ServerID: v.GetServerId(), NextKey: v.GetNextKey(), IsLast: v.GetIsLast(), Timestamp: v.GetTimestamp()}
		if v.GetQueue() != nil {
			q := fromPBJID(v.GetQueue())
			s.Queue = &q
		}
		for _, m := range v.GetMetas() {
			if m != nil {
				s.Metas = append(s.Metas, fromPBMeta(m))
			}
		}
		out.Sync = s
	case pb.MSync_LOGOUT:
		var v pb.Logout
		if err := proto.Unmarshal(m.GetPayload(), &v); err != nil {
			return nil, err
		}
		out.Logout = &protocol.Logout{Status: fromStatus(v.GetStatus())}
	default:
		return nil, fmt.Errorf("unsupported command %d", m.GetCommand())
	}
	return out, nil
}

func decompressEnvelopePayload(data []byte) ([]byte, error) {
	var m pb.MSync
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	if m.GetCompressAlgorimth() == 0 {
		return data, nil
	}
	if m.GetCompressAlgorimth() != 1 {
		return nil, fmt.Errorf("unsupported payload compression algorithm %d", m.GetCompressAlgorimth())
	}
	r, err := zlib.NewReader(bytes.NewReader(m.GetPayload()))
	if err != nil {
		return nil, fmt.Errorf("open zlib payload: %w", err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(r, 16<<20))
	closeErr := r.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read zlib payload: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close zlib payload: %w", closeErr)
	}
	m.Payload = payload
	m.CompressAlgorimth = proto.Uint32(0)
	return proto.Marshal(&m)
}

func (Codec) EncodeMessageBody(v protocol.MessageBody) ([]byte, error) {
	kind := pb.MessageBody_Type(v.Kind)
	w := &pb.MessageBody{Type: &kind, From: toPBJID(v.From), To: toPBJID(v.To), Ext: toPBKeyValues(v.Ext)}
	for _, c := range v.Contents {
		t := pb.MessageBody_Content_Type(c.Kind)
		wc := &pb.MessageBody_Content{Type: &t, Text: optional(c.Text), Action: optional(c.Action), Params: toPBKeyValues(c.Params), CustomEvent: optional(c.Event), CustomExts: toPBKeyValues(c.CustomExts)}
		w.Contents = append(w.Contents, wc)
	}
	b, err := proto.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("marshal message body: %w", err)
	}
	return b, nil
}

func (Codec) DecodeMessageBody(data []byte) (*protocol.MessageBody, error) {
	var w pb.MessageBody
	if err := proto.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("unmarshal message body: %w", err)
	}
	v := &protocol.MessageBody{Kind: protocol.MessageKind(w.GetType()), From: fromPBJID(w.GetFrom()), To: fromPBJID(w.GetTo()), Ext: fromPBKeyValues(w.GetExt())}
	for _, wc := range w.GetContents() {
		if wc == nil {
			continue
		}
		c := protocol.Content{Kind: protocol.ContentKind(wc.GetType()), Text: wc.GetText(), Action: wc.GetAction(), Params: fromPBKeyValues(wc.GetParams()), Event: wc.GetCustomEvent(), CustomExts: fromPBKeyValues(wc.GetCustomExts())}
		if wc.GetType() < pb.MessageBody_Content_TEXT || wc.GetType() > pb.MessageBody_Content_CUSTOM {
			c.RawPayload, _ = proto.Marshal(wc)
		}
		v.Contents = append(v.Contents, c)
	}
	return v, nil
}

func (Codec) DecodeStatistic(data []byte) (*protocol.Statistic, error) {
	var w pb.StatisticsBody
	if err := proto.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("unmarshal statistic: %w", err)
	}
	return &protocol.Statistic{Operation: protocol.StatisticOperation(w.GetOperation()), ReplaceDeviceName: w.GetReplaceDeviceName(), SessionID: w.GetSessionId(), Reason: w.GetReason()}, nil
}

func toPBJID(v protocol.JID) *pb.JID {
	return &pb.JID{AppKey: optional(v.AppKey), Name: optional(v.Name), Domain: optional(v.Domain), ClientResource: optional(v.ClientResource)}
}
func fromPBJID(v *pb.JID) protocol.JID {
	if v == nil {
		return protocol.JID{}
	}
	return protocol.JID{AppKey: v.GetAppKey(), Name: v.GetName(), Domain: v.GetDomain(), ClientResource: v.GetClientResource()}
}
func fromStatus(v *pb.Status) *protocol.Status {
	if v == nil {
		return nil
	}
	s := &protocol.Status{Code: protocol.StatusCode(v.GetErrorCode()), Reason: v.GetReason()}
	for _, r := range v.GetRedirectInfo() {
		if r != nil {
			s.Redirects = append(s.Redirects, protocol.RedirectInfo{Host: r.GetHost(), Port: r.GetPort()})
		}
	}
	return s
}
func toPBMeta(v protocol.Meta) *pb.Meta {
	ns := pb.Meta_NameSpace(v.Namespace)
	m := &pb.Meta{Id: proto.Uint64(v.ID), From: optionalPBJID(v.From), To: optionalPBJID(v.To), Ns: &ns, Payload: clone(v.Payload), Ext: toPBKeyValues(v.Ext), DirectedUsers: append([]string(nil), v.DirectedUsers...), Env: optional(v.Environment)}
	// 0 值字段（timestamp / expire_time / local_timestamp / routetype）不显式序列化，
	// 与 iOS 客户端发送的消息保持一致。
	if v.Timestamp != 0 {
		m.Timestamp = proto.Uint64(v.Timestamp)
	}
	if v.ExpireTime != 0 {
		m.ExpireTime = proto.Uint64(v.ExpireTime)
	}
	if v.LocalTimestamp != 0 {
		m.LocalTimestamp = proto.Uint64(v.LocalTimestamp)
	}
	if v.Route != protocol.RouteAll {
		route := pb.Meta_RouteType(v.Route)
		m.Routetype = &route
	}
	return m
}

func optionalPBJID(v protocol.JID) *pb.JID {
	if v == (protocol.JID{}) {
		return nil
	}
	return toPBJID(v)
}
func fromPBMeta(v *pb.Meta) protocol.Meta {
	return protocol.Meta{ID: v.GetId(), From: fromPBJID(v.GetFrom()), To: fromPBJID(v.GetTo()), Timestamp: v.GetTimestamp(), Namespace: protocol.Namespace(v.GetNs()), Payload: clone(v.GetPayload()), Route: protocol.RouteType(v.GetRoutetype()), Ext: fromPBKeyValues(v.GetExt()), DirectedUsers: append([]string(nil), v.GetDirectedUsers()...), ExpireTime: v.GetExpireTime(), LocalTimestamp: v.GetLocalTimestamp(), Environment: v.GetEnv()}
}
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}
func clone(b []byte) []byte { return append([]byte(nil), b...) }

func toPBKeyValues(vs []protocol.KeyValue) []*pb.KeyValue {
	out := make([]*pb.KeyValue, 0, len(vs))
	for _, v := range vs {
		t := pb.KeyValue_ValueType(v.Kind)
		w := &pb.KeyValue{Key: optional(v.Key), Type: &t}
		switch v.Kind {
		case protocol.KeyValueBool:
			if v.Bool {
				w.Value = &pb.KeyValue_VarintValue{VarintValue: 1}
			} else {
				w.Value = &pb.KeyValue_VarintValue{}
			}
		case protocol.KeyValueInt, protocol.KeyValueLong:
			w.Value = &pb.KeyValue_VarintValue{VarintValue: v.Int64}
		case protocol.KeyValueUint:
			w.Value = &pb.KeyValue_VarintValue{VarintValue: int64(v.Uint64)}
		case protocol.KeyValueFloat:
			w.Value = &pb.KeyValue_FloatValue{FloatValue: v.Float}
		case protocol.KeyValueDouble:
			w.Value = &pb.KeyValue_DoubleValue{DoubleValue: v.Double}
		case protocol.KeyValueString, protocol.KeyValueJSONString:
			w.Value = &pb.KeyValue_StringValue{StringValue: v.String}
		}
		out = append(out, w)
	}
	return out
}
func fromPBKeyValues(vs []*pb.KeyValue) []protocol.KeyValue {
	if len(vs) == 0 {
		return nil
	}
	out := make([]protocol.KeyValue, 0, len(vs))
	for _, v := range vs {
		if v == nil {
			continue
		}
		x := protocol.KeyValue{Key: v.GetKey(), Kind: protocol.KeyValueKind(v.GetType())}
		switch v.GetType() {
		case pb.KeyValue_BOOL:
			x.Bool = v.GetVarintValue() != 0
		case pb.KeyValue_INT, pb.KeyValue_LLINT:
			x.Int64 = v.GetVarintValue()
		case pb.KeyValue_UINT:
			x.Uint64 = uint64(v.GetVarintValue())
		case pb.KeyValue_FLOAT:
			x.Float = v.GetFloatValue()
		case pb.KeyValue_DOUBLE:
			x.Double = v.GetDoubleValue()
		case pb.KeyValue_STRING, pb.KeyValue_JSON_STRING:
			x.String = v.GetStringValue()
		}
		out = append(out, x)
	}
	return out
}
