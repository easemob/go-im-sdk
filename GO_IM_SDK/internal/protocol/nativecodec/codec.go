//go:build !gopbcodec && cgo && ((linux && (arm64 || amd64)) || (nativecodecdev && darwin && (arm64 || amd64)))

package nativecodec

/*
#cgo CFLAGS: -I${SRCDIR}/../../../native/include
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/../../../native/build/darwin-arm64/libem_msync_codec.a -lc++
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/../../../native/build/darwin-amd64/libem_msync_codec.a -lc++
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/../../../native/lib/linux-arm64-glibc/libem_msync_codec.a -lstdc++ -pthread
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/../../../native/lib/linux-amd64-glibc/libem_msync_codec.a -lstdc++ -pthread
#include <stdlib.h>
#include "em_msync_codec.h"
*/
import "C"

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/pb"
	"google.golang.org/protobuf/proto"
)

type Codec struct {
	mu     sync.RWMutex
	handle *C.EMCodec
}

var _ protocol.Codec = (*Codec)(nil)

func New() (*Codec, error) {
	var code C.EMCodecError
	h := C.em_codec_create(C.EM_CODEC_ABI_VERSION, &code)
	if h == nil {
		return nil, codecError(code)
	}
	return &Codec{handle: h}, nil
}
func (c *Codec) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c != nil && c.handle != nil {
		C.em_codec_destroy(c.handle)
		c.handle = nil
	}
}

func codecError(v C.EMCodecError) error {
	if v == C.EM_CODEC_OK {
		return nil
	}
	return fmt.Errorf("native codec error %d", int(v))
}
func cstr(s string) (*C.char, func()) {
	if s == "" {
		return nil, func() {}
	}
	p := C.CString(s)
	if p == nil {
		return nil, func() {}
	}
	return p, func() { C.free(unsafe.Pointer(p)) }
}
func bytesPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}
func takeBuffer(out *C.EMCodecBuffer) ([]byte, error) {
	defer C.em_codec_buffer_free(out)
	if out.size == 0 {
		return nil, nil
	}
	if uint64(out.size) > uint64(^uint32(0)>>1) {
		return nil, fmt.Errorf("native codec output too large: %d", uint64(out.size))
	}
	return C.GoBytes(unsafe.Pointer(out.data), C.int(out.size)), nil
}
func newBuffer() C.EMCodecBuffer {
	var b C.EMCodecBuffer
	b.struct_size = C.uint32_t(C.sizeof_EMCodecBuffer)
	return b
}

type cJID struct {
	v    C.EMCodecJID
	free func()
}

func makeJID(j protocol.JID) cJID {
	a, fa := cstr(j.AppKey)
	n, fn := cstr(j.Name)
	d, fd := cstr(j.Domain)
	r, fr := cstr(j.ClientResource)
	v := C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID), app_key: a, name: n, domain: d, resource: r}
	return cJID{v: v, free: func() { fa(); fn(); fd(); fr() }}
}
func goString(p *C.char) string {
	if p == nil {
		return ""
	}
	return C.GoString(p)
}
func goJID(v C.EMCodecJID) protocol.JID {
	return protocol.JID{AppKey: goString(v.app_key), Name: goString(v.name), Domain: goString(v.domain), ClientResource: goString(v.resource)}
}

func (c *Codec) EncodeProvision(v protocol.ProvisionRequest) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	j := makeJID(v.User)
	defer j.free()
	sv, f1 := cstr(v.SDKVersion)
	defer f1()
	r, f2 := cstr(v.Resource)
	defer f2()
	out := newBuffer()
	e := C.em_codec_encode_provision(c.handle, &j.v, sv, r, bytesPtr(v.AuthToken), C.size_t(len(v.AuthToken)), &out)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	return takeBuffer(&out)
}
func (c *Codec) EncodeUnread() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	out := newBuffer()
	e := C.em_codec_encode_unread(c.handle, &out)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	return takeBuffer(&out)
}
func (c *Codec) EncodeLogout(v protocol.LogoutRequest) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	s, f1 := cstr(v.SessionID)
	defer f1()
	r, f2 := cstr(v.Reason)
	defer f2()
	out := newBuffer()
	e := C.em_codec_encode_logout(c.handle, s, r, &out)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	return takeBuffer(&out)
}
func (c *Codec) EncodeSync(v protocol.SyncRequest) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	out := newBuffer()
	if v.Meta == nil {
		if v.Queue == nil {
			return nil, fmt.Errorf("sync requires meta or queue")
		}
		q := makeJID(*v.Queue)
		defer q.free()
		e := C.em_codec_encode_sync_queue(c.handle, &q.v, C.uint64_t(v.Key), &out)
		if e != C.EM_CODEC_OK {
			return nil, codecError(e)
		}
		return takeBuffer(&out)
	}
	m := v.Meta
	from := makeJID(m.From)
	defer from.free()
	to := makeJID(m.To)
	defer to.free()
	users, freeUsers, err := makeStrings(m.DirectedUsers)
	if err != nil {
		return nil, err
	}
	defer freeUsers()
	e := C.em_codec_encode_sync_meta(c.handle, C.uint64_t(m.ID), &from.v, &to.v, C.uint64_t(m.Timestamp), C.uint32_t(m.Namespace), C.uint32_t(m.Route), bytesPtr(m.Payload), C.size_t(len(m.Payload)), users, C.size_t(len(m.DirectedUsers)), &out)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	return takeBuffer(&out)
}

func makeStrings(v []string) (**C.char, func(), error) {
	if len(v) == 0 {
		return nil, func() {}, nil
	}
	base := (**C.char)(C.calloc(C.size_t(len(v)), C.size_t(unsafe.Sizeof(uintptr(0)))))
	if base == nil {
		return nil, func() {}, fmt.Errorf("native codec: allocate string array")
	}
	ps := unsafe.Slice(base, len(v))
	for i := range v {
		ps[i] = C.CString(v[i])
		if ps[i] == nil && v[i] != "" {
			for j := 0; j < i; j++ {
				C.free(unsafe.Pointer(ps[j]))
			}
			C.free(unsafe.Pointer(base))
			return nil, func() {}, fmt.Errorf("native codec: allocate string")
		}
	}
	return base, func() {
		for _, p := range ps {
			C.free(unsafe.Pointer(p))
		}
		C.free(unsafe.Pointer(base))
	}, nil
}

type requestMemory struct {
	req   C.EMCodecSendRequest
	frees []func()
}

func makeRequest(v protocol.MessageBody) (*requestMemory, error) {
	m := &requestMemory{}
	from := makeJID(v.From)
	to := makeJID(v.To)
	m.frees = append(m.frees, from.free, to.free)
	m.req.struct_size = C.uint32_t(C.sizeof_EMCodecSendRequest)
	m.req.from = from.v
	m.req.to = to.v
	m.req.message_type = C.uint32_t(v.Kind)
	ext, ef, err := makeKVs(v.Ext)
	if err != nil {
		m.free()
		return nil, err
	}
	m.frees = append(m.frees, ef)
	if ext != nil {
		m.req.extensions = ext
		m.req.extension_count = C.size_t(len(v.Ext))
	}
	var contents *C.EMCodecMessageContent
	if len(v.Contents) > 0 {
		contents = (*C.EMCodecMessageContent)(C.calloc(C.size_t(len(v.Contents)), C.size_t(C.sizeof_EMCodecMessageContent)))
		if contents == nil {
			m.free()
			return nil, fmt.Errorf("native codec: allocate contents")
		}
		m.frees = append(m.frees, func() { C.free(unsafe.Pointer(contents)) })
	}
	contentSlice := unsafe.Slice(contents, len(v.Contents))
	for idx, x := range v.Contents {
		ct := C.EMCodecMessageContent{struct_size: C.uint32_t(C.sizeof_EMCodecMessageContent), _type: C.uint32_t(x.Kind)}
		ct.text, ef = cstr(x.Text)
		m.frees = append(m.frees, ef)
		ct.action, ef = cstr(x.Action)
		m.frees = append(m.frees, ef)
		ct.custom_event, ef = cstr(x.Event)
		m.frees = append(m.frees, ef)
		vals := x.Params
		if x.Kind == protocol.ContentCustom {
			vals = x.CustomExts
		}
		kv, kf, err := makeKVs(vals)
		if err != nil {
			m.free()
			return nil, err
		}
		m.frees = append(m.frees, kf)
		if kv != nil {
			ct.values = kv
			ct.value_count = C.size_t(len(vals))
		}
		contentSlice[idx] = ct
	}
	if contents != nil {
		m.req.contents = contents
		m.req.content_count = C.size_t(len(v.Contents))
	}
	return m, nil
}
func (m *requestMemory) free() {
	for _, f := range m.frees {
		f()
	}
}
func makeKVs(v []protocol.KeyValue) (*C.EMCodecKeyValue, func(), error) {
	if len(v) == 0 {
		return nil, func() {}, nil
	}
	out := (*C.EMCodecKeyValue)(C.calloc(C.size_t(len(v)), C.size_t(C.sizeof_EMCodecKeyValue)))
	if out == nil {
		return nil, func() {}, fmt.Errorf("native codec: allocate key values")
	}
	slice := unsafe.Slice(out, len(v))
	frees := []func(){}
	for i, x := range v {
		k, f := cstr(x.Key)
		frees = append(frees, f)
		s, sf := cstr(x.String)
		frees = append(frees, sf)
		slice[i] = C.EMCodecKeyValue{struct_size: C.uint32_t(C.sizeof_EMCodecKeyValue), key: k, _type: C.uint32_t(x.Kind), integer_value: C.int64_t(x.Int64), number_value: C.double(x.Double), string_value: s}
		if x.Kind == protocol.KeyValueBool && x.Bool {
			slice[i].integer_value = 1
		}
		if x.Kind == protocol.KeyValueUint {
			slice[i].integer_value = C.int64_t(x.Uint64)
		}
		if x.Kind == protocol.KeyValueFloat {
			slice[i].number_value = C.double(x.Float)
		}
	}
	return out, func() {
		for _, f := range frees {
			f()
		}
		C.free(unsafe.Pointer(out))
	}, nil
}
func (c *Codec) EncodeMessageBody(v protocol.MessageBody) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	m, err := makeRequest(v)
	if err != nil {
		return nil, err
	}
	defer m.free()
	out := newBuffer()
	e := C.em_codec_encode_message_body(c.handle, &m.req, &out)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	return takeBuffer(&out)
}

func (c *Codec) DecodeFrame(data []byte) (*protocol.Frame, error) {
	data, err := decompressEnvelopePayload(data)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	var f *C.EMCodecFrame
	e := C.em_codec_decode_frame(c.handle, bytesPtr(data), C.size_t(len(data)), &f)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	defer C.em_codec_frame_free(f)
	return decodeFrame(f), nil
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
func decodeFrame(f *C.EMCodecFrame) *protocol.Frame {
	out := &protocol.Frame{Command: protocol.Command(C.em_codec_frame_command(f)), TraceID: uint64(C.em_codec_frame_trace_id(f))}
	st := readStatus(f)
	switch C.em_codec_frame_kind(f) {
	case C.EM_CODEC_FRAME_PROVISION:
		out.Provision = &protocol.Provision{Status: st, SessionID: goString(C.em_codec_frame_session_id(f)), AuthToken: readBytes(C.em_codec_frame_auth_token(f, nil), 0)}
		var n C.size_t
		p := C.em_codec_frame_auth_token(f, &n)
		out.Provision.AuthToken = readBytes(p, n)
	case C.EM_CODEC_FRAME_UNREAD:
		u := &protocol.Unread{Status: st, Timestamp: uint64(C.em_codec_frame_timestamp(f))}
		for i := C.size_t(0); i < C.em_codec_frame_unread_queue_count(f); i++ {
			var j C.EMCodecJID
			j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
			if C.em_codec_frame_unread_queue(f, i, &j, nil) != 0 {
				u.Queues = append(u.Queues, goJID(j))
			}
		}
		out.Unread = u
	case C.EM_CODEC_FRAME_NOTICE:
		var j C.EMCodecJID
		j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
		if C.em_codec_frame_queue(f, &j) != 0 {
			q := goJID(j)
			out.Notice = &q
		}
	case C.EM_CODEC_FRAME_SYNC_ACK, C.EM_CODEC_FRAME_SYNC_BATCH:
		out.Sync = readSync(f, st)
	case C.EM_CODEC_FRAME_LOGOUT:
		out.Logout = &protocol.Logout{Status: st}
	}
	return out
}
func readStatus(f *C.EMCodecFrame) *protocol.Status {
	code := int32(C.em_codec_frame_status_code(f))
	if code < 0 {
		return nil
	}
	s := &protocol.Status{Code: protocol.StatusCode(code), Reason: goString(C.em_codec_frame_status_reason(f))}
	for i := C.size_t(0); i < C.em_codec_frame_redirect_count(f); i++ {
		s.Redirects = append(s.Redirects, protocol.RedirectInfo{Host: goString(C.em_codec_frame_redirect_host(f, i)), Port: uint32(C.em_codec_frame_redirect_port(f, i))})
	}
	return s
}
func readSync(f *C.EMCodecFrame, st *protocol.Status) *protocol.Sync {
	s := &protocol.Sync{Status: st, MetaID: uint64(C.em_codec_frame_ack_client_id(f)), ServerID: uint64(C.em_codec_frame_ack_server_id(f)), Timestamp: uint64(C.em_codec_frame_timestamp(f)), NextKey: uint64(C.em_codec_frame_next_key(f)), IsLast: C.em_codec_frame_is_last(f) != 0}
	var q C.EMCodecJID
	q.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
	if C.em_codec_frame_queue(f, &q) != 0 {
		x := goJID(q)
		s.Queue = &x
	}
	for i := C.size_t(0); i < C.em_codec_frame_meta_count(f); i++ {
		m := protocol.Meta{ID: uint64(C.em_codec_meta_id(f, i)), Timestamp: uint64(C.em_codec_meta_timestamp(f, i)), Namespace: protocol.Namespace(C.em_codec_meta_namespace(f, i)), Route: protocol.RouteType(C.em_codec_meta_route_type(f, i))}
		var n C.size_t
		m.Payload = readBytes(C.em_codec_meta_payload(f, i, &n), n)
		var j C.EMCodecJID
		j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
		if C.em_codec_meta_from(f, i, &j) != 0 {
			m.From = goJID(j)
		}
		j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
		if C.em_codec_meta_to(f, i, &j) != 0 {
			m.To = goJID(j)
		}
		for u := C.size_t(0); u < C.em_codec_meta_directed_user_count(f, i); u++ {
			m.DirectedUsers = append(m.DirectedUsers, goString(C.em_codec_meta_directed_user(f, i, u)))
		}
		s.Metas = append(s.Metas, m)
	}
	return s
}
func readBytes(p *C.uint8_t, n C.size_t) []byte {
	if p == nil || n == 0 {
		return nil
	}
	if uint64(n) > uint64(^uint32(0)>>1) {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(p), C.int(n))
}

func (c *Codec) DecodeMessageBody(data []byte) (*protocol.MessageBody, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	var f *C.EMCodecFrame
	e := C.em_codec_decode_message_body(c.handle, bytesPtr(data), C.size_t(len(data)), &f)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	defer C.em_codec_frame_free(f)
	v := &protocol.MessageBody{Kind: protocol.MessageKind(C.em_codec_meta_message_type(f, 0))}
	var j C.EMCodecJID
	j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
	if C.em_codec_message_from(f, 0, &j) != 0 {
		v.From = goJID(j)
	}
	j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
	if C.em_codec_message_to(f, 0, &j) != 0 {
		v.To = goJID(j)
	}
	v.Ext = readKVs(f, 0, -1)
	for i := C.size_t(0); i < C.em_codec_meta_content_count(f, 0); i++ {
		x := protocol.Content{Kind: protocol.ContentKind(C.em_codec_content_type(f, 0, i)), Text: goString(C.em_codec_content_text(f, 0, i)), Action: goString(C.em_codec_content_action(f, 0, i)), Event: goString(C.em_codec_content_custom_event(f, 0, i))}
		if x.Kind == protocol.ContentCommand {
			x.Params = readKVs(f, 0, int(i))
		} else if x.Kind == protocol.ContentCustom {
			x.CustomExts = readKVs(f, 0, int(i))
		} else if x.Kind > protocol.ContentCustom {
			var n C.size_t
			x.RawPayload = readBytes(C.em_codec_content_raw(f, 0, i, &n), n)
		}
		v.Contents = append(v.Contents, x)
	}
	return v, nil
}
func readKVs(f *C.EMCodecFrame, m C.size_t, content int) []protocol.KeyValue {
	var count C.size_t
	if content < 0 {
		count = C.em_codec_meta_key_value_count(f, m)
	} else {
		count = C.em_codec_content_key_value_count(f, m, C.size_t(content))
	}
	out := make([]protocol.KeyValue, 0, int(count))
	for i := C.size_t(0); i < count; i++ {
		var v C.EMCodecKeyValue
		v.struct_size = C.uint32_t(C.sizeof_EMCodecKeyValue)
		var ok C.int
		if content < 0 {
			ok = C.em_codec_meta_key_value(f, m, i, &v)
		} else {
			ok = C.em_codec_content_key_value(f, m, C.size_t(content), i, &v)
		}
		if ok == 0 {
			continue
		}
		x := protocol.KeyValue{Key: goString(v.key), Kind: protocol.KeyValueKind(v._type), Int64: int64(v.integer_value), Double: float64(v.number_value), String: goString(v.string_value)}
		x.Bool = x.Int64 != 0
		x.Uint64 = uint64(x.Int64)
		x.Float = float32(x.Double)
		out = append(out, x)
	}
	return out
}
func (c *Codec) DecodeStatistic(data []byte) (*protocol.Statistic, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.handle == nil {
		return nil, fmt.Errorf("native codec is closed")
	}
	var f *C.EMCodecFrame
	e := C.em_codec_decode_statistic(c.handle, bytesPtr(data), C.size_t(len(data)), &f)
	if e != C.EM_CODEC_OK {
		return nil, codecError(e)
	}
	defer C.em_codec_frame_free(f)
	return &protocol.Statistic{Operation: protocol.StatisticOperation(C.em_codec_meta_statistic_operation(f, 0)), ReplaceDeviceName: goString(C.em_codec_meta_statistic_device(f, 0)), Reason: goString(C.em_codec_meta_statistic_reason(f, 0)), SessionID: goString(C.em_codec_meta_statistic_session_id(f, 0))}, nil
}
