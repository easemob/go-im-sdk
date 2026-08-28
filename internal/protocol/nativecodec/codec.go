//go:build cgo && ((linux && (arm64 || amd64)) || (nativecodecdev && darwin && (arm64 || amd64)))

package nativecodec

/*
#cgo CFLAGS: -I${SRCDIR}/../../../native/include
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/../../../native/build/darwin-arm64/libem_msync_codec.a -lc++
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/../../../native/build/darwin-amd64/libem_msync_codec.a -lc++
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/../../../native/lib/linux-arm64-glibc/libem_msync_codec.a -lstdc++ -pthread
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/../../../native/lib/linux-amd64-glibc/libem_msync_codec.a -lstdc++ -pthread
#include <stdlib.h>
#include <string.h>
#include "em_msync_codec.h"
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

type Codec struct {
	mu     sync.RWMutex
	handle *C.EMCodec
}

var _ protocol.Codec = (*Codec)(nil)
var _ protocol.DecodeAdmissionEstimator = (*Codec)(nil)

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
	if v == C.EM_CODEC_LIMIT_EXCEEDED {
		return fmt.Errorf("%w: native codec error %d", protocol.ErrLimitExceeded, int(v))
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
	if uint64(out.size) > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: native codec output exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
	if out.data == nil {
		return nil, fmt.Errorf("native codec returned nil output with size %d", uint64(out.size))
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

type decodeTracker struct {
	stringBytes   uint64
	payloadBytes  uint64
	directedUsers uint64
}

func (t *decodeTracker) readString(p *C.char) (string, error) {
	return t.readStringPointer(unsafe.Pointer(p))
}

func (t *decodeTracker) readStringPointer(p unsafe.Pointer) (string, error) {
	if p == nil {
		return "", nil
	}
	if t.stringBytes > protocol.MaxSyncStringBytes {
		return "", fmt.Errorf("%w: decoded string bytes exceed %d", protocol.ErrLimitExceeded, protocol.MaxSyncStringBytes)
	}
	remaining := uint64(protocol.MaxSyncStringBytes) - t.stringBytes
	length := uint64(C.strnlen((*C.char)(p), C.size_t(remaining+1)))
	if length > remaining {
		return "", fmt.Errorf("%w: decoded string bytes exceed %d", protocol.ErrLimitExceeded, protocol.MaxSyncStringBytes)
	}
	t.stringBytes += length
	return copyCStringPointer(p, length), nil
}

func (t *decodeTracker) reserveStringBytes(size uint64) error {
	if t.stringBytes > protocol.MaxSyncStringBytes || size > uint64(protocol.MaxSyncStringBytes)-t.stringBytes {
		return fmt.Errorf("%w: decoded string bytes exceed %d", protocol.ErrLimitExceeded, protocol.MaxSyncStringBytes)
	}
	t.stringBytes += size
	return nil
}

func (t *decodeTracker) addPayload(size uint64) error {
	if t.payloadBytes > protocol.MaxSyncPayloadBytes || size > uint64(protocol.MaxSyncPayloadBytes)-t.payloadBytes {
		return fmt.Errorf("%w: sync payload bytes exceed %d", protocol.ErrLimitExceeded, protocol.MaxSyncPayloadBytes)
	}
	t.payloadBytes += size
	return nil
}

func (t *decodeTracker) addDirectedUsers(count uint64) error {
	if t.directedUsers > protocol.MaxSyncDirectedUsers || count > uint64(protocol.MaxSyncDirectedUsers)-t.directedUsers {
		return fmt.Errorf("%w: sync directed users exceed %d", protocol.ErrLimitExceeded, protocol.MaxSyncDirectedUsers)
	}
	t.directedUsers += count
	return nil
}

func readJID(v C.EMCodecJID, tracker *decodeTracker) (protocol.JID, error) {
	pointers := [...]unsafe.Pointer{unsafe.Pointer(v.app_key), unsafe.Pointer(v.name), unsafe.Pointer(v.domain), unsafe.Pointer(v.resource)}
	return readJIDPointers(pointers, tracker)
}

func readJIDPointers(pointers [4]unsafe.Pointer, tracker *decodeTracker) (protocol.JID, error) {
	var lengths [len(pointers)]uint64
	var total uint64
	for i, p := range pointers {
		if p == nil {
			continue
		}
		lengths[i] = uint64(C.strnlen((*C.char)(p), C.size_t(protocol.MaxJIDComponentBytes+1)))
		if lengths[i] > protocol.MaxJIDComponentBytes {
			return protocol.JID{}, fmt.Errorf("%w: JID component exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxJIDComponentBytes)
		}
		if total > uint64(protocol.MaxJIDBytes)-lengths[i] {
			return protocol.JID{}, fmt.Errorf("%w: JID exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxJIDBytes)
		}
		total += lengths[i]
	}
	if err := tracker.reserveStringBytes(total); err != nil {
		return protocol.JID{}, err
	}
	return protocol.JID{
		AppKey:         copyCStringPointer(pointers[0], lengths[0]),
		Name:           copyCStringPointer(pointers[1], lengths[1]),
		Domain:         copyCStringPointer(pointers[2], lengths[2]),
		ClientResource: copyCStringPointer(pointers[3], lengths[3]),
	}, nil
}

func copyCStringPointer(p unsafe.Pointer, length uint64) string {
	if p == nil || length == 0 {
		return ""
	}
	return C.GoStringN((*C.char)(p), C.int(length))
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
	frame, err := takeBuffer(&out)
	if err != nil {
		return nil, err
	}
	return withProvisionActionVersion(frame)
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
	return decodeFrame(f)
}

func (c *Codec) EstimateDecodeAdmission(data []byte) (protocol.DecodeAdmissionClass, error) {
	return estimateDecodeAdmission(data)
}

func nativeCodecMaxInputBytes() uint64 {
	return uint64(C.EM_CODEC_MAX_INPUT_BYTES)
}

func decodeFrame(f *C.EMCodecFrame) (*protocol.Frame, error) {
	out := &protocol.Frame{Command: protocol.Command(C.em_codec_frame_command(f)), TraceID: uint64(C.em_codec_frame_trace_id(f))}
	kind := C.em_codec_frame_kind(f)
	tracker := &decodeTracker{}
	st, err := readStatus(f, tracker)
	if err != nil {
		return nil, err
	}
	switch kind {
	case C.EM_CODEC_FRAME_PROVISION:
		sessionID, err := tracker.readString(C.em_codec_frame_session_id(f))
		if err != nil {
			return nil, err
		}
		var n C.size_t
		p := C.em_codec_frame_auth_token(f, &n)
		authToken, err := readBytes(unsafe.Pointer(p), uint64(n))
		if err != nil {
			return nil, err
		}
		out.Provision = &protocol.Provision{Status: st, SessionID: sessionID, AuthToken: authToken}
	case C.EM_CODEC_FRAME_UNREAD:
		queueCount := uint64(C.em_codec_frame_unread_queue_count(f))
		if queueCount > protocol.MaxFrameCollectionItems {
			return nil, fmt.Errorf("%w: unread queue count exceeds %d", protocol.ErrLimitExceeded, protocol.MaxFrameCollectionItems)
		}
		u := &protocol.Unread{Status: st, Timestamp: uint64(C.em_codec_frame_timestamp(f)), Queues: make([]protocol.JID, 0, int(queueCount))}
		for i := C.size_t(0); i < C.size_t(queueCount); i++ {
			var j C.EMCodecJID
			j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
			if C.em_codec_frame_unread_queue(f, i, &j, nil) != 0 {
				jid, err := readJID(j, tracker)
				if err != nil {
					return nil, err
				}
				u.Queues = append(u.Queues, jid)
			}
		}
		out.Unread = u
	case C.EM_CODEC_FRAME_NOTICE:
		var j C.EMCodecJID
		j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
		if C.em_codec_frame_queue(f, &j) != 0 {
			q, err := readJID(j, tracker)
			if err != nil {
				return nil, err
			}
			out.Notice = &q
		}
	case C.EM_CODEC_FRAME_SYNC_ACK, C.EM_CODEC_FRAME_SYNC_BATCH:
		out.Sync, err = readSync(f, st, tracker)
		if err != nil {
			return nil, err
		}
	case C.EM_CODEC_FRAME_LOGOUT:
		out.Logout = &protocol.Logout{Status: st}
	}
	return out, nil
}

func readStatus(f *C.EMCodecFrame, tracker *decodeTracker) (*protocol.Status, error) {
	code := int32(C.em_codec_frame_status_code(f))
	if code < 0 {
		return nil, nil
	}
	reason, err := tracker.readString(C.em_codec_frame_status_reason(f))
	if err != nil {
		return nil, err
	}
	redirectCount := uint64(C.em_codec_frame_redirect_count(f))
	if redirectCount > protocol.MaxFrameCollectionItems {
		return nil, fmt.Errorf("%w: status redirect count exceeds %d", protocol.ErrLimitExceeded, protocol.MaxFrameCollectionItems)
	}
	s := &protocol.Status{Code: protocol.StatusCode(code), Reason: reason, Redirects: make([]protocol.RedirectInfo, 0, int(redirectCount))}
	for i := C.size_t(0); i < C.size_t(redirectCount); i++ {
		host, err := tracker.readString(C.em_codec_frame_redirect_host(f, i))
		if err != nil {
			return nil, err
		}
		s.Redirects = append(s.Redirects, protocol.RedirectInfo{Host: host, Port: uint32(C.em_codec_frame_redirect_port(f, i))})
	}
	return s, nil
}

func readSync(f *C.EMCodecFrame, st *protocol.Status, tracker *decodeTracker) (*protocol.Sync, error) {
	metaCount := uint64(C.em_codec_frame_meta_count(f))
	if metaCount > protocol.MaxSyncMetas {
		return nil, fmt.Errorf("%w: sync meta count exceeds %d", protocol.ErrLimitExceeded, protocol.MaxSyncMetas)
	}
	s := &protocol.Sync{Status: st, MetaID: uint64(C.em_codec_frame_ack_client_id(f)), ServerID: uint64(C.em_codec_frame_ack_server_id(f)), Timestamp: uint64(C.em_codec_frame_timestamp(f)), NextKey: uint64(C.em_codec_frame_next_key(f)), IsLast: C.em_codec_frame_is_last(f) != 0}
	var q C.EMCodecJID
	q.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
	if C.em_codec_frame_queue(f, &q) != 0 {
		x, err := readJID(q, tracker)
		if err != nil {
			return nil, err
		}
		s.Queue = &x
	}
	s.Metas = make([]protocol.Meta, 0, int(metaCount))
	for i := C.size_t(0); i < C.size_t(metaCount); i++ {
		m := protocol.Meta{ID: uint64(C.em_codec_meta_id(f, i)), Timestamp: uint64(C.em_codec_meta_timestamp(f, i)), Namespace: protocol.Namespace(C.em_codec_meta_namespace(f, i)), Route: protocol.RouteType(C.em_codec_meta_route_type(f, i))}
		var n C.size_t
		payload := C.em_codec_meta_payload(f, i, &n)
		if err := tracker.addPayload(uint64(n)); err != nil {
			return nil, err
		}
		var err error
		m.Payload, err = readBytes(unsafe.Pointer(payload), uint64(n))
		if err != nil {
			return nil, err
		}
		// Field 9 shares the payload budget so an oversized attribute blob
		// cannot become an unbounded allocation vector of its own.
		var attributesLen C.size_t
		attributes := C.em_codec_meta_attributes(f, i, &attributesLen)
		if err := tracker.addPayload(uint64(attributesLen)); err != nil {
			return nil, err
		}
		m.Attributes, err = readBytes(unsafe.Pointer(attributes), uint64(attributesLen))
		if err != nil {
			return nil, err
		}
		var j C.EMCodecJID
		j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
		if C.em_codec_meta_from(f, i, &j) != 0 {
			m.From, err = readJID(j, tracker)
			if err != nil {
				return nil, err
			}
		}
		j = C.EMCodecJID{}
		j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
		if C.em_codec_meta_to(f, i, &j) != 0 {
			m.To, err = readJID(j, tracker)
			if err != nil {
				return nil, err
			}
		}
		userCount := uint64(C.em_codec_meta_directed_user_count(f, i))
		if err := tracker.addDirectedUsers(userCount); err != nil {
			return nil, err
		}
		m.DirectedUsers = make([]string, 0, int(userCount))
		for u := C.size_t(0); u < C.size_t(userCount); u++ {
			user, err := tracker.readString(C.em_codec_meta_directed_user(f, i, u))
			if err != nil {
				return nil, err
			}
			m.DirectedUsers = append(m.DirectedUsers, user)
		}
		s.Metas = append(s.Metas, m)
	}
	if _, err := protocol.SyncRetainedWeight(s); err != nil {
		return nil, err
	}
	return s, nil
}

func readBytes(p unsafe.Pointer, size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	if size > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: native byte field exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
	if p == nil {
		return nil, fmt.Errorf("native codec returned nil byte field with size %d", size)
	}
	return C.GoBytes(p, C.int(size)), nil
}

func (c *Codec) DecodeMessageBody(data []byte) (*protocol.MessageBody, error) {
	if len(data) > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: message body exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
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
	tracker := &decodeTracker{}
	var err error
	v := &protocol.MessageBody{Kind: protocol.MessageKind(C.em_codec_meta_message_type(f, 0))}
	var j C.EMCodecJID
	j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
	if C.em_codec_message_from(f, 0, &j) != 0 {
		v.From, err = readJID(j, tracker)
		if err != nil {
			return nil, err
		}
	}
	j = C.EMCodecJID{}
	j.struct_size = C.uint32_t(C.sizeof_EMCodecJID)
	if C.em_codec_message_to(f, 0, &j) != 0 {
		v.To, err = readJID(j, tracker)
		if err != nil {
			return nil, err
		}
	}
	v.Ext, err = readKVs(f, 0, -1, tracker)
	if err != nil {
		return nil, err
	}
	contentCount := uint64(C.em_codec_meta_content_count(f, 0))
	if contentCount > protocol.MaxFrameCollectionItems {
		return nil, fmt.Errorf("%w: message content count exceeds %d", protocol.ErrLimitExceeded, protocol.MaxFrameCollectionItems)
	}
	v.Contents = make([]protocol.Content, 0, int(contentCount))
	for i := C.size_t(0); i < C.size_t(contentCount); i++ {
		x := protocol.Content{Kind: protocol.ContentKind(C.em_codec_content_type(f, 0, i))}
		x.Text, err = tracker.readString(C.em_codec_content_text(f, 0, i))
		if err != nil {
			return nil, err
		}
		x.Action, err = tracker.readString(C.em_codec_content_action(f, 0, i))
		if err != nil {
			return nil, err
		}
		x.Event, err = tracker.readString(C.em_codec_content_custom_event(f, 0, i))
		if err != nil {
			return nil, err
		}
		if x.Kind == protocol.ContentCommand {
			x.Params, err = readKVs(f, 0, int(i), tracker)
		} else if x.Kind == protocol.ContentCustom {
			x.CustomExts, err = readKVs(f, 0, int(i), tracker)
		} else if x.Kind > protocol.ContentCustom {
			var n C.size_t
			p := C.em_codec_content_raw(f, 0, i, &n)
			x.RawPayload, err = readBytes(unsafe.Pointer(p), uint64(n))
		}
		if err != nil {
			return nil, err
		}
		v.Contents = append(v.Contents, x)
	}
	return v, nil
}

func readKVs(f *C.EMCodecFrame, m C.size_t, content int, tracker *decodeTracker) ([]protocol.KeyValue, error) {
	var count C.size_t
	if content < 0 {
		count = C.em_codec_meta_key_value_count(f, m)
	} else {
		count = C.em_codec_content_key_value_count(f, m, C.size_t(content))
	}
	if uint64(count) > protocol.MaxFrameCollectionItems {
		return nil, fmt.Errorf("%w: key/value count exceeds %d", protocol.ErrLimitExceeded, protocol.MaxFrameCollectionItems)
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
		key, err := tracker.readString(v.key)
		if err != nil {
			return nil, err
		}
		value, err := tracker.readString(v.string_value)
		if err != nil {
			return nil, err
		}
		x := protocol.KeyValue{Key: key, Kind: protocol.KeyValueKind(v._type), Int64: int64(v.integer_value), Double: float64(v.number_value), String: value}
		x.Bool = x.Int64 != 0
		x.Uint64 = uint64(x.Int64)
		x.Float = float32(x.Double)
		out = append(out, x)
	}
	return out, nil
}
func (c *Codec) DecodeStatistic(data []byte) (*protocol.Statistic, error) {
	if len(data) > protocol.MaxCodecInputBytes {
		return nil, fmt.Errorf("%w: statistic exceeds %d bytes", protocol.ErrLimitExceeded, protocol.MaxCodecInputBytes)
	}
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
	tracker := &decodeTracker{}
	replaceDeviceName, err := tracker.readString(C.em_codec_meta_statistic_device(f, 0))
	if err != nil {
		return nil, err
	}
	reason, err := tracker.readString(C.em_codec_meta_statistic_reason(f, 0))
	if err != nil {
		return nil, err
	}
	sessionID, err := tracker.readString(C.em_codec_meta_statistic_session_id(f, 0))
	if err != nil {
		return nil, err
	}
	return &protocol.Statistic{Operation: protocol.StatisticOperation(C.em_codec_meta_statistic_operation(f, 0)), ReplaceDeviceName: replaceDeviceName, Reason: reason, SessionID: sessionID}, nil
}
