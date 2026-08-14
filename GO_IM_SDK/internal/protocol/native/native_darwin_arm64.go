//go:build darwin && arm64 && cgo && nativecodec

package native

/*
#cgo CFLAGS: -I${SRCDIR}/../../../native/include
#cgo LDFLAGS: ${SRCDIR}/../../../native/build/darwin-arm64/libem_msync_codec.a -lc++
#include "em_msync_codec.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

type Codec struct{ h *C.EMCodec }

var _ protocol.Codec = (*Codec)(nil)

func New() (*Codec, error) {
	var e C.EMCodecError
	h := C.em_codec_create(C.EM_CODEC_ABI_VERSION, &e)
	if h == nil {
		return nil, fmt.Errorf("native codec create: %d", e)
	}
	// Deliberately no finalizer: this developer adapter has no synchronization
	// (unlike nativecodec.Codec). A finalizer would race with in-flight C calls
	// or a concurrent Close and cause use-after-free / double-free. Callers must
	// call Close explicitly and must not use the codec after Close.
	return &Codec{h: h}, nil
}
func (c *Codec) Close() {
	if c != nil && c.h != nil {
		C.em_codec_destroy(c.h)
		c.h = nil
	}
}

func cstr(s string) *C.char {
	if s == "" {
		return nil
	}
	return C.CString(s)
}
func free(p *C.char) {
	if p != nil {
		C.free(unsafe.Pointer(p))
	}
}
func bytesPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}
func outBytes(out *C.EMCodecBuffer, e C.EMCodecError) ([]byte, error) {
	defer C.em_codec_buffer_free(out)
	if e != C.EM_CODEC_OK {
		return nil, fmt.Errorf("native codec: %d", e)
	}
	if out.size == 0 {
		return []byte{}, nil
	}
	return C.GoBytes(unsafe.Pointer(out.data), C.int(out.size)), nil
}
func cj(j protocol.JID) (C.EMCodecJID, func()) {
	a, n, d, r := cstr(j.AppKey), cstr(j.Name), cstr(j.Domain), cstr(j.ClientResource)
	return C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID), app_key: a, name: n, domain: d, resource: r}, func() { free(a); free(n); free(d); free(r) }
}
func gj(j C.EMCodecJID) protocol.JID {
	return protocol.JID{AppKey: C.GoString(j.app_key), Name: C.GoString(j.name), Domain: C.GoString(j.domain), ClientResource: C.GoString(j.resource)}
}
func status(f *C.EMCodecFrame) *protocol.Status {
	code := C.em_codec_frame_status_code(f)
	if code < 0 {
		return nil
	}
	s := &protocol.Status{Code: protocol.StatusCode(code), Reason: C.GoString(C.em_codec_frame_status_reason(f))}
	for i := C.size_t(0); i < C.em_codec_frame_redirect_count(f); i++ {
		s.Redirects = append(s.Redirects, protocol.RedirectInfo{Host: C.GoString(C.em_codec_frame_redirect_host(f, i)), Port: uint32(C.em_codec_frame_redirect_port(f, i))})
	}
	return s
}

func (c *Codec) EncodeProvision(v protocol.ProvisionRequest) ([]byte, error) {
	j, done := cj(v.User)
	defer done()
	ver, res := cstr(v.SDKVersion), cstr(v.Resource)
	defer free(ver)
	defer free(res)
	out := C.EMCodecBuffer{struct_size: C.uint32_t(C.sizeof_EMCodecBuffer)}
	e := C.em_codec_encode_provision(c.h, &j, ver, res, bytesPtr(v.AuthToken), C.size_t(len(v.AuthToken)), &out)
	return outBytes(&out, e)
}
func (c *Codec) EncodeUnread() ([]byte, error) {
	out := C.EMCodecBuffer{struct_size: C.uint32_t(C.sizeof_EMCodecBuffer)}
	return outBytes(&out, C.em_codec_encode_unread(c.h, &out))
}
func (c *Codec) EncodeLogout(v protocol.LogoutRequest) ([]byte, error) {
	sid, r := cstr(v.SessionID), cstr(v.Reason)
	defer free(sid)
	defer free(r)
	out := C.EMCodecBuffer{struct_size: C.uint32_t(C.sizeof_EMCodecBuffer)}
	return outBytes(&out, C.em_codec_encode_logout(c.h, sid, r, &out))
}
func (c *Codec) EncodeSync(v protocol.SyncRequest) ([]byte, error) {
	out := C.EMCodecBuffer{struct_size: C.uint32_t(C.sizeof_EMCodecBuffer)}
	if v.Queue != nil {
		j, d := cj(*v.Queue)
		defer d()
		return outBytes(&out, C.em_codec_encode_sync_queue(c.h, &j, C.uint64_t(v.Key), &out))
	}
	if v.Meta == nil {
		return nil, fmt.Errorf("sync requires queue or meta")
	}
	f, fd := cj(v.Meta.From)
	defer fd()
	t, td := cj(v.Meta.To)
	defer td()
	users := make([]*C.char, len(v.Meta.DirectedUsers))
	for i, s := range v.Meta.DirectedUsers {
		users[i] = cstr(s)
		defer free(users[i])
	}
	var up **C.char
	if len(users) > 0 {
		up = (**C.char)(unsafe.Pointer(&users[0]))
	}
	e := C.em_codec_encode_sync_meta(c.h, C.uint64_t(v.Meta.ID), &f, &t, C.uint64_t(v.Meta.Timestamp), C.uint32_t(v.Meta.Namespace), C.uint32_t(v.Meta.Route), bytesPtr(v.Meta.Payload), C.size_t(len(v.Meta.Payload)), up, C.size_t(len(users)), &out)
	return outBytes(&out, e)
}

func (c *Codec) decode(data []byte, fn func(*C.EMCodec, *C.uint8_t, C.size_t, **C.EMCodecFrame) C.EMCodecError) (*C.EMCodecFrame, error) {
	var f *C.EMCodecFrame
	e := fn(c.h, bytesPtr(data), C.size_t(len(data)), &f)
	if e != C.EM_CODEC_OK {
		return nil, fmt.Errorf("native decode: %d", e)
	}
	return f, nil
}
func (c *Codec) DecodeFrame(data []byte) (*protocol.Frame, error) {
	f, e := c.decode(data, func(h *C.EMCodec, p *C.uint8_t, n C.size_t, o **C.EMCodecFrame) C.EMCodecError {
		return C.em_codec_decode_frame(h, p, n, o)
	})
	if e != nil {
		return nil, e
	}
	defer C.em_codec_frame_free(f)
	out := &protocol.Frame{Command: protocol.Command(C.em_codec_frame_command(f))}
	switch C.em_codec_frame_kind(f) {
	case C.EM_CODEC_FRAME_PROVISION:
		var n C.size_t
		p := C.em_codec_frame_auth_token(f, &n)
		out.Provision = &protocol.Provision{Status: status(f), SessionID: C.GoString(C.em_codec_frame_session_id(f)), AuthToken: C.GoBytes(unsafe.Pointer(p), C.int(n))}
	case C.EM_CODEC_FRAME_UNREAD:
		u := &protocol.Unread{Status: status(f), Timestamp: uint64(C.em_codec_frame_timestamp(f))}
		for i := C.size_t(0); i < C.em_codec_frame_unread_queue_count(f); i++ {
			j := C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID)}
			var n C.uint32_t
			if C.em_codec_frame_unread_queue(f, i, &j, &n) != 0 {
				u.Queues = append(u.Queues, gj(j))
			}
		}
		out.Unread = u
	case C.EM_CODEC_FRAME_NOTICE:
		j := C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID)}
		if C.em_codec_frame_queue(f, &j) != 0 {
			x := gj(j)
			out.Notice = &x
		}
	case C.EM_CODEC_FRAME_SYNC_ACK, C.EM_CODEC_FRAME_SYNC_BATCH:
		out.Sync = copySync(f)
	case C.EM_CODEC_FRAME_LOGOUT:
		out.Logout = &protocol.Logout{Status: status(f)}
	}
	return out, nil
}
func copySync(f *C.EMCodecFrame) *protocol.Sync {
	s := &protocol.Sync{Status: status(f), MetaID: uint64(C.em_codec_frame_ack_client_id(f)), ServerID: uint64(C.em_codec_frame_ack_server_id(f)), Timestamp: uint64(C.em_codec_frame_timestamp(f)), NextKey: uint64(C.em_codec_frame_next_key(f)), IsLast: C.em_codec_frame_is_last(f) != 0}
	q := C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID)}
	if C.em_codec_frame_queue(f, &q) != 0 {
		x := gj(q)
		s.Queue = &x
	}
	for i := C.size_t(0); i < C.em_codec_frame_meta_count(f); i++ {
		var n C.size_t
		p := C.em_codec_meta_payload(f, i, &n)
		m := protocol.Meta{ID: uint64(C.em_codec_meta_id(f, i)), Timestamp: uint64(C.em_codec_meta_timestamp(f, i)), Namespace: protocol.Namespace(C.em_codec_meta_namespace(f, i)), Route: protocol.RouteType(C.em_codec_meta_route_type(f, i)), Payload: C.GoBytes(unsafe.Pointer(p), C.int(n))}
		a := C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID)}
		if C.em_codec_meta_from(f, i, &a) != 0 {
			m.From = gj(a)
		}
		if C.em_codec_meta_to(f, i, &a) != 0 {
			m.To = gj(a)
		}
		for u := C.size_t(0); u < C.em_codec_meta_directed_user_count(f, i); u++ {
			m.DirectedUsers = append(m.DirectedUsers, C.GoString(C.em_codec_meta_directed_user(f, i, u)))
		}
		s.Metas = append(s.Metas, m)
	}
	return s
}

func kv(v C.EMCodecKeyValue) protocol.KeyValue {
	x := protocol.KeyValue{Key: C.GoString(v.key), Kind: protocol.KeyValueKind(v._type)}
	switch x.Kind {
	case protocol.KeyValueBool:
		x.Bool = v.integer_value != 0
	case protocol.KeyValueInt, protocol.KeyValueLong:
		x.Int64 = int64(v.integer_value)
	case protocol.KeyValueUint:
		x.Uint64 = uint64(v.integer_value)
	case protocol.KeyValueFloat:
		x.Float = float32(v.number_value)
	case protocol.KeyValueDouble:
		x.Double = float64(v.number_value)
	case protocol.KeyValueString, protocol.KeyValueJSONString:
		x.String = C.GoString(v.string_value)
	}
	return x
}
func body(f *C.EMCodecFrame) *protocol.MessageBody {
	b := &protocol.MessageBody{Kind: protocol.MessageKind(C.em_codec_meta_message_type(f, 0))}
	j := C.EMCodecJID{struct_size: C.uint32_t(C.sizeof_EMCodecJID)}
	if C.em_codec_message_from(f, 0, &j) != 0 {
		b.From = gj(j)
	}
	if C.em_codec_message_to(f, 0, &j) != 0 {
		b.To = gj(j)
	}
	for i := C.size_t(0); i < C.em_codec_meta_key_value_count(f, 0); i++ {
		v := C.EMCodecKeyValue{struct_size: C.uint32_t(C.sizeof_EMCodecKeyValue)}
		if C.em_codec_meta_key_value(f, 0, i, &v) != 0 {
			b.Ext = append(b.Ext, kv(v))
		}
	}
	for i := C.size_t(0); i < C.em_codec_meta_content_count(f, 0); i++ {
		x := protocol.Content{Kind: protocol.ContentKind(C.em_codec_content_type(f, 0, i)), Text: C.GoString(C.em_codec_content_text(f, 0, i)), Action: C.GoString(C.em_codec_content_action(f, 0, i)), Event: C.GoString(C.em_codec_content_custom_event(f, 0, i))}
		var n C.size_t
		p := C.em_codec_content_raw(f, 0, i, &n)
		if x.Kind > protocol.ContentCustom {
			x.RawPayload = C.GoBytes(unsafe.Pointer(p), C.int(n))
		}
		for k := C.size_t(0); k < C.em_codec_content_key_value_count(f, 0, i); k++ {
			v := C.EMCodecKeyValue{struct_size: C.uint32_t(C.sizeof_EMCodecKeyValue)}
			if C.em_codec_content_key_value(f, 0, i, k, &v) != 0 {
				if x.Kind == protocol.ContentCommand {
					x.Params = append(x.Params, kv(v))
				} else {
					x.CustomExts = append(x.CustomExts, kv(v))
				}
			}
		}
		b.Contents = append(b.Contents, x)
	}
	return b
}
func (c *Codec) DecodeMessageBody(data []byte) (*protocol.MessageBody, error) {
	f, e := c.decode(data, func(h *C.EMCodec, p *C.uint8_t, n C.size_t, o **C.EMCodecFrame) C.EMCodecError {
		return C.em_codec_decode_message_body(h, p, n, o)
	})
	if e != nil {
		return nil, e
	}
	defer C.em_codec_frame_free(f)
	return body(f), nil
}
func (c *Codec) DecodeStatistic(data []byte) (*protocol.Statistic, error) {
	f, e := c.decode(data, func(h *C.EMCodec, p *C.uint8_t, n C.size_t, o **C.EMCodecFrame) C.EMCodecError {
		return C.em_codec_decode_statistic(h, p, n, o)
	})
	if e != nil {
		return nil, e
	}
	defer C.em_codec_frame_free(f)
	return &protocol.Statistic{Operation: protocol.StatisticOperation(C.em_codec_meta_statistic_operation(f, 0)), ReplaceDeviceName: C.GoString(C.em_codec_meta_statistic_device(f, 0)), Reason: C.GoString(C.em_codec_meta_statistic_reason(f, 0)), SessionID: C.GoString(C.em_codec_meta_statistic_session_id(f, 0))}, nil
}

// C ABI represents uint as an int64 protobuf varint; conversion preserves bits.
func fillCKV(dst *C.EMCodecKeyValue, v protocol.KeyValue, cs *[]*C.char) {
	dst.struct_size = C.uint32_t(C.sizeof_EMCodecKeyValue)
	dst._type = C.uint32_t(v.Kind)
	dst.integer_value = C.int64_t(v.Int64)
	if v.Kind == protocol.KeyValueBool && v.Bool {
		dst.integer_value = 1
	}
	if v.Kind == protocol.KeyValueUint {
		dst.integer_value = C.int64_t(v.Uint64)
	}
	if v.Kind == protocol.KeyValueFloat {
		dst.number_value = C.double(v.Float)
	}
	if v.Kind == protocol.KeyValueDouble {
		dst.number_value = C.double(v.Double)
	}
	dst.key = cstr(v.Key)
	dst.string_value = cstr(v.String)
	*cs = append(*cs, dst.key, dst.string_value)
}
func (c *Codec) EncodeMessageBody(v protocol.MessageBody) ([]byte, error) {
	req, cleanup := request(v)
	defer cleanup()
	out := C.EMCodecBuffer{struct_size: C.uint32_t(C.sizeof_EMCodecBuffer)}
	return outBytes(&out, C.em_codec_encode_message_body(c.h, &req, &out))
}
func request(v protocol.MessageBody) (C.EMCodecSendRequest, func()) {
	from, fd := cj(v.From)
	to, td := cj(v.To)
	cs := []*C.char{}
	ext := make([]C.EMCodecKeyValue, len(v.Ext))
	for i, x := range v.Ext {
		fillCKV(&ext[i], x, &cs)
	}
	contents := make([]C.EMCodecMessageContent, len(v.Contents))
	vals := make([][]C.EMCodecKeyValue, len(v.Contents))
	for i, x := range v.Contents {
		contents[i].struct_size = C.uint32_t(C.sizeof_EMCodecMessageContent)
		contents[i]._type = C.uint32_t(x.Kind)
		contents[i].text = cstr(x.Text)
		contents[i].action = cstr(x.Action)
		contents[i].custom_event = cstr(x.Event)
		cs = append(cs, contents[i].text, contents[i].action, contents[i].custom_event)
		src := x.Params
		if x.Kind == protocol.ContentCustom {
			src = x.CustomExts
		}
		vals[i] = make([]C.EMCodecKeyValue, len(src))
		for k, z := range src {
			fillCKV(&vals[i][k], z, &cs)
		}
		if len(vals[i]) > 0 {
			contents[i].values = &vals[i][0]
			contents[i].value_count = C.size_t(len(vals[i]))
		}
	}
	r := C.EMCodecSendRequest{struct_size: C.uint32_t(C.sizeof_EMCodecSendRequest), from: from, to: to, message_type: C.uint32_t(v.Kind)}
	if len(ext) > 0 {
		r.extensions = &ext[0]
		r.extension_count = C.size_t(len(ext))
	}
	if len(contents) > 0 {
		r.contents = &contents[0]
		r.content_count = C.size_t(len(contents))
	}
	return r, func() {
		fd()
		td()
		for _, p := range cs {
			free(p)
		}
		runtime.KeepAlive(ext)
		runtime.KeepAlive(contents)
		runtime.KeepAlive(vals)
	}
}
