//go:build cgo && ((linux && (arm64 || amd64)) || (nativecodecdev && darwin && (arm64 || amd64)))

package nativecodec

import (
	"bytes"
	"testing"

	protocol "github.com/easemob/go-im-sdk/internal/protocol"
)

// msync field numbers used to hand-build a downlink SYNC frame. The codec only
// decodes downlink payloads, so an uplink encode cannot cover this path.
const (
	metaIDField         = 1
	metaNamespaceField  = 5
	metaPayloadField    = 6
	metaAttributesField = 9

	syncMetasField    = 4
	syncIsLastField   = 7
	msyncCommandField = 8
)

// TestDecodeFrameCarriesMetaAttributes locks in the full cgo path for msync
// Meta field 9: an absent field must decode to nil rather than to an empty
// non-nil slice, because the SDK treats absence as "unknown".
func TestDecodeFrameCarriesMetaAttributes(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// A CHAT meta must carry a parseable MessageBody or the codec rejects the
	// whole frame, so build the payload with the codec itself.
	payload, err := c.EncodeMessageBody(protocol.MessageBody{
		Kind:     protocol.MessageChat,
		From:     protocol.JID{AppKey: "o#a", Name: "alice", Domain: "easemob.com"},
		To:       protocol.JID{AppKey: "o#a", Name: "bob", Domain: "easemob.com"},
		Contents: []protocol.Content{{Kind: protocol.ContentText, Text: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	attributes := []byte(`{"is_online":0}`)
	withAttributes := appendVarintFieldForTest(nil, metaIDField, 1)
	withAttributes = appendVarintFieldForTest(withAttributes, metaNamespaceField, uint64(protocol.NamespaceChat))
	withAttributes = appendBytesFieldForTest(withAttributes, metaPayloadField, payload)
	withAttributes = appendBytesFieldForTest(withAttributes, metaAttributesField, attributes)

	withoutAttributes := appendVarintFieldForTest(nil, metaIDField, 2)
	withoutAttributes = appendVarintFieldForTest(withoutAttributes, metaNamespaceField, uint64(protocol.NamespaceChat))
	withoutAttributes = appendBytesFieldForTest(withoutAttributes, metaPayloadField, payload)

	// meta_id stays unset so the codec classifies this as a batch, not an ACK.
	sync := appendBytesFieldForTest(nil, syncMetasField, withAttributes)
	sync = appendBytesFieldForTest(sync, syncMetasField, withoutAttributes)
	sync = appendVarintFieldForTest(sync, syncIsLastField, 1)

	wire := appendVarintFieldForTest(nil, msyncCommandField, uint64(protocol.CommandSync))
	wire = appendBytesFieldForTest(wire, msyncPayloadField, sync)

	frame, err := c.DecodeFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Sync == nil || len(frame.Sync.Metas) != 2 {
		t.Fatalf("frame=%+v", frame)
	}
	if !bytes.Equal(frame.Sync.Metas[0].Attributes, attributes) {
		t.Fatalf("Attributes=%q, want %q", frame.Sync.Metas[0].Attributes, attributes)
	}
	if frame.Sync.Metas[1].Attributes != nil {
		t.Fatalf("absent field 9 decoded to %#v, want nil", frame.Sync.Metas[1].Attributes)
	}
}

func TestEncodeMessageBodyAllowsProductTextSizes(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	body := protocol.MessageBody{
		Kind: protocol.MessageChat,
		From: protocol.JID{AppKey: "o#a", Name: "alice", Domain: "easemob.com"},
		To:   protocol.JID{AppKey: "o#a", Name: "bob", Domain: "easemob.com"},
	}
	for _, n := range []int{4096, 4097, 5120, 5121} {
		body.Contents = []protocol.Content{{Kind: protocol.ContentText, Text: string(bytes.Repeat([]byte{'a'}, n))}}
		payload, err := c.EncodeMessageBody(body)
		if err != nil {
			t.Fatalf("%d-byte text: encode error %v", n, err)
		}
		decoded, err := c.DecodeMessageBody(payload)
		if err != nil {
			t.Fatalf("%d-byte text: decode error %v", n, err)
		}
		if len(decoded.Contents) != 1 || len(decoded.Contents[0].Text) != n {
			t.Fatalf("%d-byte text: decoded len=%d", n, len(decoded.Contents[0].Text))
		}
	}
}
