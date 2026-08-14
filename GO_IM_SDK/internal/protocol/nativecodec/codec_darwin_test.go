//go:build nativecodecdev && darwin && cgo

package nativecodec

import (
	"testing"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

func TestNativeCodecSemanticRoundTrip(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	body := protocol.MessageBody{Kind: protocol.MessageGroupChat, From: protocol.JID{AppKey: "o#a", Name: "alice", Domain: "easemob.com", ClientResource: "go"}, To: protocol.JID{AppKey: "o#a", Name: "group", Domain: "easemob.com"}, Contents: []protocol.Content{{Kind: protocol.ContentCommand, Action: "run", Params: []protocol.KeyValue{{Key: "max", Kind: protocol.KeyValueLong, Int64: 1 << 60}, {Key: "json", Kind: protocol.KeyValueJSONString, String: `{"a":1}`}}}}}
	payload, err := c.EncodeMessageBody(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.DecodeMessageBody(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != body.Kind || decoded.From.Name != "alice" || decoded.Contents[0].Action != "run" || decoded.Contents[0].Params[0].Int64 != 1<<60 {
		t.Fatalf("decoded=%+v", decoded)
	}
	frame, err := c.EncodeSync(protocol.SyncRequest{Meta: &protocol.Meta{ID: 42, From: body.From, To: body.To, Namespace: protocol.NamespaceChat, Route: protocol.RouteDirect, Payload: payload, DirectedUsers: []string{"bob"}}})
	if err != nil {
		t.Fatal(err)
	}
	// EncodeSync creates an uplink frame; DecodeFrame intentionally decodes downlink payloads only.
	if len(frame) == 0 {
		t.Fatal("empty sync frame")
	}
	provision, err := c.EncodeProvision(protocol.ProvisionRequest{User: body.From, SDKVersion: "4.0.0-go", Resource: "go", AuthToken: []byte(`{"token":"t"}`)})
	if err != nil || len(provision) == 0 {
		t.Fatalf("provision len=%d err=%v", len(provision), err)
	}
}
