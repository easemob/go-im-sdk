package gopb

import (
	"bytes"
	"compress/zlib"
	"testing"

	"github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/pb"
	"google.golang.org/protobuf/proto"
)

func TestDecodeFrameDecompressesZlibPayload(t *testing.T) {
	inner, err := proto.Marshal(&pb.CommSyncDL{Queue: &pb.JID{Name: proto.String("lxm2")}, IsLast: proto.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	if _, err = zw.Write(inner); err != nil {
		t.Fatal(err)
	}
	if err = zw.Close(); err != nil {
		t.Fatal(err)
	}
	command := pb.MSync_SYNC
	algorithm := uint32(1)
	outer, err := proto.Marshal(&pb.MSync{Command: &command, CompressAlgorimth: &algorithm, Payload: compressed.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := (Codec{}).DecodeFrame(outer)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Sync == nil || frame.Sync.Queue == nil || frame.Sync.Queue.Name != "lxm2" || !frame.Sync.IsLast {
		t.Fatalf("zlib SYNC payload was not decoded: %#v", frame.Sync)
	}
}

// Pin neutral enums to the wire integers consumed by both codec backends.
func TestNeutralEnumsMatchWire(t *testing.T) {
	checks := []struct {
		name      string
		got, want int32
	}{
		{"namespace statistic", int32(protocol.NamespaceStatistic), int32(pb.Meta_STATISTIC)},
		{"namespace chat", int32(protocol.NamespaceChat), int32(pb.Meta_CHAT)},
		{"namespace muc", int32(protocol.NamespaceMUC), int32(pb.Meta_MUC)},
		{"namespace roster", int32(protocol.NamespaceRoster), int32(pb.Meta_ROSTER)},
		{"namespace conference", int32(protocol.NamespaceConference), int32(pb.Meta_CONFERENCE)},
		{"namespace notify", int32(protocol.NamespaceNotify), int32(pb.Meta_NOTIFY)},
		{"namespace query", int32(protocol.NamespaceQuery), int32(pb.Meta_QUERY)},
		{"route all", int32(protocol.RouteAll), int32(pb.Meta_ROUTE_ALL)},
		{"route online", int32(protocol.RouteOnline), int32(pb.Meta_ROUTE_ONLINE)},
		{"route direct", int32(protocol.RouteDirect), int32(pb.Meta_ROUTE_DIRECT)},
		{"redirect", int32(protocol.StatusRedirect), int32(pb.Status_REDIRECT)},
		{"content text", int32(protocol.ContentText), int32(pb.MessageBody_Content_TEXT)},
		{"content image", int32(protocol.ContentImage), int32(pb.MessageBody_Content_IMAGE)},
		{"content video", int32(protocol.ContentVideo), int32(pb.MessageBody_Content_VIDEO)},
		{"content location", int32(protocol.ContentLocation), int32(pb.MessageBody_Content_LOCATION)},
		{"content voice", int32(protocol.ContentVoice), int32(pb.MessageBody_Content_VOICE)},
		{"content file", int32(protocol.ContentFile), int32(pb.MessageBody_Content_FILE)},
		{"content command", int32(protocol.ContentCommand), int32(pb.MessageBody_Content_COMMAND)},
		{"content custom", int32(protocol.ContentCustom), int32(pb.MessageBody_Content_CUSTOM)},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %d want %d", c.name, c.got, c.want)
		}
	}
}

func TestSyncRoundTripPreservesJIDNamespacesAndRawPayloads(t *testing.T) {
	codec := Codec{}
	queue := protocol.JID{AppKey: "org#app", Name: "queue", Domain: "example.test", ClientResource: "server-1"}
	want := []protocol.Meta{
		{ID: 1, Namespace: protocol.NamespaceStatistic, Payload: []byte{1, 2}},
		{ID: 2, Namespace: protocol.NamespaceNotify, Payload: []byte(`{"type":"future"}`)},
		{ID: 3, Namespace: protocol.NamespaceMUC, Payload: []byte{0xff, 0x00}},
	}
	wire := &pb.CommSyncDL{Queue: toPBJID(queue), Metas: []*pb.Meta{toPBMeta(want[0]), toPBMeta(want[1]), toPBMeta(want[2])}}
	payload, err := proto.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	frameBytes, err := envelope(pb.MSync_SYNC, nil, "", payload)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := codec.DecodeFrame(frameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Sync.Queue == nil || *frame.Sync.Queue != queue {
		t.Fatalf("queue JID changed: %#v", frame.Sync.Queue)
	}
	for i := range want {
		got := frame.Sync.Metas[i]
		if got.Namespace != want[i].Namespace || !bytes.Equal(got.Payload, want[i].Payload) {
			t.Errorf("meta %d changed: %#v", i, got)
		}
	}
}

func TestUnknownContentPreservesCompleteWirePayload(t *testing.T) {
	typ := pb.MessageBody_Content_COMBINE
	unknown := &pb.MessageBody_Content{Type: &typ, Text: proto.String("future-content")}
	raw, err := proto.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(&pb.MessageBody{Contents: []*pb.MessageBody_Content{unknown}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := (Codec{}).DecodeMessageBody(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Contents) != 1 || decoded.Contents[0].Kind != protocol.ContentKind(typ) || !bytes.Equal(decoded.Contents[0].RawPayload, raw) {
		t.Fatalf("unknown content not preserved: %#v", decoded.Contents)
	}
}

func TestDecodeStatistic(t *testing.T) {
	op := pb.StatisticsBody_USER_KICKED_BY_OTHER_DEVICE
	b, err := proto.Marshal(&pb.StatisticsBody{Operation: &op, ReplaceDeviceName: proto.String("phone"), SessionId: proto.String("s"), Reason: proto.String("replaced")})
	if err != nil {
		t.Fatal(err)
	}
	got, err := (Codec{}).DecodeStatistic(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Operation != protocol.StatisticUserKickedByOtherDevice || got.ReplaceDeviceName != "phone" || got.SessionID != "s" || got.Reason != "replaced" {
		t.Fatalf("statistic changed: %#v", got)
	}
}
