package sdk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/easemob/go-im-sdk/pb"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func TestWSSLoginSendReceiveAndLogout(t *testing.T) {
	handled := make(chan *Message, 1)
	serverErr := make(chan error, 1)
	readyForLogout := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		serverErr <- serveProtocolFixture(conn, readyForLogout)
	}))
	defer server.Close()

	cfg := validConfig()
	cfg.MsyncHost = "wss" + strings.TrimPrefix(server.URL, "https") + "/websocket"
	cfg.HeartbeatInterval = time.Hour
	cfg.MessageHandler = func(_ context.Context, msg *Message) error { handled <- msg; return nil }
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	client.wsDialer.TLSClientConfig = tlsConfig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := client.Send(ctx, SendRequest{ClientMessageID: 77, To: "peer", Body: MessageBody{Type: MessageBodyText, Text: "outgoing"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.ServerMessageID != 9001 || result.ServerTimestamp != 123456 {
		t.Fatalf("unexpected ACK: %+v", result)
	}
	select {
	case msg := <-handled:
		if msg.MetaID != 88 || msg.Bodies[0].Text != "incoming" {
			t.Fatalf("unexpected message: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("message handler not called")
	}
	select {
	case <-readyForLogout:
	case <-ctx.Done():
		t.Fatal("next_key was not committed")
	}
	if err = client.Logout(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("fixture did not finish")
	}
}

func serveProtocolFixture(conn *websocket.Conn, readyForLogout chan<- struct{}) error {
	read := func(command pb.MSync_Command) (*pb.MSync, error) {
		_, b, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		var m pb.MSync
		if err = proto.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		if m.GetCommand() != command {
			return nil, &fixtureError{"unexpected command"}
		}
		return &m, nil
	}
	write := func(command pb.MSync_Command, payload proto.Message) error {
		b, err := proto.Marshal(payload)
		if err != nil {
			return err
		}
		frame, err := proto.Marshal(&pb.MSync{Command: command.Enum(), Payload: b})
		if err != nil {
			return err
		}
		return conn.WriteMessage(websocket.BinaryMessage, frame)
	}
	if _, err := read(pb.MSync_PROVISION); err != nil {
		return err
	}
	if err := write(pb.MSync_PROVISION, &pb.Provision{Status: &pb.Status{ErrorCode: pb.Status_OK.Enum()}, SessionId: proto.String("session")}); err != nil {
		return err
	}
	if _, err := read(pb.MSync_UNREAD); err != nil {
		return err
	}
	if err := write(pb.MSync_UNREAD, &pb.CommUnreadDL{Status: &pb.Status{ErrorCode: pb.Status_OK.Enum()}}); err != nil {
		return err
	}
	sent, err := read(pb.MSync_SYNC)
	if err != nil {
		return err
	}
	var ul pb.CommSyncUL
	if err = proto.Unmarshal(sent.GetPayload(), &ul); err != nil {
		return err
	}
	if err = write(pb.MSync_SYNC, &pb.CommSyncDL{Status: &pb.Status{ErrorCode: pb.Status_OK.Enum()}, MetaId: proto.Uint64(ul.GetMeta().GetId()), ServerId: proto.Uint64(9001), Timestamp: proto.Uint64(123456)}); err != nil {
		return err
	}
	queue := &pb.JID{Name: proto.String("queue")}
	if err = write(pb.MSync_NOTICE, &pb.CommNotice{Queue: queue}); err != nil {
		return err
	}
	if _, err = read(pb.MSync_SYNC); err != nil {
		return err
	}
	body, _ := proto.Marshal(&pb.MessageBody{Type: pb.MessageBody_CHAT.Enum(), From: &pb.JID{Name: proto.String("peer")}, To: &pb.JID{Name: proto.String("user")}, Contents: []*pb.MessageBody_Content{{Type: pb.MessageBody_Content_TEXT.Enum(), Text: proto.String("incoming")}}})
	meta := &pb.Meta{Id: proto.Uint64(88), Timestamp: proto.Uint64(123457), Ns: pb.Meta_CHAT.Enum(), Payload: body}
	if err = write(pb.MSync_SYNC, &pb.CommSyncDL{Status: &pb.Status{ErrorCode: pb.Status_OK.Enum()}, Metas: []*pb.Meta{meta}, NextKey: proto.Uint64(42), Queue: queue}); err != nil {
		return err
	}
	next, err := read(pb.MSync_SYNC)
	if err != nil {
		return err
	}
	if err = proto.Unmarshal(next.GetPayload(), &ul); err != nil {
		return err
	}
	if ul.GetKey() != 42 {
		return &fixtureError{"next_key not advanced"}
	}
	if err = write(pb.MSync_SYNC, &pb.CommSyncDL{Status: &pb.Status{ErrorCode: pb.Status_OK.Enum()}, Queue: queue, IsLast: proto.Bool(true)}); err != nil {
		return err
	}
	close(readyForLogout)
	if _, err = read(pb.MSync_LOGOUT); err != nil {
		return err
	}
	return write(pb.MSync_LOGOUT, &pb.Logout{Status: &pb.Status{ErrorCode: pb.Status_OK.Enum()}})
}

type fixtureError struct{ message string }

func (e *fixtureError) Error() string { return e.message }
