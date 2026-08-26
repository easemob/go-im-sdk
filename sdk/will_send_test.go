package sdk

import (
	"context"
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

func TestOnWillSendSeesAssignedIDAndDoesNotBlockSend(t *testing.T) {
	const explicitID = uint64(77)
	var seen *Message
	codec := &messageTestCodec{}
	run := &connectionRun{
		pending: make(map[uint64]chan ackResult), writes: make(chan writeRequest, 1), done: make(chan struct{}),
	}
	c := &Client{
		cfg: Config{
			AppKey: "org#app", Domain: "easemob.com", Resource: "resource",
			OnWillSend: func(_ context.Context, msg *Message) {
				seen = msg
				panic("observer must not fail Send")
			},
		},
		state: LoginStateLoggedIn, connState: ConnStateConnected, run: run, codec: codec, userID: "alice",
	}
	run.client = c
	go func() {
		req := <-run.writes
		req.done <- nil
		run.completeACK(&internalprotocol.Sync{MetaID: explicitID, ServerID: 9, Status: &internalprotocol.Status{Code: internalprotocol.StatusOK}})
	}()

	result, err := c.Send(context.Background(), SendRequest{
		ClientMessageID: explicitID, To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != 9 {
		t.Fatalf("result=%#v", result)
	}
	if seen == nil || seen.MetaID != explicitID || seen.To != "bob" {
		t.Fatalf("will-send message=%+v", seen)
	}
	raw, err := seen.ToJSON()
	if err != nil || len(raw) == 0 {
		t.Fatalf("ToJSON err=%v raw=%s", err, raw)
	}
}

func TestOnWillSendSeesAutomaticClientMessageID(t *testing.T) {
	var seenID uint64
	codec := &messageTestCodec{}
	run := &connectionRun{
		pending: make(map[uint64]chan ackResult), writes: make(chan writeRequest, 1), done: make(chan struct{}),
	}
	c := &Client{
		cfg: Config{
			AppKey: "org#app", Domain: "easemob.com",
			OnWillSend: func(_ context.Context, msg *Message) { seenID = msg.MetaID },
		},
		state: LoginStateLoggedIn, connState: ConnStateConnected, run: run, codec: codec, userID: "alice",
	}
	run.client = c
	c.idCounter.Store(initialMessageIDCounter([4]byte{1, 2, 3, 4}))
	go func() {
		req := <-run.writes
		req.done <- nil
		run.completeACK(&internalprotocol.Sync{MetaID: seenID, ServerID: 1, Status: &internalprotocol.Status{Code: internalprotocol.StatusOK}})
	}()

	result, err := c.Send(context.Background(), SendRequest{
		To: "bob", Body: MessageBody{Type: MessageBodyText, Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if seenID == 0 || result.ClientMessageID != seenID {
		t.Fatalf("seen=%d result=%#v", seenID, result)
	}
}
