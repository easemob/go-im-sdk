package sdk

import (
	"testing"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

func TestSuccessfulACKUsesServerIDAsFinalMessageID(t *testing.T) {
	const (
		clientID   = uint64(1786689166097840000)
		serverID   = uint64(1585377081150146024)
		serverTime = uint64(1786689166323)
	)
	wait := make(chan ackResult, 1)
	run := &connectionRun{pending: map[uint64]chan ackResult{clientID: wait}}
	run.completeACK(&internalprotocol.Sync{
		Status: &internalprotocol.Status{Code: internalprotocol.StatusOK},
		MetaID: clientID, ServerID: serverID, Timestamp: serverTime,
	})

	result := <-wait
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.result == nil || result.result.MessageID != serverID ||
		result.result.ServerMessageID != serverID || result.result.ClientMessageID != clientID ||
		result.result.ServerTimestamp != serverTime {
		t.Fatalf("result=%#v", result.result)
	}
	if _, exists := run.pending[clientID]; exists {
		t.Fatal("ACK did not remove the client correlation ID")
	}
}
