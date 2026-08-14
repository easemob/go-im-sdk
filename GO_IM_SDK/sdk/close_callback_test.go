package sdk

import (
	"context"
	"testing"
	"time"
)

func TestCloseFromEventCallbackDoesNotDeadlock(t *testing.T) {
	c, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	c.OnConnectionStateChanged(func(ConnState) { _ = c.Close(context.Background()); close(done) })
	c.setStates(LoginStateLoggedIn, ConnStateConnected)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked in callback dispatcher")
	}
}
