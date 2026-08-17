//go:build linux || nativecodecdev

package sdk

import (
	"context"
	"testing"
	"time"
)

func TestCloseFromEventCallbackDoesNotDeadlock(t *testing.T) {
	done := make(chan struct{})
	var c *Client
	config := validConfig()
	config.OnConnectionStateChanged = func(string, ConnState) { _ = c.Close(context.Background()); close(done) }
	var err error
	c, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	c.setStates(LoginStateLoggedIn, ConnStateConnected)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked in callback dispatcher")
	}
}
