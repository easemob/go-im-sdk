//go:build nativecodecdev && darwin && cgo

package nativecodec

import (
	"sync"
	"testing"

	"github.com/easemob/go-im-sdk/internal/protocol"
)

func TestConcurrentUseAndClose(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 200; n++ {
				_, _ = c.EncodeUnread()
				_, _ = c.EncodeMessageBody(protocol.MessageBody{Kind: protocol.MessageChat, Contents: []protocol.Content{{Kind: protocol.ContentText, Text: "x"}}})
			}
		}()
	}
	c.Close()
	wg.Wait()
	c.Close()
	if _, err = c.EncodeUnread(); err == nil {
		t.Fatal("use after Close must fail")
	}
}
