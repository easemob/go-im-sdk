//go:build linux || nativecodecdev

package sdk

import (
	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/internal/protocol/nativecodec"
)

func newProtocolCodec() (internalprotocol.Codec, error) {
	c, err := nativecodec.New()
	if err != nil {
		return nil, err
	}
	return c, nil
}
