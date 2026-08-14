//go:build gopbcodec || (!linux && !nativecodecdev)

package sdk

import (
	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/internal/protocol/gopb"
)

func newProtocolCodec() (internalprotocol.Codec, error) { return gopb.New(), nil }
