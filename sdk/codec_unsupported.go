//go:build !linux && !nativecodecdev

package sdk

import (
	"fmt"

	internalprotocol "github.com/easemob/go-im-sdk/internal/protocol"
)

// The public release supports Linux native builds. macOS can be used for
// local validation with -tags nativecodecdev; unsupported platforms fail at
// client construction instead of silently falling back to a Go protobuf
// implementation that is not part of the public distribution.
func newProtocolCodec() (internalprotocol.Codec, error) {
	return nil, fmt.Errorf("native codec is unavailable on this platform; use a supported Linux build or -tags nativecodecdev")
}
