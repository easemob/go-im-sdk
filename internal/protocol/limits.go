package protocol

import "errors"

// MaxCodecInputBytes is the maximum complete frame accepted by the native
// codec ABI. Go-side adapters must enforce the same limit before constructing
// a replacement frame.
const MaxCodecInputBytes = 16 << 20

const (
	MaxSyncMetas            = 256
	MaxSyncPayloadBytes     = 2 << 20
	MaxSyncStringBytes      = 1 << 20
	MaxSyncDirectedUsers    = 1024
	MaxSyncRetainedWeight   = 8 << 20
	MaxJIDComponentBytes    = 4 << 10
	MaxJIDBytes             = 16 << 10
	MaxFrameCollectionItems = 4096
)

// ActionVersion is written on provision so MSync returns token detail errors
// (action_version >= 3.0) and merges MUC presence events (>= 5.1).
const ActionVersion = "v5.1"

// ErrLimitExceeded marks deterministic protocol resource-limit violations.
// Callers should use errors.Is because codec layers add operation context.
var ErrLimitExceeded = errors.New("protocol limit exceeded")
