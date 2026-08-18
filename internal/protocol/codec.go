package protocol

// Codec is the complete msync serialization boundary. The public SDK uses the
// native C ABI; tests may provide a deterministic fake.
// Implementations must be safe for concurrent use.
type Codec interface {
	EncodeProvision(ProvisionRequest) ([]byte, error)
	EncodeUnread() ([]byte, error)
	EncodeSync(SyncRequest) ([]byte, error)
	EncodeLogout(LogoutRequest) ([]byte, error)
	DecodeFrame([]byte) (*Frame, error)

	EncodeMessageBody(MessageBody) ([]byte, error)
	DecodeMessageBody([]byte) (*MessageBody, error)
	DecodeStatistic([]byte) (*Statistic, error)
}

// DecodeAdmissionClass describes the conservative transient-memory tier for
// decoding a complete native codec frame. It intentionally describes a class,
// not a byte reservation, so the SDK can tune process budgets independently of
// the wire parser.
type DecodeAdmissionClass uint8

const (
	DecodeAdmissionTiny DecodeAdmissionClass = iota + 1
	DecodeAdmissionSmall
	DecodeAdmissionLarge
	DecodeAdmissionCompressed
	DecodeAdmissionAmbiguous
)

// DecodeAdmissionEstimator is an optional preflight capability implemented by
// codecs that can classify a frame without allocating decoded objects.
type DecodeAdmissionEstimator interface {
	EstimateDecodeAdmission([]byte) (DecodeAdmissionClass, error)
}
