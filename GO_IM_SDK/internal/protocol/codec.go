package protocol

// Codec is the complete msync serialization boundary. Implementations may use
// generated Go protobuf, a native C ABI, or a deterministic test fake.
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
