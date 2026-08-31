// Package protocol defines the protobuf-independent wire boundary used by the
// SDK. Values in this package are owned Go values: codec implementations must
// copy input/output buffers and must not retain caller memory.
package protocol

import (
	"fmt"
	"unsafe"
)

type Command int32

const (
	CommandSync Command = iota
	CommandUnread
	CommandNotice
	CommandProvision
	CommandLogout
)

type Namespace int32

const (
	NamespaceStatistic  Namespace = 0
	NamespaceChat       Namespace = 1
	NamespaceMUC        Namespace = 2
	NamespaceRoster     Namespace = 3
	NamespaceConference Namespace = 4
	NamespaceNotify     Namespace = 5
	NamespaceQuery      Namespace = 6
)

type RouteType int32

const (
	RouteAll RouteType = iota
	RouteOnline
	RouteDirect
)

type StatusCode int32

const (
	StatusOK                       StatusCode = 0
	StatusFail                     StatusCode = 1
	StatusUnauthorized             StatusCode = 2
	StatusMissingParameter         StatusCode = 3
	StatusWrongParameter           StatusCode = 4
	StatusRedirect                 StatusCode = 5
	StatusTokenExpired             StatusCode = 6
	StatusPermissionDenied         StatusCode = 7
	StatusNoRoute                  StatusCode = 8
	StatusUnknownCommand           StatusCode = 9
	StatusPBParserError            StatusCode = 10
	StatusBindAnotherDevice        StatusCode = 11
	StatusIMForbidden              StatusCode = 12
	StatusTooManyDevices           StatusCode = 13
	StatusPlatformLimit            StatusCode = 14
	StatusUserMuted                StatusCode = 15
	StatusEncryptDisable           StatusCode = 16
	StatusEncryptEnable            StatusCode = 17
	StatusDecryptFailure           StatusCode = 18
	StatusPermissionDeniedExternal StatusCode = 19
	StatusResourceChanged          StatusCode = 20
)

type JID struct {
	AppKey         string
	Name           string
	Domain         string
	ClientResource string
}

func (j JID) ID() string {
	return j.AppKey + "/" + j.Name + "/" + j.Domain + "/" + j.ClientResource
}

// BareID is the queue identity used by the server. Resource identifies a
// device, but NOTICE and SYNC may differ in whether it is present.
func (j JID) BareID() string { return j.AppKey + "/" + j.Name + "/" + j.Domain }

type RedirectInfo struct {
	Host string
	Port uint32
}

type Status struct {
	Code      StatusCode
	Reason    string
	Redirects []RedirectInfo
}

type ProvisionRequest struct {
	User       JID
	SDKVersion string
	Resource   string
	AuthToken  []byte
}

type Provision struct {
	Status    *Status
	SessionID string
	AuthToken []byte
}

type Unread struct {
	Status    *Status
	Queues    []JID
	Timestamp uint64
}

type SyncRequest struct {
	Meta  *Meta
	Key   uint64
	Queue *JID
}

type Sync struct {
	Status    *Status
	MetaID    uint64
	ServerID  uint64
	Metas     []Meta
	NextKey   uint64
	Queue     *JID
	IsLast    bool
	Timestamp uint64
}

type LogoutRequest struct {
	SessionID string
	Reason    string
}

type Logout struct{ Status *Status }

type Frame struct {
	Command   Command
	TraceID   uint64
	Provision *Provision
	Unread    *Unread
	Notice    *JID
	Sync      *Sync
	Logout    *Logout
}

type Meta struct {
	ID             uint64
	From           JID
	To             JID
	Timestamp      uint64
	Namespace      Namespace
	Payload        []byte
	Route          RouteType
	Ext            []KeyValue
	DirectedUsers  []string
	ExpireTime     uint64
	LocalTimestamp uint64
	Environment    string
	// Attributes is the server-populated msync Meta field 9. When present it
	// is a JSON object carrying delivery metadata such as "is_online". The
	// server only emits it when the feature is enabled, so an empty value
	// means "unknown" rather than any particular default.
	Attributes []byte
}

const (
	retainedAllocationQuantum  = uint64(128)
	retainedAllocationOverhead = uint64(16)
	retainedSafetyFactor       = uint64(2)
	retainedPageSize           = uint64(4 << 10)
)

// SyncRetainedWeight returns a conservative deterministic charge for a Sync
// retained by an SDK worker queue. It accounts for object and slice backing
// allocations, rounds each allocation upward, applies a safety factor, then
// rounds the result to a page. The result is a queue-admission charge rather
// than a claim about the process's complete RSS.
func SyncRetainedWeight(sync *Sync) (int64, error) {
	if sync == nil {
		return 0, nil
	}
	var estimate retainedWeightEstimator
	if err := estimate.allocation(uint64(unsafe.Sizeof(*sync))); err != nil {
		return 0, err
	}
	if sync.Status != nil {
		if err := estimate.allocation(uint64(unsafe.Sizeof(*sync.Status))); err != nil {
			return 0, err
		}
		if err := estimate.string(sync.Status.Reason); err != nil {
			return 0, err
		}
		if err := estimate.slice(cap(sync.Status.Redirects), unsafe.Sizeof(RedirectInfo{})); err != nil {
			return 0, err
		}
		for i := range sync.Status.Redirects {
			if err := estimate.string(sync.Status.Redirects[i].Host); err != nil {
				return 0, err
			}
		}
	}
	if sync.Queue != nil {
		if err := estimate.allocation(uint64(unsafe.Sizeof(*sync.Queue))); err != nil {
			return 0, err
		}
		if err := estimate.jid(*sync.Queue); err != nil {
			return 0, err
		}
	}
	if err := estimate.slice(cap(sync.Metas), unsafe.Sizeof(Meta{})); err != nil {
		return 0, err
	}
	for i := range sync.Metas {
		meta := &sync.Metas[i]
		if err := estimate.slice(cap(meta.Payload), unsafe.Sizeof(byte(0))); err != nil {
			return 0, err
		}
		if err := estimate.slice(cap(meta.Attributes), unsafe.Sizeof(byte(0))); err != nil {
			return 0, err
		}
		if err := estimate.jid(meta.From); err != nil {
			return 0, err
		}
		if err := estimate.jid(meta.To); err != nil {
			return 0, err
		}
		if err := estimate.slice(cap(meta.Ext), unsafe.Sizeof(KeyValue{})); err != nil {
			return 0, err
		}
		for j := range meta.Ext {
			if err := estimate.string(meta.Ext[j].Key); err != nil {
				return 0, err
			}
			if err := estimate.string(meta.Ext[j].String); err != nil {
				return 0, err
			}
		}
		if err := estimate.slice(cap(meta.DirectedUsers), unsafe.Sizeof(string(""))); err != nil {
			return 0, err
		}
		for _, user := range meta.DirectedUsers {
			if err := estimate.string(user); err != nil {
				return 0, err
			}
		}
		if err := estimate.string(meta.Environment); err != nil {
			return 0, err
		}
	}

	weighted, ok := checkedRetainedMul(estimate.total, retainedSafetyFactor)
	if !ok {
		return 0, fmt.Errorf("%w: sync retained weight overflows", ErrLimitExceeded)
	}
	weighted, ok = checkedRetainedRoundUp(weighted, retainedPageSize)
	if !ok || weighted > MaxSyncRetainedWeight {
		return 0, fmt.Errorf("%w: sync retained weight exceeds %d bytes", ErrLimitExceeded, MaxSyncRetainedWeight)
	}
	return int64(weighted), nil
}

type retainedWeightEstimator struct {
	total uint64
}

func (e *retainedWeightEstimator) allocation(size uint64) error {
	if size == 0 {
		return nil
	}
	rounded, ok := checkedRetainedRoundUp(size, retainedAllocationQuantum)
	if !ok {
		return fmt.Errorf("%w: sync retained allocation overflows", ErrLimitExceeded)
	}
	rounded, ok = checkedRetainedAdd(rounded, retainedAllocationOverhead)
	if !ok {
		return fmt.Errorf("%w: sync retained allocation overflows", ErrLimitExceeded)
	}
	e.total, ok = checkedRetainedAdd(e.total, rounded)
	if !ok {
		return fmt.Errorf("%w: sync retained weight overflows", ErrLimitExceeded)
	}
	return nil
}

func (e *retainedWeightEstimator) slice(capacity int, elementSize uintptr) error {
	if capacity <= 0 {
		return nil
	}
	size, ok := checkedRetainedMul(uint64(capacity), uint64(elementSize))
	if !ok {
		return fmt.Errorf("%w: sync retained slice overflows", ErrLimitExceeded)
	}
	return e.allocation(size)
}

func (e *retainedWeightEstimator) string(value string) error {
	return e.allocation(uint64(len(value)))
}

func (e *retainedWeightEstimator) jid(jid JID) error {
	for _, value := range [...]string{jid.AppKey, jid.Name, jid.Domain, jid.ClientResource} {
		if err := e.string(value); err != nil {
			return err
		}
	}
	return nil
}

func checkedRetainedAdd(a, b uint64) (uint64, bool) {
	if a > ^uint64(0)-b {
		return 0, false
	}
	return a + b, true
}

func checkedRetainedMul(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, false
	}
	return a * b, true
}

func checkedRetainedRoundUp(value, quantum uint64) (uint64, bool) {
	if quantum == 0 {
		return 0, false
	}
	remainder := value % quantum
	if remainder == 0 {
		return value, true
	}
	return checkedRetainedAdd(value, quantum-remainder)
}

type MessageKind int32

const (
	MessageNormal     MessageKind = 0
	MessageChat       MessageKind = 1
	MessageGroupChat  MessageKind = 2
	MessageChatRoom   MessageKind = 3
	MessageReadACK    MessageKind = 4
	MessageDeliverACK MessageKind = 5
	MessageRecall     MessageKind = 6
	MessageChannelACK MessageKind = 7
	MessageEdit       MessageKind = 8
)

type ContentKind int32

const (
	ContentText     ContentKind = 0
	ContentImage    ContentKind = 1
	ContentVideo    ContentKind = 2
	ContentLocation ContentKind = 3
	ContentVoice    ContentKind = 4
	ContentFile     ContentKind = 5
	ContentCommand  ContentKind = 6
	ContentCustom   ContentKind = 7
	ContentCombine  ContentKind = 8
)

type MessageBody struct {
	Kind     MessageKind
	From     JID
	To       JID
	Contents []Content
	Ext      []KeyValue
}

type Content struct {
	Kind       ContentKind
	Text       string
	Action     string
	Params     []KeyValue
	Event      string
	CustomExts []KeyValue
	// RawPayload is the serialized complete content message. It is populated
	// for unknown kinds so future server fields survive the native boundary.
	RawPayload []byte
}

type KeyValueKind int32

const (
	KeyValueBool KeyValueKind = iota + 1
	KeyValueInt
	KeyValueUint
	KeyValueLong
	KeyValueFloat
	KeyValueDouble
	KeyValueString
	KeyValueJSONString
)

type KeyValue struct {
	Key    string
	Kind   KeyValueKind
	Bool   bool
	Int64  int64
	Uint64 uint64
	Float  float32
	Double float64
	String string
}

type StatisticOperation int32

const (
	StatisticInformation StatisticOperation = iota
	StatisticUserRemoved
	StatisticUserLoginAnotherDevice
	StatisticUserKickedByChangePassword
	StatisticUserKickedByOtherDevice
)

type Statistic struct {
	Operation         StatisticOperation
	ReplaceDeviceName string
	SessionID         string
	Reason            string
}
