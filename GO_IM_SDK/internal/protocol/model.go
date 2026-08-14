// Package protocol defines the protobuf-independent wire boundary used by the
// SDK. Values in this package are owned Go values: codec implementations must
// copy input/output buffers and must not retain caller memory.
package protocol

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
}

type MessageKind int32

const (
	MessageNormal MessageKind = iota
	MessageChat
	MessageGroupChat
	MessageChatRoom
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
