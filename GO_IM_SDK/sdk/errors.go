package sdk

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
)

// ErrorCode is stable and suitable for errors.Is/error inspection by callers.
type ErrorCode string

const (
	ErrDNS                ErrorCode = "DNS_ERROR"
	ErrStreamClosed       ErrorCode = "STREAM_CLOSED"
	ErrTLSFailed          ErrorCode = "TLS_FAILED"
	ErrTimeout            ErrorCode = "TIMEOUT"
	ErrHandshake          ErrorCode = "HANDSHAKE_ERROR"
	ErrIO                 ErrorCode = "IO_ERROR"
	ErrNotConnected       ErrorCode = "NOT_CONNECTED"
	ErrNotLoggedIn        ErrorCode = "NOT_LOGGED_IN"
	ErrAlreadyLoggedIn    ErrorCode = "ALREADY_LOGGED_IN"
	ErrClientClosed       ErrorCode = "CLIENT_CLOSED"
	ErrWriteBackpressure  ErrorCode = "WRITE_BACKPRESSURE"
	ErrInvalidToken       ErrorCode = "INVALID_TOKEN"
	ErrTokenExpired       ErrorCode = "TOKEN_EXPIRED"
	ErrUserNotFound       ErrorCode = "USER_NOT_FOUND"
	ErrUserForbidden      ErrorCode = "IM_FORBIDDEN"
	ErrPermissionDenied   ErrorCode = "PERMISSION_DENIED"
	ErrAppActiveLimit     ErrorCode = "APP_ACTIVE_LIMIT"
	ErrBindAnotherDevice  ErrorCode = "USER_BIND_ANOTHER_DEVICE"
	ErrTooManyDevices     ErrorCode = "USER_LOGIN_TOO_MANY_DEVICES"
	ErrResourceChanged    ErrorCode = "USER_DEVICE_CHANGED"
	ErrAuthentication     ErrorCode = "AUTHENTICATION_FAILED"
	ErrProtocol           ErrorCode = "PROTOCOL_ERROR"
	ErrRedirectLoop       ErrorCode = "REDIRECT_LOOP"
	ErrRedirectLimit      ErrorCode = "REDIRECT_LIMIT"
	ErrHandlerFailed      ErrorCode = "HANDLER_FAILED"
	ErrHandlerBacklog     ErrorCode = "HANDLER_BACKLOG"
	ErrKickedChangePass   ErrorCode = "KICKED_BY_PASSWORD_CHANGE"
	ErrSendOutcomeUnknown ErrorCode = "SEND_OUTCOME_UNKNOWN"
)

type SDKError struct {
	Code       ErrorCode
	Operation  string
	Reason     string
	StatusCode int32
	HTTPStatus int
	Cause      error
}

func (e *SDKError) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := string(e.Code)
	if e.Operation != "" {
		msg = e.Operation + ": " + msg
	}
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	return msg
}
func (e *SDKError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func errorCode(err error) ErrorCode {
	var se *SDKError
	if errors.As(err, &se) {
		return se.Code
	}
	return ""
}

func classifyNetworkError(op string, err error) error {
	if err == nil {
		return nil
	}
	var existing *SDKError
	if errors.As(err, &existing) {
		return err
	}
	var redirect *redirectError
	if errors.As(err, &redirect) {
		return err
	}
	code := ErrIO
	var dns *net.DNSError
	var certUnknown x509.UnknownAuthorityError
	var certHost x509.HostnameError
	var record tls.RecordHeaderError
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		code = ErrTimeout
	case errors.As(err, &dns):
		code = ErrDNS
	case errors.As(err, &certUnknown), errors.As(err, &certHost), errors.As(err, &record), strings.Contains(strings.ToLower(err.Error()), "tls") || strings.Contains(strings.ToLower(err.Error()), "certificate"):
		code = ErrTLSFailed
	case errors.Is(err, net.ErrClosed), strings.Contains(strings.ToLower(err.Error()), "eof"), strings.Contains(strings.ToLower(err.Error()), "close"):
		code = ErrStreamClosed
	}
	return &SDKError{Code: code, Operation: op, Cause: err}
}

func protocolError(op string, status int32, reason string) error {
	code := ErrProtocol
	switch status {
	case 1:
		switch {
		case strings.Contains(reason, "Sorry, token expired"):
			code = ErrTokenExpired
		case strings.Contains(reason, "Sorry, token or password does not match login info"):
			code = ErrInvalidToken
		case strings.Contains(reason, "Sorry, user not found"):
			code = ErrUserNotFound
		default:
			code = ErrAuthentication
		}
	case 2, 3, 4, 16, 17, 18:
		code = ErrAuthentication
	case 7:
		code = ErrPermissionDenied
		for _, s := range []string{"Sorry, the app month live count limit", "Sorry, the app day live count limit", "Sorry, the app online count limit"} {
			if strings.Contains(reason, s) {
				code = ErrAppActiveLimit
				break
			}
		}
	case 11:
		code = ErrBindAnotherDevice
	case 12:
		code = ErrUserForbidden
	case 13:
		code = ErrTooManyDevices
	case 20:
		code = ErrResourceChanged
	}
	return &SDKError{Code: code, Operation: op, StatusCode: status, Reason: reason}
}

func newError(code ErrorCode, op, reason string) error {
	return &SDKError{Code: code, Operation: op, Reason: reason}
}

func wrapError(code ErrorCode, op string, cause error) error {
	return &SDKError{Code: code, Operation: op, Cause: cause, Reason: fmt.Sprint(cause)}
}
