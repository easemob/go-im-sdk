package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultRESTTimeout      = 30 * time.Second
	defaultRESTBodyMaxBytes = int64(4 << 20)
)

// Response contains the bounded, fully-read HTTP response returned by a REST API.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// APIError describes a non-successful HTTP response. Response is always present
// for HTTP errors and contains at most defaultRESTBodyMaxBytes bytes.
type APIError struct {
	Response    *Response
	ServiceCode string
	RequestID   string
	RetryAfter  time.Duration
	Cause       error
}

func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	parts := []string{"REST API request failed"}
	if e.Response != nil {
		parts = append(parts, "status="+strconv.Itoa(e.Response.StatusCode))
	}
	if e.ServiceCode != "" {
		parts = append(parts, "service_code="+e.ServiceCode)
	}
	if e.RequestID != "" {
		parts = append(parts, "request_id="+e.RequestID)
	}
	if e.Cause != nil {
		parts = append(parts, "cause="+e.Cause.Error())
	}
	return strings.Join(parts, " ")
}

func (e *APIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// CreatePublicGroupOptions configures a public group. Public and member-only
// flags are deliberately not exposed: public=true and memberonly=false are fixed.
type CreatePublicGroupOptions struct {
	AllowInvites      *bool
	InviteNeedConfirm *bool
	MaxUsers          int
	Description       string
	Welcome           string
	Members           []string
}

// UpdateOwnUserInfo replaces the explicitly supplied properties of the current user.
func (c *Client) UpdateOwnUserInfo(ctx context.Context, attrs map[string]string) (*Response, error) {
	form := make(url.Values, len(attrs))
	for key, value := range attrs {
		form.Set(key, value)
	}
	return c.doREST(ctx, "update_own_user_info", http.MethodPut,
		"/metadata/user/"+url.PathEscape(c.currentUserID()), "", strings.NewReader(form.Encode()),
		"application/x-www-form-urlencoded", userInfoServiceCode)
}

// UserInfoField identifies one of the standard user properties accepted by
// UpdateOwnUserInfoField. Its value is the wire-level property name so that
// callers can use the same names with UpdateOwnUserInfo for custom fields.
type UserInfoField string

const (
	UserInfoNickname  UserInfoField = "nickname"
	UserInfoAvatarURL UserInfoField = "avatarurl"
	UserInfoPhone     UserInfoField = "phone"
	UserInfoMail      UserInfoField = "mail"
	UserInfoGender    UserInfoField = "gender"
	UserInfoSign      UserInfoField = "sign"
	UserInfoBirth     UserInfoField = "birth"
	UserInfoExt       UserInfoField = "ext"
)

func (f UserInfoField) valid() bool {
	switch f {
	case UserInfoNickname, UserInfoAvatarURL, UserInfoPhone, UserInfoMail,
		UserInfoGender, UserInfoSign, UserInfoBirth, UserInfoExt:
		return true
	default:
		return false
	}
}

// UpdateOwnUserInfoField updates one standard user property. It is a
// convenience wrapper around UpdateOwnUserInfo; empty values are passed
// through to the server so callers can use the same API if the service
// supports clearing a property.
func (c *Client) UpdateOwnUserInfoField(ctx context.Context, field UserInfoField, value string) (*Response, error) {
	if !field.valid() {
		return nil, fmt.Errorf("unsupported user info field %q", field)
	}
	return c.UpdateOwnUserInfo(ctx, map[string]string{string(field): value})
}

// FetchUserInfo fetches selected properties for the supplied users without caching them.
func (c *Client) FetchUserInfo(ctx context.Context, users, properties []string) (*Response, error) {
	body, err := json.Marshal(struct {
		Targets    []string `json:"targets"`
		Properties []string `json:"properties"`
	}{Targets: users, Properties: properties})
	if err != nil {
		return nil, fmt.Errorf("encode fetch user info request: %w", err)
	}
	return c.doREST(ctx, "fetch_user_info", http.MethodPost, "/metadata/user/get", "",
		bytes.NewReader(body), "application/json", fetchUserInfoServiceCode)
}

// CreatePublicGroup creates a public group that users can join without approval.
func (c *Client) CreatePublicGroup(ctx context.Context, name string, opt CreatePublicGroupOptions) (*Response, error) {
	allowInvites := true
	if opt.AllowInvites != nil {
		allowInvites = *opt.AllowInvites
	}
	inviteNeedConfirm := false
	if opt.InviteNeedConfirm != nil {
		inviteNeedConfirm = *opt.InviteNeedConfirm
	}
	maxUsers := opt.MaxUsers
	if maxUsers <= 0 {
		maxUsers = 200
	}
	body, err := json.Marshal(struct {
		Name              string   `json:"name"`
		Description       string   `json:"description"`
		Owner             string   `json:"owner"`
		Public            bool     `json:"public"`
		MemberOnly        bool     `json:"memberonly"`
		AllowInvites      bool     `json:"allowinvites"`
		InviteNeedConfirm bool     `json:"invite_need_confirm"`
		MaxUsers          int      `json:"maxusers"`
		Welcome           string   `json:"welcome,omitempty"`
		Members           []string `json:"members,omitempty"`
	}{
		Name: name, Description: opt.Description, Owner: c.currentUserID(),
		Public: true, MemberOnly: false, AllowInvites: allowInvites,
		InviteNeedConfirm: inviteNeedConfirm, MaxUsers: maxUsers,
		Welcome: opt.Welcome, Members: opt.Members,
	})
	if err != nil {
		return nil, fmt.Errorf("encode create public group request: %w", err)
	}
	return c.doREST(ctx, "create_public_group", http.MethodPost, "/chatgroups", c.resourceQuery(),
		bytes.NewReader(body), "application/json", genericServiceCode)
}

// JoinPublicGroup directly applies to a group. The server remains authoritative
// for group visibility, approval policy, capacity, and duplicate membership.
func (c *Client) JoinPublicGroup(ctx context.Context, groupID string) (*Response, error) {
	started := time.Now()
	response, err := c.doREST(ctx, "join_public_group_request", http.MethodPost,
		"/chatgroups/"+url.PathEscape(groupID)+"/apply", c.resourceQuery(), nil, "", genericServiceCode)
	statusCode, serviceCode := responseErrorFields(response, err)
	c.recordRESTTelemetry(ctx, "join_public_group", 1, time.Since(started), statusCode, serviceCode, err)
	return response, err
}

// LeaveGroup leaves a group without maintaining local membership state.
func (c *Client) LeaveGroup(ctx context.Context, groupID string) (*Response, error) {
	return c.doREST(ctx, "leave_group", http.MethodDelete,
		"/chatgroups/"+url.PathEscape(groupID)+"/quit", c.resourceQuery(), nil, "", genericServiceCode)
}

func (c *Client) resourceQuery() string {
	query := url.Values{"version": {"v3"}, "resource": {c.cfg.Resource}}
	return query.Encode()
}

type serviceCodeMapper func(status int) string

func userInfoServiceCode(status int) string {
	return map[int]string{http.StatusNotFound: "USER_NOT_FOUND", http.StatusUnauthorized: "AUTH_FAILED",
		http.StatusForbidden: "DATALENGTH_EXCEED", http.StatusTooManyRequests: "EXCEED_SERVICE_LIMIT"}[status]
}

func fetchUserInfoServiceCode(status int) string {
	return map[int]string{http.StatusNotFound: "USER_NOT_FOUND", http.StatusUnauthorized: "AUTH_FAILED",
		http.StatusBadRequest: "USERCOUNT_EXCEED", http.StatusTooManyRequests: "EXCEED_SERVICE_LIMIT"}[status]
}

func genericServiceCode(status int) string { return "" }

func (c *Client) doREST(ctx context.Context, operation, method, path, rawQuery string, body io.Reader,
	contentType string, mapCode serviceCodeMapper) (*Response, error) {
	started := time.Now()
	statusCode := 0
	serviceCode := ""
	var resultErr error
	defer func() {
		c.recordRESTTelemetry(ctx, operation, 1, time.Since(started), statusCode, serviceCode, resultErr)
	}()

	c.mu.RLock()
	restBase := c.restBase
	loginState := c.state
	token := c.token
	c.mu.RUnlock()
	if loginState == LoginStateLogout || loginState == LoginStateLoggingIn || restBase == "" {
		resultErr = newError(ErrNotLoggedIn, operation, "REST requires a successful login")
		return nil, resultErr
	}
	base, err := url.Parse(restBase)
	if err != nil {
		resultErr = fmt.Errorf("parse REST base URL: %w", err)
		return nil, resultErr
	}
	endpoint := strings.TrimRight(base.String(), "/") + path
	if rawQuery != "" {
		endpoint += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		resultErr = fmt.Errorf("create REST request: %w", err)
		return nil, resultErr
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.debug {
		c.logger.Debug("rest.request", "operation", operation, "method", method, "url", endpoint,
			"body_present", body != nil)
	}

	httpResponse, err := c.restHTTPClient().Do(req)
	if err != nil {
		resultErr = fmt.Errorf("execute REST request: %w", err)
		if c.debug {
			c.logger.Debug("rest.error", "operation", operation, "method", method, "url", endpoint, "error", resultErr)
		}
		return nil, resultErr
	}
	defer httpResponse.Body.Close()
	statusCode = httpResponse.StatusCode
	bodyBytes, err := readBoundedResponse(httpResponse.Body, defaultRESTBodyMaxBytes)
	response := &Response{StatusCode: statusCode, Header: httpResponse.Header.Clone(), Body: bodyBytes}
	if c.debug {
		c.logger.Debug("rest.response", "operation", operation, "method", method, "url", endpoint,
			"status", statusCode, "request_id", requestID(httpResponse.Header), "body_bytes", len(bodyBytes))
	}
	if err != nil {
		resultErr = &APIError{Response: response, RequestID: requestID(httpResponse.Header), Cause: err}
		return response, resultErr
	}
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		return response, nil
	}

	if mapCode != nil {
		serviceCode = mapCode(statusCode)
	}
	if serviceCode == "" {
		serviceCode = serviceCodeFromResponse(bodyBytes)
	}
	resultErr = &APIError{
		Response: response, ServiceCode: serviceCode, RequestID: requestID(httpResponse.Header),
		RetryAfter: parseRetryAfter(httpResponse.Header.Get("Retry-After"), time.Now()),
	}
	return response, resultErr
}

func defaultRESTHTTPClient() *http.Client {
	defaultRESTClientOnce.Do(func() {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
		transport.TLSHandshakeTimeout = 10 * time.Second
		transport.ResponseHeaderTimeout = 15 * time.Second
		transport.ExpectContinueTimeout = time.Second
		defaultRESTClient = &http.Client{Transport: transport, Timeout: defaultRESTTimeout}
	})
	return defaultRESTClient
}

var (
	defaultRESTClientOnce sync.Once
	defaultRESTClient     *http.Client
)

func readBoundedResponse(reader io.Reader, limit int64) ([]byte, error) {
	limited := io.LimitReader(reader, limit+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return body, fmt.Errorf("read REST response: %w", err)
	}
	if int64(len(body)) > limit {
		return body[:limit], fmt.Errorf("REST response body exceeds %d bytes", limit)
	}
	return body, nil
}

func requestID(header http.Header) string {
	for _, name := range []string{"X-Request-Id", "X-Request-ID", "Request-Id", "Request-ID"} {
		if value := header.Get(name); value != "" {
			return value
		}
	}
	return ""
}

func serviceCodeFromResponse(body []byte) string {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	for _, key := range []string{"error_code", "code", "error"} {
		if value, ok := payload[key]; ok {
			switch typed := value.(type) {
			case string:
				return typed
			case json.Number:
				return typed.String()
			case float64:
				return strconv.FormatFloat(typed, 'f', -1, 64)
			}
		}
	}
	return ""
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

// These small adapters keep REST policy centralized while allowing Client token
// synchronization and TelemetryEvent shape to remain owned by client.go.
func (c *Client) restHTTPClient() *http.Client {
	if c.cfg.HTTPClient != nil {
		return c.cfg.HTTPClient
	}
	return defaultRESTHTTPClient()
}

func (c *Client) recordRESTTelemetry(ctx context.Context, operation string, attempt int, duration time.Duration,
	statusCode int, serviceCode string, err error) {
	if c.cfg.Telemetry == nil {
		return
	}
	c.cfg.Telemetry.Record(ctx, TelemetryEvent{
		Operation: operation, Attempt: attempt, Duration: duration, StatusCode: statusCode,
		ServiceCode: serviceCode, Error: errorString(err),
	})
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func responseErrorFields(response *Response, err error) (int, string) {
	statusCode := 0
	if response != nil {
		statusCode = response.StatusCode
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return statusCode, apiErr.ServiceCode
	}
	return statusCode, ""
}
