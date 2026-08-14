package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type telemetryRecorder struct {
	mu     sync.Mutex
	events []TelemetryEvent
}

func (r *telemetryRecorder) Record(_ context.Context, event TelemetryEvent) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *telemetryRecorder) snapshot() []TelemetryEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TelemetryEvent(nil), r.events...)
}

func restTestClient(server *httptest.Server, telemetry Telemetry) *Client {
	return &Client{cfg: Config{
		RestBase: server.URL + "/org/app", UserID: "owner/name", Resource: "server resource",
		HTTPClient: server.Client(), Telemetry: telemetry,
	}, token: "secret-token"}
}

func TestUpdateOwnUserInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s", r.Method)
		}
		if r.URL.EscapedPath() != "/org/app/metadata/user/owner%2Fname" {
			t.Errorf("escaped path = %q", r.URL.EscapedPath())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Errorf("Content-Type = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("display name") != "A&B" || r.Form.Get("city") != "深圳" {
			t.Errorf("form = %#v", r.Form)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	response, err := restTestClient(server, nil).UpdateOwnUserInfo(context.Background(), map[string]string{
		"display name": "A&B", "city": "深圳",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestFetchUserInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/org/app/metadata/user/get" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Targets    []string `json:"targets"`
			Properties []string `json:"properties"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Join(body.Targets, ",") != "alice,bob" || strings.Join(body.Properties, ",") != "avatar,nickname" {
			t.Errorf("body = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{}}`)
	}))
	defer server.Close()

	response, err := restTestClient(server, nil).FetchUserInfo(context.Background(),
		[]string{"alice", "bob"}, []string{"avatar", "nickname"})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Body) != `{"data":{}}` {
		t.Fatalf("body = %s", response.Body)
	}
}

func TestCreatePublicGroupFixedPolicyAndDefaults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/org/app/chatgroups" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("version") != "v3" || r.URL.Query().Get("resource") != "server resource" {
			t.Errorf("query = %v", r.URL.Query())
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		want := map[string]any{
			"name": "support", "owner": "owner/name", "public": true, "memberonly": false,
			"allowinvites": true, "invite_need_confirm": false, "maxusers": float64(800),
		}
		for key, value := range want {
			if body[key] != value {
				t.Errorf("body[%q] = %#v, want %#v", key, body[key], value)
			}
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	_, err := restTestClient(server, nil).CreatePublicGroup(context.Background(), "support",
		CreatePublicGroupOptions{MaxUsers: 800})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJoinAndLeaveGroupEscapePathAndReportTelemetry(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		if r.URL.EscapedPath() != "/org/app/chatgroups/group%2Fwith%20space/"+map[string]string{
			http.MethodPost: "apply", http.MethodDelete: "quit",
		}[r.Method] {
			t.Errorf("escaped path = %q", r.URL.EscapedPath())
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	recorder := &telemetryRecorder{}
	client := restTestClient(server, recorder)

	if _, err := client.JoinPublicGroup(context.Background(), "group/with space"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.LeaveGroup(context.Background(), "group/with space"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(methods, ",") != "POST,DELETE" {
		t.Fatalf("methods = %v", methods)
	}
	events := recorder.snapshot()
	if len(events) != 3 {
		t.Fatalf("telemetry events = %#v", events)
	}
	if events[0].Operation != "join_public_group_request" || events[1].Operation != "join_public_group" ||
		events[2].Operation != "leave_group" {
		t.Fatalf("telemetry operations = %#v", events)
	}
	for _, event := range events {
		if event.Attempt != 1 || event.StatusCode != http.StatusOK || event.Error != "" || event.Duration <= 0 {
			t.Errorf("telemetry event = %#v", event)
		}
	}
}

func TestRESTAPIErrorMapsStatusAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.Header().Set("X-Request-ID", "request-123")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"message":"slow down"}`)
	}))
	defer server.Close()
	recorder := &telemetryRecorder{}

	response, err := restTestClient(server, recorder).UpdateOwnUserInfo(context.Background(), map[string]string{"a": "b"})
	if response == nil || response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("response = %#v", response)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %T %v", err, err)
	}
	if apiErr.Response != response || apiErr.ServiceCode != "EXCEED_SERVICE_LIMIT" ||
		apiErr.RequestID != "request-123" || apiErr.RetryAfter != 17*time.Second {
		t.Fatalf("APIError = %#v", apiErr)
	}
	events := recorder.snapshot()
	if len(events) != 1 || events[0].ServiceCode != "EXCEED_SERVICE_LIMIT" ||
		events[0].StatusCode != http.StatusTooManyRequests || events[0].Error == "" {
		t.Fatalf("telemetry = %#v", events)
	}
}

func TestRESTAPIErrorUsesServiceResponseCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error_code":"GROUP_ALREADY_JOINED"}`)
	}))
	defer server.Close()

	_, err := restTestClient(server, nil).JoinPublicGroup(context.Background(), "group")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.ServiceCode != "GROUP_ALREADY_JOINED" {
		t.Fatalf("error = %#v", err)
	}
}

func TestRESTResponseBodyLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("x", int(defaultRESTBodyMaxBytes)+1))
	}))
	defer server.Close()

	response, err := restTestClient(server, nil).LeaveGroup(context.Background(), "group")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %#v", err)
	}
	if response == nil || int64(len(response.Body)) != defaultRESTBodyMaxBytes || apiErr.Response != response {
		t.Fatalf("response body length = %d", len(response.Body))
	}
}

func TestRESTContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	response, err := restTestClient(server, nil).LeaveGroup(ctx, "group")
	if response != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("response=%#v error=%v", response, err)
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	got := parseRetryAfter(now.Add(9*time.Second).Format(http.TimeFormat), now)
	if got != 9*time.Second {
		t.Fatalf("Retry-After = %s", got)
	}
	for _, value := range []string{"", "-2", "invalid", now.Add(-time.Second).Format(http.TimeFormat)} {
		if got := parseRetryAfter(value, now); got != 0 {
			t.Errorf("parseRetryAfter(%q) = %s", value, got)
		}
	}
}

func TestResourceQueryIsEncoded(t *testing.T) {
	client := &Client{cfg: Config{Resource: "a&b=c"}}
	query, err := url.ParseQuery(client.resourceQuery())
	if err != nil || query.Get("resource") != "a&b=c" || query.Get("version") != "v3" {
		t.Fatalf("query=%v err=%v", query, err)
	}
}
