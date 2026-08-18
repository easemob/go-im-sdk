package sdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestEndpointRoundClonesAndVisitsCircularlyOnce(t *testing.T) {
	candidates := []string{"a", "b", "c"}
	round := newEndpointRound(candidates, 1)
	candidates[1] = "mutated"

	var endpoints []string
	var indexes []int
	for {
		endpoint, index, ok := round.nextEndpoint()
		if !ok {
			break
		}
		endpoints = append(endpoints, endpoint)
		indexes = append(indexes, index)
	}
	if !reflect.DeepEqual(endpoints, []string{"b", "c", "a"}) {
		t.Fatalf("endpoints = %#v", endpoints)
	}
	if !reflect.DeepEqual(indexes, []int{1, 2, 0}) {
		t.Fatalf("indexes = %#v", indexes)
	}
	if !round.exhausted() {
		t.Fatal("round must be explicitly exhausted")
	}
	if endpoint, index, ok := round.nextEndpoint(); ok || endpoint != "" || index != 0 {
		t.Fatalf("post-exhaustion next = %q, %d, %v", endpoint, index, ok)
	}
}

func TestEndpointRoundNormalizesCursorAndHandlesEmpty(t *testing.T) {
	tests := []struct {
		name  string
		start int
		want  string
	}{
		{name: "large positive", start: 4, want: "b"},
		{name: "negative", start: -1, want: "c"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			round := newEndpointRound([]string{"a", "b", "c"}, tc.start)
			got, _, ok := round.nextEndpoint()
			if !ok || got != tc.want {
				t.Fatalf("next = %q, %v; want %q", got, ok, tc.want)
			}
		})
	}
	empty := newEndpointRound(nil, 99)
	if !empty.exhausted() {
		t.Fatal("empty round must start exhausted")
	}
}

func TestShouldRotateEndpointOnlyForEndpointLocalErrors(t *testing.T) {
	rotate := []ErrorCode{ErrIO, ErrTimeout, ErrTLSFailed, ErrStreamClosed, ErrDNS, ErrHandshake}
	for _, code := range rotate {
		if !shouldRotateEndpoint(newError(code, "test", "")) {
			t.Errorf("%s should rotate", code)
		}
	}
	doNotRotate := []ErrorCode{
		ErrHandlerBacklog, ErrWriteBackpressure, ErrProtocol, ErrRedirectLoop,
		ErrRedirectLimit, ErrTokenExpired, ErrInvalidToken, ErrPermissionDenied,
	}
	for _, code := range doNotRotate {
		if shouldRotateEndpoint(newError(code, "test", "")) {
			t.Errorf("%s must not rotate", code)
		}
	}
	if shouldRotateEndpoint(nil) || shouldRotateEndpoint(errors.New("unclassified")) {
		t.Fatal("nil and unclassified errors must not rotate")
	}
	wrapped := &SDKError{Code: ErrTimeout, Operation: "outer", Cause: errors.New("timeout")}
	if !shouldRotateEndpoint(wrapped) {
		t.Fatal("SDK timeout should rotate")
	}
}

func TestConnectLoginCandidatesRotatesAndPreservesEffectiveEndpoint(t *testing.T) {
	client := &Client{}
	var attempts []string
	client.connectEndpointFn = func(_ context.Context, endpoint string) (*connectionRun, error) {
		attempts = append(attempts, endpoint)
		if len(attempts) == 1 {
			return nil, newError(ErrHandshake, "test dial", "first unavailable")
		}
		return &connectionRun{endpoint: "wss://redirect-effective.example/websocket"}, nil
	}
	run, index, err := client.connectLoginCandidates(context.Background(), []string{
		"wss://first.example/websocket",
		"wss://second.example/websocket",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantAttempts := []string{"wss://first.example/websocket", "wss://second.example/websocket"}
	if !reflect.DeepEqual(attempts, wantAttempts) || index != 1 {
		t.Fatalf("attempts=%#v index=%d", attempts, index)
	}
	if run.endpoint != "wss://redirect-effective.example/websocket" {
		t.Fatalf("effective endpoint = %q", run.endpoint)
	}
	if cursor := nextCandidateCursor(index, len(wantAttempts)); cursor != 0 {
		t.Fatalf("next cursor = %d", cursor)
	}
}

func TestConnectLoginCandidatesDoesNotRotateProtocolFailure(t *testing.T) {
	client := &Client{}
	var attempts atomic.Int32
	client.connectEndpointFn = func(context.Context, string) (*connectionRun, error) {
		attempts.Add(1)
		return nil, newError(ErrProtocol, "provision", "invalid")
	}
	_, index, err := client.connectLoginCandidates(context.Background(), []string{"first", "second"})
	if errorCode(err) != ErrProtocol || index != 0 || attempts.Load() != 1 {
		t.Fatalf("error=%v index=%d attempts=%d", err, index, attempts.Load())
	}
}

func TestResolveCachedEndpointCandidatesUsesExactHTTPClientIdentity(t *testing.T) {
	withTestSharedDNSResolver(t, time.Now)
	var requests atomic.Int32
	httpClientA := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return dnsResponse(http.StatusOK, validDNSDocument), nil
	})}
	clientA1 := &Client{cfg: Config{AppKey: "org#app", HTTPClient: httpClientA}}
	clientA2 := &Client{cfg: Config{AppKey: "org#app", HTTPClient: httpClientA}}

	first, err := clientA1.resolveCachedEndpointCandidates(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := clientA2.resolveCachedEndpointCandidates(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || first.Generation != second.Generation {
		t.Fatalf("same identity requests=%d generations=%d/%d", requests.Load(), first.Generation, second.Generation)
	}

	httpClientB := &http.Client{Transport: httpClientA.Transport}
	clientB := &Client{cfg: Config{AppKey: "org#app", HTTPClient: httpClientB}}
	if _, err = clientB.resolveCachedEndpointCandidates(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatalf("different *http.Client identity requests = %d, want 2", requests.Load())
	}
	clientA1.mu.RLock()
	pending := len(clientA1.pendingDNSFlights)
	clientA1.mu.RUnlock()
	if pending != 0 {
		t.Fatalf("successful settled lookup retained %d pending flights", pending)
	}
}

func TestResolveCachedEndpointCandidatesGenerationForcesOneRefresh(t *testing.T) {
	withTestSharedDNSResolver(t, time.Now)
	var requests atomic.Int32
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return dnsResponse(http.StatusOK, validDNSDocument), nil
	})}
	client := &Client{cfg: Config{AppKey: "org#app", HTTPClient: httpClient}}
	first, err := client.resolveCachedEndpointCandidates(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	refreshed, err := client.resolveCachedEndpointCandidates(context.Background(), first.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Generation <= first.Generation || requests.Load() != 2 {
		t.Fatalf("generations=%d/%d requests=%d", first.Generation, refreshed.Generation, requests.Load())
	}
	adopted, err := client.resolveCachedEndpointCandidates(context.Background(), first.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if adopted.Generation != refreshed.Generation || requests.Load() != 2 {
		t.Fatalf("adopted=%d refreshed=%d requests=%d", adopted.Generation, refreshed.Generation, requests.Load())
	}
}

func TestResolveCachedEndpointCandidatesUsesStaleOnlyForTransientDNSFailure(t *testing.T) {
	tests := []struct {
		name        string
		failure     func() (*http.Response, error)
		wantStale   bool
		wantRetries int32
	}{
		{
			name: "transient status",
			failure: func() (*http.Response, error) {
				return dnsResponse(http.StatusBadGateway, "temporary"), nil
			},
			wantStale: true, wantRetries: dnsMaxAttempts,
		},
		{
			name: "permanent invalid payload",
			failure: func() (*http.Response, error) {
				return dnsResponse(http.StatusOK, `{`), nil
			},
			wantStale: false, wantRetries: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			withTestSharedDNSResolver(t, func() time.Time { return now })
			var requests atomic.Int32
			httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				if requests.Add(1) == 1 {
					return dnsResponse(http.StatusOK, validDNSDocument), nil
				}
				return tc.failure()
			})}
			client := &Client{cfg: Config{AppKey: "org#app", HTTPClient: httpClient}}
			first, err := client.resolveCachedEndpointCandidates(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(dnsFreshTTL + time.Second)
			result, err := client.resolveCachedEndpointCandidates(context.Background(), first.Generation)
			if tc.wantStale {
				if err != nil || !result.Stale || result.Generation != first.Generation {
					t.Fatalf("result=%#v err=%v", result, err)
				}
			} else if err == nil || result.Stale {
				t.Fatalf("permanent failure result=%#v err=%v", result, err)
			}
			if got, want := requests.Load(), 1+tc.wantRetries; got != want {
				t.Fatalf("requests=%d want=%d", got, want)
			}
		})
	}
}

func TestTransientDNSResolutionClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "transport", err: dnsStageError("request failed", 0, errors.New("round trip failed")), want: true},
		{name: "read", err: dnsStageError("read response", 0, io.ErrUnexpectedEOF), want: true},
		{name: "rate limited", err: dnsStageError("unexpected HTTP status", http.StatusTooManyRequests, nil), want: true},
		{name: "server error", err: dnsStageError("unexpected HTTP status", http.StatusServiceUnavailable, nil), want: true},
		{name: "bad request", err: dnsStageError("unexpected HTTP status", http.StatusBadRequest, nil), want: false},
		{name: "invalid JSON", err: dnsStageError("invalid JSON response", 0, errors.New("syntax")), want: false},
		{name: "not DNS", err: newError(ErrIO, "test", ""), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientDNSResolutionError(tc.err); got != tc.want {
				t.Fatalf("got %v, want %v for %v", got, tc.want, tc.err)
			}
		})
	}
}

func withTestSharedDNSResolver(t *testing.T, now func() time.Time) {
	t.Helper()
	previous := sharedDNSResolver
	sharedDNSResolver = newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, now)
	t.Cleanup(func() { sharedDNSResolver = previous })
}
