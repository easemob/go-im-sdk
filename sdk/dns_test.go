package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func dnsResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

const validDNSDocument = `{
  "msync-wx":{"hosts":[
    {"protocol":"wss","port":"8443","domain":"fallback.example","priority":"2"},
    {"protocol":"https","port":"443","domain":"preferred-wss.example","priority":"1"}
  ]},
  "rest":{"hosts":[
    {"protocol":"http","port":"80","domain":"insecure.example","priority":"1"},
    {"protocol":"https","port":443,"domain":"preferred-rest.example","priority":1}
  ]}
}`

func TestResolveLoginEndpointsUsesFixedDNSRequestAndPriorityHosts(t *testing.T) {
	var requests atomic.Int32
	client := &Client{cfg: Config{
		AppKey: "org#app",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests.Add(1)
			if req.URL.Scheme != "https" || req.URL.Host != "rs.easemob.com" || req.URL.Path != "/easemob/server.json" {
				t.Errorf("DNS URL = %s", req.URL)
			}
			query := req.URL.Query()
			if query.Get("sdk_version") != sdkVersion || query.Get("app_key") != "org#app" || query.Get("file_version") != "1" {
				t.Errorf("DNS query = %v", query)
			}
			return dnsResponse(http.StatusOK, validDNSDocument), nil
		})},
	}}
	wss, rest, err := client.resolveLoginEndpoints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d", requests.Load())
	}
	if wss != "wss://preferred-wss.example/websocket" {
		t.Fatalf("WSS = %q", wss)
	}
	if rest != "https://preferred-rest.example/org/app" {
		t.Fatalf("REST = %q", rest)
	}
}

func TestResolveLoginEndpointsRetriesTransientFailure(t *testing.T) {
	var requests atomic.Int32
	client := &Client{cfg: Config{
		AppKey: "org#app",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempt := requests.Add(1)
			if attempt < dnsMaxAttempts {
				return dnsResponse(http.StatusBadGateway, "temporary"), nil
			}
			return dnsResponse(http.StatusOK, validDNSDocument), nil
		})},
	}}
	if _, _, err := client.resolveLoginEndpoints(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != dnsMaxAttempts {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestResolveLoginEndpointsDoesNotRetryPermanentHTTPFailure(t *testing.T) {
	var requests atomic.Int32
	client := &Client{cfg: Config{
		AppKey: "org#app",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return dnsResponse(http.StatusBadRequest, "bad request"), nil
		})},
	}}
	_, _, err := client.resolveLoginEndpoints(context.Background())
	var sdkErr *SDKError
	if !errors.As(err, &sdkErr) || sdkErr.Code != ErrDNS || sdkErr.Operation != "dns bootstrap" || sdkErr.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("error = %#v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestResolveDNSPayloadRejectsInvalidResponses(t *testing.T) {
	tests := map[string]string{
		"invalid JSON":   `{`,
		"missing WSS":    `{"msync-wx":{"hosts":[]},"rest":{"hosts":[{"protocol":"https","domain":"rest.example"}]}}`,
		"missing REST":   `{"msync-wx":{"hosts":[{"protocol":"https","domain":"wss.example"}]},"rest":{"hosts":[]}}`,
		"insecure WSS":   `{"msync-wx":{"hosts":[{"protocol":"http","domain":"wss.example"}]},"rest":{"hosts":[{"protocol":"https","domain":"rest.example"}]}}`,
		"insecure REST":  `{"msync-wx":{"hosts":[{"protocol":"wss","domain":"wss.example"}]},"rest":{"hosts":[{"protocol":"http","domain":"rest.example"}]}}`,
		"malformed host": `{"msync-wx":{"hosts":[{"protocol":"wss","domain":"bad%2fhost.example"}]},"rest":{"hosts":[{"protocol":"https","domain":"rest.example"}]}}`,
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := resolveDNSPayload([]byte(document), "org#app")
			if errorCode(err) != ErrDNS || !strings.Contains(err.Error(), "dns bootstrap") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestReadBoundedDNSResponseRejectsOversizedBody(t *testing.T) {
	_, err := readBoundedDNSResponse(strings.NewReader(strings.Repeat("x", int(dnsResponseMaxBytes)+1)))
	if errorCode(err) != ErrDNS || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v", err)
	}
}

func TestResolveDNSPayloadFallsBackToFirstValidHost(t *testing.T) {
	document := `{
      "msync-wx":{"hosts":[{"protocol":"https","port":"8443","domain":"first-wss.example","priority":"9"}]},
      "rest":{"hosts":[{"protocol":"https","port":"8443","domain":"first-rest.example","priority":"9"}]}
    }`
	wss, rest, err := resolveDNSPayload([]byte(document), "org#app")
	if err != nil {
		t.Fatal(err)
	}
	if wss != "wss://first-wss.example:8443/websocket" || rest != "https://first-rest.example:8443/org/app" {
		t.Fatalf("WSS=%q REST=%q", wss, rest)
	}
}

func TestResolveDNSCandidatesReturnsStablePriorityOrderAndDeduplicates(t *testing.T) {
	document := `{
      "msync-wx":{"hosts":[
        {"protocol":"wss","domain":"fallback-a.example","priority":2},
        {"protocol":"https","domain":"preferred-a.example","priority":1},
        {"protocol":"wss","domain":"fallback-a.example","priority":1},
        {"protocol":"http","domain":"invalid.example","priority":1},
        {"protocol":"wss","port":8443,"domain":"preferred-b.example","priority":1},
        {"protocol":"wss","domain":"fallback-b.example","priority":9},
        {"protocol":"wss","domain":"preferred-a.example","priority":9}
      ]},
      "rest":{"hosts":[
        {"protocol":"https","domain":"rest-fallback.example","priority":2},
        {"protocol":"https","domain":"rest-preferred.example","priority":1},
        {"protocol":"https","domain":"rest-unused.example","priority":1}
      ]}
    }`
	resolved, err := resolveDNSCandidates([]byte(document), "org#app")
	if err != nil {
		t.Fatal(err)
	}
	wantWSS := []string{
		"wss://preferred-a.example/websocket",
		"wss://fallback-a.example/websocket",
		"wss://preferred-b.example:8443/websocket",
		"wss://fallback-b.example/websocket",
	}
	if !reflect.DeepEqual(resolved.WSS, wantWSS) {
		t.Fatalf("WSS = %#v, want %#v", resolved.WSS, wantWSS)
	}
	if resolved.REST != "https://rest-preferred.example/org/app" {
		t.Fatalf("REST = %q", resolved.REST)
	}
	wss, rest, err := resolveDNSPayload([]byte(document), "org#app")
	if err != nil || wss != wantWSS[0] || rest != resolved.REST {
		t.Fatalf("compatibility wrapper: WSS=%q REST=%q err=%v", wss, rest, err)
	}
}

func TestResolveDNSCandidatesRejectsMoreThanMaximumWSSHosts(t *testing.T) {
	var hosts strings.Builder
	for i := 0; i <= dnsMaxWSSCandidates; i++ {
		if i > 0 {
			hosts.WriteByte(',')
		}
		fmt.Fprintf(&hosts, `{"protocol":"wss","domain":"host-%d.example"}`, i)
	}
	document := `{"msync-wx":{"hosts":[` + hosts.String() + `]},"rest":{"hosts":[{"protocol":"https","domain":"rest.example"}]}}`
	_, err := resolveDNSCandidates([]byte(document), "org#app")
	if errorCode(err) != ErrDNS || !strings.Contains(err.Error(), fmt.Sprint(dnsMaxWSSCandidates)) {
		t.Fatalf("error = %v", err)
	}
}

func TestDNSResolverCoalescesConcurrentLookups(t *testing.T) {
	resolver := newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, time.Now)
	key := dnsCacheKey{AppKey: "org#app", HTTPClient: &http.Client{}}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context) (loginEndpoints, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return testLoginEndpoints("one.example"), false, nil
	}

	const callers = 64
	var ready sync.WaitGroup
	ready.Add(callers)
	var done sync.WaitGroup
	done.Add(callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			result, err := resolver.resolve(context.Background(), key, 0, load)
			if err == nil && (result.Generation != 1 || result.Stale || len(result.Endpoints.WSS) != 1) {
				err = fmt.Errorf("unexpected result: %#v", result)
			}
			errs <- err
		}()
	}
	ready.Wait()
	<-started
	close(release)
	done.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("load calls = %d, want 1", calls.Load())
	}
}

func TestDNSResolverKeyIncludesHTTPClientIdentity(t *testing.T) {
	resolver := newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, time.Now)
	var calls atomic.Int32
	load := func(context.Context) (loginEndpoints, bool, error) {
		calls.Add(1)
		return testLoginEndpoints("one.example"), false, nil
	}
	clientA := &http.Client{}
	clientB := &http.Client{}
	keys := []dnsCacheKey{
		{AppKey: "org#app", HTTPClient: clientA},
		{AppKey: "org#app", HTTPClient: clientB},
		{AppKey: "other#app", HTTPClient: clientA},
	}
	for _, key := range keys {
		if _, err := resolver.resolve(context.Background(), key, 0, load); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != int32(len(keys)) {
		t.Fatalf("load calls = %d, want %d", calls.Load(), len(keys))
	}
}

func TestDNSResolverWaiterCancellationDoesNotCancelSharedLoad(t *testing.T) {
	resolver := newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, time.Now)
	key := dnsCacheKey{AppKey: "org#app", HTTPClient: &http.Client{}}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context) (loginEndpoints, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return testLoginEndpoints("one.example"), false, nil
	}
	backgroundResult := make(chan error, 1)
	go func() {
		_, err := resolver.resolve(context.Background(), key, 0, load)
		backgroundResult <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := resolver.resolve(ctx, key, 0, load); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled waiter error = %v", err)
	}
	close(release)
	if err := <-backgroundResult; err != nil {
		t.Fatalf("shared load failed after other waiter canceled: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("load calls = %d, want 1", calls.Load())
	}
}

func TestDNSResolverFreshStaleCooldownAndGeneration(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	resolver := newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, func() time.Time { return now })
	key := dnsCacheKey{AppKey: "org#app", HTTPClient: &http.Client{}}
	var calls atomic.Int32
	load := func(context.Context) (loginEndpoints, bool, error) {
		switch calls.Add(1) {
		case 1:
			return testLoginEndpoints("one.example"), false, nil
		case 2:
			return loginEndpoints{}, true, errors.New("temporary DNS outage")
		default:
			return testLoginEndpoints("two.example"), false, nil
		}
	}

	first, err := resolver.resolve(context.Background(), key, 0, load)
	if err != nil || first.Generation != 1 || first.Stale {
		t.Fatalf("first = %#v, err=%v", first, err)
	}
	first.Endpoints.WSS[0] = "mutated"
	fresh, err := resolver.resolve(context.Background(), key, 0, load)
	if err != nil || fresh.Generation != 1 || fresh.Stale || fresh.Endpoints.WSS[0] != "wss://one.example/websocket" {
		t.Fatalf("fresh = %#v, err=%v", fresh, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("fresh cache called loader %d times", calls.Load())
	}

	now = now.Add(dnsFreshTTL + time.Second)
	stale, err := resolver.resolve(context.Background(), key, first.Generation, load)
	if err != nil || !stale.Stale || stale.Generation != first.Generation {
		t.Fatalf("stale = %#v, err=%v", stale, err)
	}
	now = now.Add(time.Second)
	cooled, err := resolver.resolve(context.Background(), key, first.Generation, load)
	if err != nil || !cooled.Stale || calls.Load() != 2 {
		t.Fatalf("cooldown result=%#v calls=%d err=%v", cooled, calls.Load(), err)
	}

	now = now.Add(dnsFailureCooldown)
	refreshed, err := resolver.resolve(context.Background(), key, first.Generation, load)
	if err != nil || refreshed.Stale || refreshed.Generation <= first.Generation ||
		refreshed.Endpoints.WSS[0] != "wss://two.example/websocket" {
		t.Fatalf("refreshed = %#v, err=%v", refreshed, err)
	}
	adopted, err := resolver.resolve(context.Background(), key, first.Generation, load)
	if err != nil || adopted.Generation != refreshed.Generation || calls.Load() != 3 {
		t.Fatalf("adopted=%#v calls=%d err=%v", adopted, calls.Load(), err)
	}
}

func TestDNSResolverDoesNotUseStaleForPermanentFailureOrPastStaleTTL(t *testing.T) {
	tests := []struct {
		name      string
		advance   time.Duration
		transient bool
	}{
		{name: "permanent", advance: dnsFreshTTL + time.Second, transient: false},
		{name: "expired stale", advance: dnsStaleTTL + time.Second, transient: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			resolver := newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, func() time.Time { return now })
			key := dnsCacheKey{AppKey: "org#app", HTTPClient: &http.Client{}}
			var calls atomic.Int32
			load := func(context.Context) (loginEndpoints, bool, error) {
				if calls.Add(1) == 1 {
					return testLoginEndpoints("one.example"), false, nil
				}
				return loginEndpoints{}, tc.transient, errors.New("refresh failed")
			}
			first, err := resolver.resolve(context.Background(), key, 0, load)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(tc.advance)
			if _, err = resolver.resolve(context.Background(), key, first.Generation, load); err == nil || err.Error() != "refresh failed" {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestDNSResolverLRUCapacity(t *testing.T) {
	resolver := newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, time.Now)
	clients := make([]*http.Client, dnsResolverCapacity+1)
	for i := range clients {
		clients[i] = &http.Client{}
		key := dnsCacheKey{AppKey: fmt.Sprintf("org#app-%d", i), HTTPClient: clients[i]}
		if _, err := resolver.resolve(context.Background(), key, 0, func(context.Context) (loginEndpoints, bool, error) {
			return testLoginEndpoints("one.example"), false, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.entries) != dnsResolverCapacity || resolver.lru.Len() != dnsResolverCapacity {
		t.Fatalf("entries=%d lru=%d", len(resolver.entries), resolver.lru.Len())
	}
	first := dnsCacheKey{AppKey: "org#app-0", HTTPClient: clients[0]}
	last := dnsCacheKey{AppKey: fmt.Sprintf("org#app-%d", dnsResolverCapacity), HTTPClient: clients[dnsResolverCapacity]}
	if _, exists := resolver.entries[first]; exists {
		t.Fatal("oldest completed entry was not evicted")
	}
	if _, exists := resolver.entries[last]; !exists {
		t.Fatal("newest entry is missing")
	}
}

func TestDNSResolverFullInFlightCacheUsesBoundedUncachedLoad(t *testing.T) {
	resolver := newDNSResolver(2, dnsFreshTTL, dnsStaleTTL, time.Now)
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	results := make(chan error, 2)
	clients := []*http.Client{{}, {}}
	for i := range clients {
		key := dnsCacheKey{AppKey: fmt.Sprintf("org#blocked-%d", i), HTTPClient: clients[i]}
		go func() {
			_, err := resolver.resolve(context.Background(), key, 0, func(context.Context) (loginEndpoints, bool, error) {
				started <- struct{}{}
				<-release
				return testLoginEndpoints("blocked.example"), false, nil
			})
			results <- err
		}()
	}
	<-started
	<-started
	overflowCalls := atomic.Int32{}
	third, err := resolver.resolve(context.Background(), dnsCacheKey{AppKey: "org#overflow", HTTPClient: &http.Client{}}, 0,
		func(context.Context) (loginEndpoints, bool, error) {
			overflowCalls.Add(1)
			return testLoginEndpoints("overflow.example"), false, nil
		})
	if err != nil || overflowCalls.Load() != 1 || third.Endpoints.WSS[0] != "wss://overflow.example/websocket" {
		t.Fatalf("overflow result=%#v calls=%d err=%v", third, overflowCalls.Load(), err)
	}
	resolver.mu.Lock()
	entryCount := len(resolver.entries)
	resolver.mu.Unlock()
	if entryCount != 2 {
		t.Fatalf("cache grew to %d entries, want strict capacity 2", entryCount)
	}
	close(release)
	for range clients {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func testLoginEndpoints(host string) loginEndpoints {
	return loginEndpoints{
		WSS:  []string{"wss://" + host + "/websocket"},
		REST: "https://rest.example/org/app",
	}
}
