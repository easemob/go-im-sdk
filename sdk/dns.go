package sdk

import (
	"container/list"
	"context"
	"encoding/json"
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
	dnsBootstrapURL       = "https://rs.easemob.com/easemob/server.json"
	dnsFileVersion        = "1"
	dnsMaxAttempts        = 2
	dnsResponseMaxBytes   = int64(1 << 20)
	dnsRetryInitialDelay  = 100 * time.Millisecond
	dnsMaxWSSCandidates   = 64
	dnsResolverCapacity   = 256
	dnsResolverOverflow   = 8
	dnsFreshTTL           = 5 * time.Minute
	dnsStaleTTL           = 30 * time.Minute
	dnsFailureCooldown    = 5 * time.Second
	dnsMaxFailureCooldown = 2 * time.Minute
)

// loginEndpoints keeps the complete, ordered WSS candidate set returned by
// bootstrap DNS. REST deliberately remains a single endpoint: MSync failover
// must not silently broaden into a separate REST failover policy.
type loginEndpoints struct {
	WSS  []string
	REST string
}

type dnsDocument struct {
	MsyncWX dnsService `json:"msync-wx"`
	REST    dnsService `json:"rest"`
}

type dnsService struct {
	Hosts []dnsHost `json:"hosts"`
}

type dnsHost struct {
	Protocol string         `json:"protocol"`
	Port     flexibleString `json:"port"`
	Domain   string         `json:"domain"`
	Priority flexibleString `json:"priority"`
}

type flexibleString string

func (s *flexibleString) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*s = flexibleString(text)
		return nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil {
		return fmt.Errorf("must be a string or number")
	}
	*s = flexibleString(number.String())
	return nil
}

func (c *Client) resolveLoginEndpoints(ctx context.Context) (string, string, error) {
	resolved, err := c.resolveLoginEndpointCandidates(ctx)
	if err != nil {
		return "", "", err
	}
	return resolved.WSS[0], resolved.REST, nil
}

func (c *Client) resolveLoginEndpointCandidates(ctx context.Context) (loginEndpoints, error) {
	endpoint, err := url.Parse(dnsBootstrapURL)
	if err != nil {
		return loginEndpoints{}, dnsStageError("invalid SDK bootstrap URL", 0, err)
	}
	query := endpoint.Query()
	query.Set("sdk_version", sdkVersion)
	query.Set("app_key", c.cfg.AppKey)
	query.Set("file_version", dnsFileVersion)
	endpoint.RawQuery = query.Encode()

	var lastErr error
	for attempt := 1; attempt <= dnsMaxAttempts; attempt++ {
		resolved, retry, err := c.fetchLoginEndpointCandidates(ctx, endpoint.String())
		if err == nil {
			if c.debug {
				c.logger.Debug("dns.resolved", "attempt", attempt, "wss", resolved.WSS[0],
					"wss_candidates", len(resolved.WSS), "rest_base", resolved.REST)
			}
			return resolved, nil
		}
		lastErr = err
		if !retry || attempt == dnsMaxAttempts {
			break
		}
		timer := time.NewTimer(dnsRetryInitialDelay * time.Duration(1<<(attempt-1)))
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return loginEndpoints{}, dnsStageError("request canceled while retrying", 0, ctx.Err())
		}
	}
	return loginEndpoints{}, lastErr
}

func (c *Client) fetchLoginEndpoints(ctx context.Context, endpoint string) (string, string, bool, error) {
	resolved, retry, err := c.fetchLoginEndpointCandidates(ctx, endpoint)
	if err != nil {
		return "", "", retry, err
	}
	return resolved.WSS[0], resolved.REST, false, nil
}

func (c *Client) fetchLoginEndpointCandidates(ctx context.Context, endpoint string) (loginEndpoints, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return loginEndpoints{}, false, dnsStageError("create request", 0, err)
	}
	req.Header.Set("Accept", "application/json")
	httpClient := *c.restHTTPClient()
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := httpClient.Do(req)
	if err != nil {
		return loginEndpoints{}, true, dnsStageError("request failed", 0, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		retry := response.StatusCode >= http.StatusInternalServerError || response.StatusCode == http.StatusTooManyRequests
		return loginEndpoints{}, retry, dnsStageError("unexpected HTTP status", response.StatusCode, nil)
	}
	body, err := readBoundedDNSResponse(response.Body)
	if err != nil {
		return loginEndpoints{}, false, err
	}
	resolved, err := resolveDNSCandidates(body, c.cfg.AppKey)
	return resolved, false, err
}

func readBoundedDNSResponse(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, dnsResponseMaxBytes+1))
	if err != nil {
		return nil, dnsStageError("read response", 0, err)
	}
	if int64(len(body)) > dnsResponseMaxBytes {
		return nil, dnsStageError(fmt.Sprintf("response exceeds %d bytes", dnsResponseMaxBytes), 0, nil)
	}
	return body, nil
}

func resolveDNSPayload(body []byte, appKey string) (string, string, error) {
	resolved, err := resolveDNSCandidates(body, appKey)
	if err != nil {
		return "", "", err
	}
	return resolved.WSS[0], resolved.REST, nil
}

func resolveDNSCandidates(body []byte, appKey string) (loginEndpoints, error) {
	var document dnsDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return loginEndpoints{}, dnsStageError("invalid JSON response", 0, err)
	}
	wss := selectDNSHosts(document.MsyncWX.Hosts, true)
	if len(wss) == 0 {
		return loginEndpoints{}, dnsStageError("response has no valid WSS host in msync-wx.hosts", 0, nil)
	}
	if len(wss) > dnsMaxWSSCandidates {
		return loginEndpoints{}, dnsStageError(fmt.Sprintf("response has more than %d valid WSS hosts", dnsMaxWSSCandidates), 0, nil)
	}
	rest, ok := selectDNSHost(document.REST.Hosts, false)
	if !ok {
		return loginEndpoints{}, dnsStageError("response has no valid HTTPS host in rest.hosts", 0, nil)
	}
	org, app, ok := strings.Cut(appKey, "#")
	if !ok || org == "" || app == "" || strings.Contains(app, "#") || strings.ContainsAny(org+app, " \t\r\n/") {
		return loginEndpoints{}, dnsStageError("AppKey must have org#app form", 0, nil)
	}
	restURL, err := url.Parse(rest)
	if err != nil {
		return loginEndpoints{}, dnsStageError("invalid selected REST host", 0, err)
	}
	restURL.Path = "/" + org + "/" + app
	return loginEndpoints{WSS: wss, REST: strings.TrimRight(restURL.String(), "/")}, nil
}

func selectDNSHost(hosts []dnsHost, websocket bool) (string, bool) {
	valid := selectDNSHosts(hosts, websocket)
	if len(valid) == 0 {
		return "", false
	}
	return valid[0], true
}

// selectDNSHosts returns all valid normalized endpoints. Priority 1 entries
// are a stable first partition and all other entries retain source order.
// Deduplication happens after partitioning so a priority copy wins even if a
// duplicate non-priority entry appeared earlier in the document.
func selectDNSHosts(hosts []dnsHost, websocket bool) []string {
	priority := make([]string, 0, len(hosts))
	fallback := make([]string, 0, len(hosts))
	for _, host := range hosts {
		endpoint, ok := makeDNSEndpoint(host, websocket)
		if !ok {
			continue
		}
		if strings.TrimSpace(string(host.Priority)) == "1" {
			priority = append(priority, endpoint)
		} else {
			fallback = append(fallback, endpoint)
		}
	}
	ordered := make([]string, 0, len(priority)+len(fallback))
	seen := make(map[string]struct{}, len(priority)+len(fallback))
	for _, group := range [][]string{priority, fallback} {
		for _, endpoint := range group {
			if _, duplicate := seen[endpoint]; duplicate {
				continue
			}
			seen[endpoint] = struct{}{}
			ordered = append(ordered, endpoint)
		}
	}
	return ordered
}

func makeDNSEndpoint(host dnsHost, websocket bool) (string, bool) {
	domain := strings.TrimSpace(host.Domain)
	if domain == "" || strings.ContainsAny(domain, " \t\r\n/@?#") {
		return "", false
	}
	normalizedDomain := strings.Trim(domain, "[]")
	isIPv6 := strings.Contains(normalizedDomain, ":") && net.ParseIP(normalizedDomain) != nil
	if !validDNSDomain(normalizedDomain) || strings.Contains(normalizedDomain, ":") && !isIPv6 {
		return "", false
	}
	scheme := strings.ToLower(strings.TrimSpace(host.Protocol))
	if websocket {
		if scheme != "wss" && scheme != "https" {
			return "", false
		}
		scheme = "wss"
	} else if scheme != "https" {
		return "", false
	}
	port := 443
	if raw := strings.TrimSpace(string(host.Port)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 65535 {
			return "", false
		}
		port = parsed
	}
	hostPort := normalizedDomain
	if port != 443 {
		hostPort = net.JoinHostPort(normalizedDomain, strconv.Itoa(port))
	} else if isIPv6 {
		hostPort = "[" + normalizedDomain + "]"
	}
	endpoint := &url.URL{Scheme: scheme, Host: hostPort}
	if websocket {
		endpoint.Path = "/websocket"
	}
	return endpoint.String(), true
}

func validDNSDomain(domain string) bool {
	if ip := net.ParseIP(domain); ip != nil {
		return true
	}
	if len(domain) == 0 || len(domain) > 253 {
		return false
	}
	domain = strings.TrimSuffix(domain, ".")
	for _, label := range strings.Split(domain, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			ch := label[i]
			if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') &&
				(ch < '0' || ch > '9') && ch != '-' {
				return false
			}
		}
	}
	return true
}

type dnsCacheKey struct {
	AppKey     string
	HTTPClient *http.Client
}

type dnsResolution struct {
	Endpoints  loginEndpoints
	Generation uint64
	Stale      bool
}

// dnsLoadFunc returns whether an error is transient. Only transient failures
// may fall back to a previously validated stale DNS response.
type dnsLoadFunc func(context.Context) (loginEndpoints, bool, error)

type dnsResolver struct {
	mu          sync.Mutex
	entries     map[dnsCacheKey]*dnsCacheEntry
	lru         *list.List
	capacity    int
	freshTTL    time.Duration
	staleTTL    time.Duration
	loadTimeout time.Duration
	now         func() time.Time
	generation  uint64
	overflow    chan struct{}
}

type dnsCacheEntry struct {
	key              dnsCacheKey
	endpoints        loginEndpoints
	hasValue         bool
	fetchedAt        time.Time
	generation       uint64
	flight           *dnsFlight
	failures         int
	refreshNotBefore time.Time
	lastErr          error
	lastTransient    bool
	element          *list.Element
}

type dnsFlight struct {
	done       chan struct{}
	resolution dnsResolution
	err        error
}

var (
	sharedDNSResolver = newDNSResolver(dnsResolverCapacity, dnsFreshTTL, dnsStaleTTL, time.Now)
	settledDNSLookup  = func() <-chan struct{} {
		done := make(chan struct{})
		close(done)
		return done
	}()
)

func newDNSResolver(capacity int, freshTTL, staleTTL time.Duration, now func() time.Time) *dnsResolver {
	if capacity < 1 {
		capacity = 1
	}
	if now == nil {
		now = time.Now
	}
	return &dnsResolver{
		entries:     make(map[dnsCacheKey]*dnsCacheEntry, capacity),
		lru:         list.New(),
		capacity:    capacity,
		freshTTL:    freshTTL,
		staleTTL:    staleTTL,
		loadTimeout: connectTimeout,
		now:         now,
		overflow:    make(chan struct{}, dnsResolverOverflow),
	}
}

// resolve coalesces refreshes for one AppKey and exact *http.Client identity.
// knownGeneration is zero for an ordinary lookup. A non-zero value asks for a
// result newer than the caller's current generation; if another caller has
// already refreshed the entry, that newer cached generation is reused.
func (r *dnsResolver) resolve(ctx context.Context, key dnsCacheKey, knownGeneration uint64, load dnsLoadFunc) (dnsResolution, error) {
	resolution, _, err := r.resolveWithSettlement(ctx, key, knownGeneration, load)
	return resolution, err
}

func (r *dnsResolver) resolveWithSettlement(ctx context.Context, key dnsCacheKey, knownGeneration uint64, load dnsLoadFunc) (dnsResolution, <-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return dnsResolution{}, settledDNSLookup, dnsStageError("request canceled before DNS lookup", 0, err)
	}
	if load == nil {
		return dnsResolution{}, settledDNSLookup, dnsStageError("DNS resolver has no loader", 0, nil)
	}

	r.mu.Lock()
	now := r.now()
	entry := r.entries[key]
	if entry != nil {
		r.touchLocked(entry)
		age := now.Sub(entry.fetchedAt)
		if entry.hasValue && age <= r.freshTTL && entry.generation > knownGeneration {
			result := resolutionFromEntry(entry, false)
			r.mu.Unlock()
			return result, settledDNSLookup, nil
		}
		if entry.flight != nil {
			flight := entry.flight
			r.mu.Unlock()
			result, err := waitDNSFlight(ctx, flight)
			return result, flight.done, err
		}
		if now.Before(entry.refreshNotBefore) {
			if entry.lastTransient && entry.hasValue && age <= r.staleTTL {
				result := resolutionFromEntry(entry, true)
				r.mu.Unlock()
				return result, settledDNSLookup, nil
			}
			err := entry.lastErr
			r.mu.Unlock()
			if err == nil {
				err = dnsStageError("DNS refresh is temporarily throttled", 0, nil)
			}
			return dnsResolution{}, settledDNSLookup, err
		}
	} else {
		entry = r.insertLocked(key)
		if entry == nil {
			r.mu.Unlock()
			return r.resolveUncached(ctx, load)
		}
	}

	flight := &dnsFlight{done: make(chan struct{})}
	entry.flight = flight
	r.mu.Unlock()

	go r.runLoad(key, entry, flight, load)
	result, err := waitDNSFlight(ctx, flight)
	return result, flight.done, err
}

// resolveUncached preserves the strict 256-entry map bound when every entry is
// refreshing. A small overflow semaphore avoids rejecting a new AppKey solely
// because the cache is full without allowing an unbounded goroutine/request
// fan-out. Overflow results intentionally are not inserted into the LRU.
func (r *dnsResolver) resolveUncached(ctx context.Context, load dnsLoadFunc) (dnsResolution, <-chan struct{}, error) {
	select {
	case r.overflow <- struct{}{}:
	case <-ctx.Done():
		return dnsResolution{}, settledDNSLookup, dnsStageError("request canceled waiting for DNS overflow slot", 0, ctx.Err())
	}
	flight := &dnsFlight{done: make(chan struct{})}
	go func() {
		defer func() { <-r.overflow }()
		loadCtx, cancel := context.WithTimeout(context.Background(), r.loadTimeout)
		endpoints, _, err := load(loadCtx)
		cancel()
		if err == nil {
			r.mu.Lock()
			r.generation++
			generation := r.generation
			r.mu.Unlock()
			flight.resolution = dnsResolution{Endpoints: cloneLoginEndpoints(endpoints), Generation: generation}
		} else {
			flight.err = err
		}
		close(flight.done)
	}()
	result, err := waitDNSFlight(ctx, flight)
	return result, flight.done, err
}

func (r *dnsResolver) runLoad(key dnsCacheKey, entry *dnsCacheEntry, flight *dnsFlight, load dnsLoadFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), r.loadTimeout)
	endpoints, transient, err := load(ctx)
	cancel()

	r.mu.Lock()
	now := r.now()
	current := r.entries[key]
	if current != entry || entry.flight != flight {
		flight.err = dnsStageError("DNS resolver flight was superseded", 0, nil)
		close(flight.done)
		r.mu.Unlock()
		return
	}

	entry.flight = nil
	if err == nil {
		r.generation++
		entry.endpoints = cloneLoginEndpoints(endpoints)
		entry.hasValue = true
		entry.fetchedAt = now
		entry.generation = r.generation
		entry.failures = 0
		entry.refreshNotBefore = time.Time{}
		entry.lastErr = nil
		entry.lastTransient = false
		flight.resolution = resolutionFromEntry(entry, false)
	} else {
		entry.failures++
		entry.lastErr = err
		entry.lastTransient = transient
		entry.refreshNotBefore = now.Add(dnsRefreshCooldown(entry.failures))
		if transient && entry.hasValue && now.Sub(entry.fetchedAt) <= r.staleTTL {
			flight.resolution = resolutionFromEntry(entry, true)
		} else {
			flight.err = err
		}
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *dnsResolver) insertLocked(key dnsCacheKey) *dnsCacheEntry {
	for len(r.entries) >= r.capacity {
		var victim *dnsCacheEntry
		for element := r.lru.Back(); element != nil; element = element.Prev() {
			candidate := element.Value.(*dnsCacheEntry)
			if candidate.flight == nil {
				victim = candidate
				break
			}
		}
		if victim == nil {
			return nil
		}
		delete(r.entries, victim.key)
		r.lru.Remove(victim.element)
		victim.element = nil
	}
	entry := &dnsCacheEntry{key: key}
	entry.element = r.lru.PushFront(entry)
	r.entries[key] = entry
	return entry
}

func (r *dnsResolver) touchLocked(entry *dnsCacheEntry) {
	if entry.element != nil {
		r.lru.MoveToFront(entry.element)
	}
}

func waitDNSFlight(ctx context.Context, flight *dnsFlight) (dnsResolution, error) {
	select {
	case <-flight.done:
		return flight.resolution, flight.err
	case <-ctx.Done():
		return dnsResolution{}, dnsStageError("request canceled while waiting for shared DNS lookup", 0, ctx.Err())
	}
}

func resolutionFromEntry(entry *dnsCacheEntry, stale bool) dnsResolution {
	return dnsResolution{
		Endpoints:  cloneLoginEndpoints(entry.endpoints),
		Generation: entry.generation,
		Stale:      stale,
	}
}

func cloneLoginEndpoints(endpoints loginEndpoints) loginEndpoints {
	return loginEndpoints{WSS: append([]string(nil), endpoints.WSS...), REST: endpoints.REST}
}

func dnsRefreshCooldown(failures int) time.Duration {
	if failures < 1 {
		return 0
	}
	delay := dnsFailureCooldown
	for i := 1; i < failures && delay < dnsMaxFailureCooldown; i++ {
		delay *= 2
		if delay >= dnsMaxFailureCooldown {
			return dnsMaxFailureCooldown
		}
	}
	return delay
}

func dnsStageError(reason string, status int, cause error) error {
	return &SDKError{Code: ErrDNS, Operation: "dns bootstrap", Reason: reason, HTTPStatus: status, Cause: cause}
}
