package sdk

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// endpointRound is a single, immutable-snapshot pass over DNS WSS candidates.
// It owns a clone so replacing Client DNS state cannot mutate an in-progress
// reconnect round. The cursor is circular and every candidate is returned at
// most once before exhaustion becomes explicit.
type endpointRound struct {
	candidates []string
	next       int
	remaining  int
}

func newEndpointRound(candidates []string, start int) endpointRound {
	cloned := append([]string(nil), candidates...)
	if len(cloned) == 0 {
		return endpointRound{candidates: cloned}
	}
	start %= len(cloned)
	if start < 0 {
		start += len(cloned)
	}
	return endpointRound{candidates: cloned, next: start, remaining: len(cloned)}
}

func (r *endpointRound) nextEndpoint() (endpoint string, index int, ok bool) {
	if r == nil || r.remaining == 0 || len(r.candidates) == 0 {
		return "", 0, false
	}
	index = r.next
	endpoint = r.candidates[index]
	r.next = (r.next + 1) % len(r.candidates)
	r.remaining--
	return endpoint, index, true
}

func (r *endpointRound) exhausted() bool {
	return r == nil || r.remaining == 0
}

// shouldRotateEndpoint is intentionally narrow. Only failures that can be
// local to one WSS endpoint consume the next DNS candidate. Authentication,
// protocol, handler backlog, redirect validation and other process-wide or
// deterministic failures must not fan out across every endpoint.
func shouldRotateEndpoint(err error) bool {
	switch errorCode(err) {
	case ErrIO, ErrTimeout, ErrTLSFailed, ErrStreamClosed, ErrDNS, ErrHandshake:
		return true
	default:
		return false
	}
}

// resolveCachedEndpointCandidates is the cache adapter used by the later
// Client/reconnect wiring. Cache isolation includes the exact *http.Client
// identity because distinct clients may carry different proxy, TLS and
// transport policies even for the same AppKey.
func (c *Client) resolveCachedEndpointCandidates(ctx context.Context, knownGeneration uint64) (dnsResolution, error) {
	httpClient := c.restHTTPClient()
	key := dnsCacheKey{AppKey: c.cfg.AppKey, HTTPClient: httpClient}
	resolved, settlement, err := sharedDNSResolver.resolveWithSettlement(ctx, key, knownGeneration, func(loadCtx context.Context) (loginEndpoints, bool, error) {
		resolved, err := c.resolveLoginEndpointCandidates(loadCtx)
		return resolved, isTransientDNSResolutionError(err), err
	})
	c.trackDNSFlight(settlement)
	return resolved, err
}

func (c *Client) trackDNSFlight(done <-chan struct{}) {
	if done == nil {
		return
	}
	select {
	case <-done:
		return
	default:
	}
	c.mu.Lock()
	for pending := range c.pendingDNSFlights {
		select {
		case <-pending:
			delete(c.pendingDNSFlights, pending)
		default:
		}
	}
	select {
	case <-done:
		c.mu.Unlock()
		return
	default:
	}
	if c.pendingDNSFlights == nil {
		c.pendingDNSFlights = make(map[<-chan struct{}]struct{})
	}
	if _, exists := c.pendingDNSFlights[done]; exists {
		c.mu.Unlock()
		return
	}
	c.pendingDNSFlights[done] = struct{}{}
	c.mu.Unlock()
}

func (c *Client) waitPendingDNSFlights() {
	for {
		c.mu.RLock()
		pending := make([]<-chan struct{}, 0, len(c.pendingDNSFlights))
		for done := range c.pendingDNSFlights {
			pending = append(pending, done)
		}
		c.mu.RUnlock()
		if len(pending) == 0 {
			return
		}
		for _, done := range pending {
			<-done
		}
		c.mu.Lock()
		for _, done := range pending {
			delete(c.pendingDNSFlights, done)
		}
		c.mu.Unlock()
	}
}

func isTransientDNSResolutionError(err error) bool {
	if err == nil {
		return false
	}
	var sdkErr *SDKError
	if !errors.As(err, &sdkErr) || sdkErr.Code != ErrDNS {
		return false
	}
	if sdkErr.HTTPStatus == http.StatusTooManyRequests || sdkErr.HTTPStatus >= http.StatusInternalServerError {
		return true
	}
	switch sdkErr.Reason {
	case "request failed", "request canceled while retrying", "read response":
		return true
	}
	if errors.Is(sdkErr.Cause, context.Canceled) || errors.Is(sdkErr.Cause, context.DeadlineExceeded) {
		return true
	}
	var networkError net.Error
	return errors.As(sdkErr.Cause, &networkError)
}
