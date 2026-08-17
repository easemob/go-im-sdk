package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	dnsBootstrapURL      = "https://rs.easemob.com/easemob/server.json"
	dnsFileVersion       = "1"
	dnsMaxAttempts       = 2
	dnsResponseMaxBytes  = int64(1 << 20)
	dnsRetryInitialDelay = 100 * time.Millisecond
)

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
	endpoint, err := url.Parse(dnsBootstrapURL)
	if err != nil {
		return "", "", dnsStageError("invalid SDK bootstrap URL", 0, err)
	}
	query := endpoint.Query()
	query.Set("sdk_version", sdkVersion)
	query.Set("app_key", c.cfg.AppKey)
	query.Set("file_version", dnsFileVersion)
	endpoint.RawQuery = query.Encode()

	var lastErr error
	for attempt := 1; attempt <= dnsMaxAttempts; attempt++ {
		wss, rest, retry, err := c.fetchLoginEndpoints(ctx, endpoint.String())
		if err == nil {
			if c.debug {
				c.logger.Debug("dns.resolved", "attempt", attempt, "wss", wss, "rest_base", rest)
			}
			return wss, rest, nil
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
			return "", "", dnsStageError("request canceled while retrying", 0, ctx.Err())
		}
	}
	return "", "", lastErr
}

func (c *Client) fetchLoginEndpoints(ctx context.Context, endpoint string) (string, string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", "", false, dnsStageError("create request", 0, err)
	}
	req.Header.Set("Accept", "application/json")
	httpClient := *c.restHTTPClient()
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := httpClient.Do(req)
	if err != nil {
		return "", "", true, dnsStageError("request failed", 0, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		retry := response.StatusCode >= http.StatusInternalServerError || response.StatusCode == http.StatusTooManyRequests
		return "", "", retry, dnsStageError("unexpected HTTP status", response.StatusCode, nil)
	}
	body, err := readBoundedDNSResponse(response.Body)
	if err != nil {
		return "", "", false, err
	}
	wss, rest, err := resolveDNSPayload(body, c.cfg.AppKey)
	return wss, rest, false, err
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
	var document dnsDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return "", "", dnsStageError("invalid JSON response", 0, err)
	}
	wss, ok := selectDNSHost(document.MsyncWX.Hosts, true)
	if !ok {
		return "", "", dnsStageError("response has no valid WSS host in msync-wx.hosts", 0, nil)
	}
	rest, ok := selectDNSHost(document.REST.Hosts, false)
	if !ok {
		return "", "", dnsStageError("response has no valid HTTPS host in rest.hosts", 0, nil)
	}
	org, app, ok := strings.Cut(appKey, "#")
	if !ok || org == "" || app == "" || strings.Contains(app, "#") || strings.ContainsAny(org+app, " \t\r\n/") {
		return "", "", dnsStageError("AppKey must have org#app form", 0, nil)
	}
	restURL, err := url.Parse(rest)
	if err != nil {
		return "", "", dnsStageError("invalid selected REST host", 0, err)
	}
	restURL.Path = "/" + org + "/" + app
	return wss, strings.TrimRight(restURL.String(), "/"), nil
}

func selectDNSHost(hosts []dnsHost, websocket bool) (string, bool) {
	type candidate struct {
		endpoint string
		priority bool
	}
	valid := make([]candidate, 0, len(hosts))
	for _, host := range hosts {
		endpoint, ok := makeDNSEndpoint(host, websocket)
		if !ok {
			continue
		}
		valid = append(valid, candidate{endpoint: endpoint, priority: strings.TrimSpace(string(host.Priority)) == "1"})
	}
	for _, host := range valid {
		if host.priority {
			return host.endpoint, true
		}
	}
	if len(valid) == 0 {
		return "", false
	}
	return valid[0].endpoint, true
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

func dnsStageError(reason string, status int, cause error) error {
	return &SDKError{Code: ErrDNS, Operation: "dns bootstrap", Reason: reason, HTTPStatus: status, Cause: cause}
}
