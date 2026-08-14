package sdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
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
