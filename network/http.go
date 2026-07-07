// HTTP client: connection lifecycle and verbs.
//
// This file provides the HTTPClient used when ClientType is HTTPConnClient. Request
// execution and helpers live in http_request.go.
package network

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// maxConnectivityResponseBodyBytes caps the bytes drained from the connectivity-check
// response body (after a GET fallback), limiting exposure to oversized responses.
const maxConnectivityResponseBodyBytes = 1 << 20 // 1 MiB

// HTTPMethod is the HTTP verb for a request.
type HTTPMethod string

const (
	GET    HTTPMethod = "GET"
	POST   HTTPMethod = "POST"
	PUT    HTTPMethod = "PUT"
	PATCH  HTTPMethod = "PATCH"
	DELETE HTTPMethod = "DELETE"
)

// HTTPClient is an HTTP REST client. Create via NewConnection(HTTPConnClient) and
// AsHTTPConnectionType. It embeds ConnectionOptions (URL, Timeout, Headers, Retries,
// RetryDelay, SkipConnectivityCheck).
type HTTPClient struct {
	client *http.Client
	ConnectionOptions
}

// Connect configures the HTTP client and optionally verifies the server is reachable.
// If opts.Timeout <= 0, DefaultTimeout is used. If SkipConnectivityCheck is true, no
// request is sent. Otherwise a HEAD request is sent; if the server returns 405, a GET
// is sent instead and the body is drained (up to 1 MiB). Returns an error if the
// reachability check fails.
func (h *HTTPClient) Connect(opts ConnectionOptions) error {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	h.ConnectionOptions = opts
	h.client = &http.Client{Timeout: opts.Timeout}

	fullURL, err := buildFullURL(opts.URL, 0)
	if err != nil {
		return fmt.Errorf("failed to build HTTP URL: %w", err)
	}
	if opts.URL.Scheme != HTTP && opts.URL.Scheme != HTTPS {
		return fmt.Errorf("invalid URL scheme: %s. Must be 'http' or 'https'", opts.URL.Scheme)
	}
	if opts.SkipConnectivityCheck {
		return nil
	}
	return h.checkConnectivity(fullURL)
}

// checkConnectivity sends a HEAD request (falling back to GET on 405) and drains the
// response body so the connection can be reused. Returns an error if the host is
// unreachable.
func (h *HTTPClient) checkConnectivity(fullURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), h.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fullURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create connectivity request: %w", err)
	}
	resp, err := h.client.Do(req)
	if err == nil && resp != nil && resp.StatusCode == http.StatusMethodNotAllowed {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		req, _ = http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		resp, err = h.client.Do(req)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to HTTP server at %s: %w", h.URL.Host, err)
	}
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxConnectivityResponseBodyBytes))
		_ = resp.Body.Close()
	}
	return nil
}

// Close clears the HTTP client. It is not usable until Connect is called again.
func (h *HTTPClient) Close() error {
	h.client = nil
	return nil
}

// Reconnect re-applies the current ConnectionOptions (calls Connect again).
func (h *HTTPClient) Reconnect() error {
	return h.Connect(h.ConnectionOptions)
}
