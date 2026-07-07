package network

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// urlOptionsFromServerURL parses a server URL (e.g. httptest.Server.URL) into URLOptions.
func urlOptionsFromServerURL(t *testing.T, raw string) URLOptions {
	t.Helper()
	u, err := url.Parse(raw)
	assert.NoError(t, err)
	path := u.Path
	if path == "" {
		path = "/"
	}
	scheme := HTTP
	if u.Scheme == "https" {
		scheme = HTTPS
	}
	return URLOptions{Scheme: scheme, Host: u.Host, Paths: []string{path}}
}

func TestHTTPClientConnect(t *testing.T) {
	client := &HTTPClient{}
	opts := ConnectionOptions{
		URL:     URLOptions{Scheme: HTTPS, Host: "example.com", Paths: []string{"/test"}},
		Timeout: 5 * time.Second,
	}

	err := client.Connect(opts)
	assert.Nil(t, err, "Expected no error while connecting HTTP client")
}

// TestHTTPConnect_HEADSuccess verifies Connect succeeds when server responds 200 to HEAD.
func TestHTTPConnect_HEADSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodHead, r.Method, "Connectivity check should use HEAD")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:     urlOptionsFromServerURL(t, server.URL),
		Timeout: 5 * time.Second,
	}
	client := &HTTPClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)
}

// TestHTTPConnect_HEAD405FallsBackToGET verifies Connect falls back to GET when server returns 405 for HEAD.
func TestHTTPConnect_HEAD405FallsBackToGET(t *testing.T) {
	var headCalled, getCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			headCalled = true
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodGet {
			getCalled = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:     urlOptionsFromServerURL(t, server.URL),
		Timeout: 5 * time.Second,
	}
	client := &HTTPClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)
	assert.True(t, headCalled, "HEAD should have been tried first")
	assert.True(t, getCalled, "GET should have been used after 405 on HEAD")
}

// TestHTTPConnect_SkipConnectivityCheck verifies no request is made when SkipConnectivityCheck is true.
func TestHTTPConnect_SkipConnectivityCheck(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:                   urlOptionsFromServerURL(t, server.URL),
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}
	client := &HTTPClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)
	assert.Equal(t, 0, requestCount, "No request should be made when SkipConnectivityCheck is true")
}

// TestHTTPConnect_DefaultTimeout verifies that Timeout <= 0 uses DefaultTimeout.
func TestHTTPConnect_DefaultTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:     urlOptionsFromServerURL(t, server.URL),
		Timeout: 0, // should use DefaultTimeout
	}
	client := &HTTPClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)
	assert.Equal(t, DefaultTimeout, client.Timeout, "Client should have DefaultTimeout when opts.Timeout was 0")
}

// TestHTTPConnect_FailsWhenUnreachable verifies Connect returns error when host is unreachable.
func TestHTTPConnect_FailsWhenUnreachable(t *testing.T) {
	opts := ConnectionOptions{
		URL:     URLOptions{Scheme: HTTP, Host: "127.0.0.1:19999", Paths: []string{"/"}},
		Timeout: 100 * time.Millisecond,
	}
	client := &HTTPClient{}
	err := client.Connect(opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "127.0.0.1:19999")
}

func TestHTTPClientRequestWithContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := &HTTPClient{}
	opts := ConnectionOptions{URL: urlOptionsFromServerURL(t, server.URL), Timeout: 5 * time.Second}
	assert.NoError(t, client.Connect(opts))

	response := <-client.Request(context.Background(), GET, opts.URL, nil, nil, 0, 0)
	assert.NoError(t, response.Error)
	assert.NotNil(t, response.Data, "Expected response data")
}

func TestHTTPClientRequestSync(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := &HTTPClient{}
	opts := ConnectionOptions{URL: urlOptionsFromServerURL(t, server.URL), Timeout: 5 * time.Second}
	assert.NoError(t, client.Connect(opts))

	data, err := client.RequestSync(context.Background(), GET, opts.URL, nil, nil, 0, 0)
	assert.NoError(t, err)
	assert.NotNil(t, data, "Expected response data")
}

func TestHTTPClientWithRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := &HTTPClient{}
	opts := ConnectionOptions{URL: urlOptionsFromServerURL(t, server.URL), Timeout: 5 * time.Second, RetryDelay: 10 * time.Millisecond}
	assert.NoError(t, client.Connect(opts))

	response := <-client.Request(context.Background(), GET, opts.URL, nil, nil, 0, 2)
	assert.NoError(t, response.Error)
	assert.NotNil(t, response.Data, "Expected response data after retries")
}

func TestHTTPClientContextCancellation(t *testing.T) {
	// Slow on the actual GET; the HEAD connectivity check stays fast so Connect succeeds.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			select {
			case <-time.After(5 * time.Second):
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &HTTPClient{}
	opts := ConnectionOptions{URL: urlOptionsFromServerURL(t, server.URL), Timeout: 30 * time.Second}
	assert.NoError(t, client.Connect(opts))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	response := <-client.Request(ctx, GET, opts.URL, nil, nil, 0, 0)
	assert.NotNil(t, response.Error, "Expected error due to context timeout")
}

func TestValidateStatusCode(t *testing.T) {
	// Test valid status codes
	err := validateStatusCode(200)
	assert.Nil(t, err, "Expected no error for 200 status")

	err = validateStatusCode(204)
	assert.Nil(t, err, "Expected no error for 204 status")

	// Test client-side error status codes
	err = validateStatusCode(400)
	assert.NotNil(t, err, "Expected error for 400 status")
	assert.Contains(t, err.Error(), "400", "Error should mention status code")

	err = validateStatusCode(404)
	assert.NotNil(t, err, "Expected error for 404 status")

	// Test server-side error status codes
	err = validateStatusCode(500)
	assert.NotNil(t, err, "Expected error for 500 status")
	assert.Contains(t, err.Error(), "500", "Error should mention status code")
}
