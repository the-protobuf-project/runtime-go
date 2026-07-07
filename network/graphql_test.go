package network

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGraphQLClientConnect(t *testing.T) {
	// Use SkipConnectivityCheck so test does not require a real GraphQL server
	client := &GraphQLClient{}
	opts := ConnectionOptions{
		URL:                   URLOptions{Scheme: HTTPS, Host: "example.com", Paths: []string{"/graphql"}},
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}

	err := client.Connect(opts)
	assert.NoError(t, err, "Expected no error while connecting GraphQL client")
	assert.NotNil(t, client.client)
}

func TestGraphQLClientQuerySuccess(t *testing.T) {
	client := &GraphQLClient{}
	opts := ConnectionOptions{
		URL:                   URLOptions{Scheme: HTTPS, Host: "example.com", Paths: []string{"/graphql"}},
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}

	err := client.Connect(opts)
	assert.NoError(t, err, "Expected no error while connecting GraphQL client")

	query := struct{}{} // Empty query
	responseChan := client.Query(&query, nil)
	response := <-responseChan

	assert.NotNil(t, response.Error, "Expected an error due to missing implementation")
}

func TestGraphQLClientMutationSuccess(t *testing.T) {
	client := &GraphQLClient{}
	opts := ConnectionOptions{
		URL:                   URLOptions{Scheme: HTTPS, Host: "example.com", Paths: []string{"/graphql"}},
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}

	err := client.Connect(opts)
	assert.NoError(t, err, "Expected no error while connecting GraphQL client")

	mutation := struct{}{} // Empty mutation
	responseChan := client.Mutation(&mutation, nil)
	response := <-responseChan

	assert.NotNil(t, response.Error, "Expected an error due to missing implementation")
}

func TestGraphQLClientConnectWithSkipConnectivityCheck(t *testing.T) {
	client := &GraphQLClient{}
	opts := ConnectionOptions{
		URL:                   URLOptions{Scheme: HTTPS, Host: "invalid-url", Paths: []string{"/graphql"}},
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}

	err := client.Connect(opts)
	assert.NoError(t, err, "Connection should succeed when connectivity check is skipped")
	assert.NotNil(t, client.client, "GraphQL client should be initialized")
}

// urlOptionsFromServerURL parses a server URL into URLOptions for GraphQL tests.
func urlOptionsFromGraphQLServerURL(t *testing.T, raw string) URLOptions {
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

// TestGraphQLConnect_ConnectivitySuccess verifies Connect succeeds when server returns valid GraphQL response.
func TestGraphQLConnect_ConnectivitySuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(body, &req)
		assert.Contains(t, req.Query, "__typename", "Default connectivity query should contain __typename")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"__typename":"Query"}}`))
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:     urlOptionsFromGraphQLServerURL(t, server.URL),
		Timeout: 5 * time.Second,
	}
	client := &GraphQLClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)
}

// TestGraphQLConnect_ConnectivityFailure verifies Connect returns error when server fails or returns invalid response.
func TestGraphQLConnect_ConnectivityFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("error"))
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:     urlOptionsFromGraphQLServerURL(t, server.URL),
		Timeout: 5 * time.Second,
	}
	client := &GraphQLClient{}
	err := client.Connect(opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to GraphQL server")
}

// TestGraphQLConnect_CustomConnectivityQuery verifies custom GraphQL connectivity query is used.
func TestGraphQLConnect_CustomConnectivityQuery(t *testing.T) {
	customQuery := `query { __schema { queryType { name } } }`
	var receivedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(body, &req)
		receivedQuery = req.Query
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"__schema":{"queryType":{"name":"Query"}}}}`))
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:                      urlOptionsFromGraphQLServerURL(t, server.URL),
		Timeout:                  5 * time.Second,
		GraphQLConnectivityQuery: customQuery,
	}
	client := &GraphQLClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)
	assert.Equal(t, customQuery, receivedQuery, "Custom connectivity query should be sent")
}

// TestGraphQLReconnect_SkipConnectivityCheck verifies Reconnect respects SkipConnectivityCheck.
func TestGraphQLReconnect_SkipConnectivityCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"__typename":"Query"}}`))
	}))
	defer server.Close()

	opts := ConnectionOptions{
		URL:                   urlOptionsFromGraphQLServerURL(t, server.URL),
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}
	client := &GraphQLClient{}
	err := client.Connect(opts)
	assert.NoError(t, err)

	// Reconnect with skip still true (client retains opts)
	err = client.Reconnect()
	assert.NoError(t, err)
}

// TestGraphQLConnect_Unreachable verifies Connect returns error when host is unreachable.
func TestGraphQLConnect_Unreachable(t *testing.T) {
	opts := ConnectionOptions{
		URL:     URLOptions{Scheme: HTTP, Host: "127.0.0.1:19998", Paths: []string{"/graphql"}},
		Timeout: 100 * time.Millisecond,
	}
	client := &GraphQLClient{}
	err := client.Connect(opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "127.0.0.1:19998")
}
