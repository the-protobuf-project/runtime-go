package network

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewConnection(t *testing.T) {
	// Test creating HTTP client
	client, err := NewConnection(HTTPConnClient)
	assert.Nil(t, err, "Expected no error while creating HTTP client")
	assert.NotNil(t, client, "Expected non-nil client")

	// Test creating GraphQL client
	client, err = NewConnection(GraphQLConnClient)
	assert.Nil(t, err, "Expected no error while creating GraphQL client")
	assert.NotNil(t, client, "Expected non-nil client")

	// Test creating WebSocket client
	client, err = NewConnection(WebsocketConnClient)
	assert.Nil(t, err, "Expected no error while creating WebSocket client")
	assert.NotNil(t, client, "Expected non-nil client")

	// Test invalid client type
	client, err = NewConnection("invalid")
	assert.NotNil(t, err, "Expected error for unsupported client type")
	assert.Nil(t, client, "Expected nil client for invalid type")
}

func TestNetworkClose(t *testing.T) {
	client, _ := NewConnection(HTTPConnClient)
	err := client.Close()
	assert.Nil(t, err, "Expected no error on closing client")
}

func TestNetworkReconnect(t *testing.T) {
	client, _ := NewConnection(HTTPConnClient)

	// First connect with valid options (skip connectivity check so no server required)
	opts := ConnectionOptions{
		URL:                   URLOptions{Scheme: HTTP, Host: "localhost", Paths: []string{"/test"}},
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}
	_, err := client.WithOpts(opts)
	assert.NoError(t, err, "Expected no error while setting options")

	// Now reconnect should work
	err = client.Reconnect()
	assert.NoError(t, err, "Expected no error on reconnecting client")
}

func TestBuildFullURL(t *testing.T) {
	urlOptions := URLOptions{
		Scheme: HTTPS,
		Host:   "example.com",
		Paths:  []string{"/test"},
	}

	url, err := buildFullURL(urlOptions, 0)
	assert.Nil(t, err, "Expected no error while building URL")
	assert.Equal(t, "https://example.com/test", url, "Expected valid URL")

	_, err = buildFullURL(urlOptions, -1)
	assert.NotNil(t, err, "Expected error for invalid path index")
}

func TestNetworkWithOpts(t *testing.T) {
	client, _ := NewConnection(HTTPConnClient)
	newOpts := ConnectionOptions{
		URL:                   URLOptions{Scheme: HTTP, Host: "localhost", Paths: []string{"/test"}},
		Timeout:               5 * time.Second,
		SkipConnectivityCheck: true,
	}

	_, err := client.WithOpts(newOpts)
	assert.NoError(t, err, "Expected no error while updating options")
}

func TestBuildFullURLWithQueryParams(t *testing.T) {
	urlOptions := URLOptions{
		Scheme: HTTPS,
		Host:   "example.com",
		Paths:  []string{"/api/v1/users"},
		Params: map[string]string{
			"page":  "1",
			"limit": "10",
		},
	}

	url, err := buildFullURL(urlOptions, 0)
	assert.Nil(t, err, "Expected no error while building URL with query params")
	assert.Contains(t, url, "https://example.com/api/v1/users", "URL should contain base path")
	assert.Contains(t, url, "page=1", "URL should contain page param")
	assert.Contains(t, url, "limit=10", "URL should contain limit param")
}

func TestBuildFullURLValidation(t *testing.T) {
	// Test empty host
	urlOptions := URLOptions{
		Scheme: HTTPS,
		Host:   "",
		Paths:  []string{"/test"},
	}
	_, err := buildFullURL(urlOptions, 0)
	assert.NotNil(t, err, "Expected error for empty host")

	// Test empty paths
	urlOptions = URLOptions{
		Scheme: HTTPS,
		Host:   "example.com",
		Paths:  []string{},
	}
	_, err = buildFullURL(urlOptions, 0)
	assert.NotNil(t, err, "Expected error for empty paths")

	// Test invalid scheme
	urlOptions = URLOptions{
		Scheme: "ftp",
		Host:   "example.com",
		Paths:  []string{"/test"},
	}
	_, err = buildFullURL(urlOptions, 0)
	assert.NotNil(t, err, "Expected error for invalid scheme")
}
