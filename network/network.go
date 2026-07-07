// Factory and lifecycle for network clients.
//
// This file defines the common Client interface, the Network wrapper that owns a
// single client, the NewConnection factory, and the type-cast helpers used to
// obtain a concrete client for type-specific operations.
package network

import "fmt"

// Client is the common interface implemented by the GraphQL, HTTP, and WebSocket
// clients. Use it to treat connections uniformly; use the As*ConnectionType
// helpers to obtain the concrete type for type-specific operations.
type Client interface {
	Connect(opts ConnectionOptions) error
	Close() error
	Reconnect() error
}

// Network wraps a single client (GraphQL, HTTP, or WebSocket) and exposes the
// connection lifecycle and type-cast helpers. Create with NewConnection; configure
// with WithOpts.
type Network struct {
	client  Client
	options ConnectionOptions
}

// NewConnection creates a new network client of the given type using the factory
// pattern. The client is not connected until WithOpts (or Connect) is called.
// Returns an error if clientType is not one of GraphQLConnClient, HTTPConnClient,
// or WebsocketConnClient.
func NewConnection(clientType ClientType) (*Network, error) {
	var client Client
	switch clientType {
	case GraphQLConnClient:
		client = &GraphQLClient{}
	case HTTPConnClient:
		client = &HTTPClient{}
	case WebsocketConnClient:
		client = &WebSocketClient{}
	default:
		return nil, fmt.Errorf("client type not supported: %s", clientType)
	}
	return &Network{client: client, options: defaultOpts}, nil
}

// WithOpts applies the given connection options and establishes the connection
// (including the optional connectivity check). Returns the receiver for chaining
// and an error if the connection fails.
func (n *Network) WithOpts(opts ConnectionOptions) (*Network, error) {
	n.options = opts
	if err := n.client.Connect(opts); err != nil {
		return n, err
	}
	return n, nil
}

// Close closes the underlying client connection. Safe to call multiple times.
func (n *Network) Close() error {
	return n.client.Close()
}

// Reconnect re-establishes the connection using the most recent options.
func (n *Network) Reconnect() error {
	return n.client.Reconnect()
}

// Client returns the underlying Client implementation.
func (n *Network) Client() Client {
	return n.client
}

// AsGraphQLConnectionType returns the underlying client as a *GraphQLClient.
// Returns an error if this Network was not created with GraphQLConnClient.
func (n *Network) AsGraphQLConnectionType() (*GraphQLClient, error) {
	c, ok := n.client.(*GraphQLClient)
	if !ok {
		return nil, fmt.Errorf("failed to cast to GraphQLClient")
	}
	return c, nil
}

// AsHTTPConnectionType returns the underlying client as an *HTTPClient.
// Returns an error if this Network was not created with HTTPConnClient.
func (n *Network) AsHTTPConnectionType() (*HTTPClient, error) {
	c, ok := n.client.(*HTTPClient)
	if !ok {
		return nil, fmt.Errorf("failed to cast to HTTPClient")
	}
	return c, nil
}

// AsWebSocketConnectionType returns the underlying client as a *WebSocketClient.
// Returns an error if this Network was not created with WebsocketConnClient.
func (n *Network) AsWebSocketConnectionType() (*WebSocketClient, error) {
	c, ok := n.client.(*WebSocketClient)
	if !ok {
		return nil, fmt.Errorf("failed to cast to WebSocketClient")
	}
	return c, nil
}
