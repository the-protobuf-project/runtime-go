// Package runtime is the stable, single-import facade that generated GraphQL clients
// depend on. It re-exports the essentials of the underlying network package so
// generated code references one branded package (this one) instead of reaching into
// transport internals.
//
// Generated code typically does:
//
//	conn, _ := runtime.NewConnection(runtime.GraphQLConnClient)
//	conn.WithOpts(runtime.ConnectionOptions{URL: runtime.URLOptions{
//	    Scheme: runtime.HTTP, Host: "localhost:3280", Paths: []string{"/graphql"},
//	}})
//	gql, _ := conn.AsGraphQLConnectionType()
package runtime

import (
	"net/url"

	"github.com/the-protobuf-project/runtime-go/network"
)

// Transport, connection, and result types re-exported from the network package.
type (
	Network           = network.Network
	Client            = network.Client
	ClientType        = network.ClientType
	ConnectionOptions = network.ConnectionOptions
	URLOptions        = network.URLOptions
	URLScheme         = network.URLScheme
	GraphQLClient     = network.GraphQLClient
	GraphQLResult     = network.GraphQLResult
	Subscription      = network.Subscription
	HTTPClient        = network.HTTPClient
	WebSocketClient   = network.WebSocketClient
)

// GraphQL scalar types re-exported for use in variables and struct tags.
type (
	Boolean = network.Boolean
	Float   = network.Float
	Int     = network.Int
	String  = network.String
	ID      = network.ID
)

// Client-type and URL-scheme constants re-exported from the network package.
const (
	GraphQLConnClient   = network.GraphQLConnClient
	HTTPConnClient      = network.HTTPConnClient
	WebsocketConnClient = network.WebsocketConnClient

	HTTP  = network.HTTP
	HTTPS = network.HTTPS
	WS    = network.WS
	WSS   = network.WSS
)

// NewConnection creates a network client of the given type using the factory.
func NewConnection(clientType ClientType) (*Network, error) {
	return network.NewConnection(clientType)
}

// URLFromStd converts a parsed *url.URL into URLOptions for ConnectionOptions, so
// generated clients can connect straight from url.Parse output.
func URLFromStd(u *url.URL) URLOptions {
	return network.URLFromStd(u)
}
