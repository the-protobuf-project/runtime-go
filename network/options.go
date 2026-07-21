package network

import (
	"time"

	"go.opentelemetry.io/otel/propagation"
)

// ClientType identifies which kind of network client NewConnection creates.
type ClientType string

const (
	GraphQLConnClient   ClientType = "graphql"   // GraphQL client for queries, mutations, subscriptions
	HTTPConnClient      ClientType = "http"      // HTTP client for REST-style requests
	WebsocketConnClient ClientType = "websocket" // WebSocket client for full-duplex messaging
)

// URLScheme is the protocol part of a URL.
type URLScheme string

const (
	HTTP  URLScheme = "http"  // Plain HTTP
	HTTPS URLScheme = "https" // TLS HTTP
	WS    URLScheme = "ws"    // Plain WebSocket
	WSS   URLScheme = "wss"   // TLS WebSocket
)

// DefaultTimeout is used when ConnectionOptions.Timeout is zero or negative. It
// applies to connection establishment, HTTP requests, and GraphQL operations.
var DefaultTimeout = 10 * time.Second

// URLOptions describes the target URL: scheme, host, path(s), and optional query
// parameters. Host may include a port (e.g. "localhost:8080"). Paths is a list of
// path segments; a client selects one by index (e.g. pathIndex 0 for the first).
type URLOptions struct {
	Scheme URLScheme         // Protocol: http, https, ws, or wss
	Host   string            // Hostname and optional port (e.g. "api.example.com:443")
	Paths  []string          // Paths to choose from (e.g. ["/graphql", "/v2/graphql"])
	Params map[string]string // Optional query parameters
}

// ConnectionOptions holds settings shared by all client types. Pass it to WithOpts
// or to a client's Connect method.
type ConnectionOptions struct {
	// URL is the target endpoint. For HTTP/GraphQL use http or https; for
	// WebSocket use ws or wss.
	URL URLOptions
	// Timeout applies to connection establishment and to individual requests. If
	// zero or negative, DefaultTimeout is used.
	Timeout time.Duration
	// Headers are sent on every request (and on the WebSocket handshake).
	Headers map[string]string
	// Retries is the maximum number of retries for HTTP requests.
	Retries int
	// RetryDelay is the pause between retries. If zero, a default of 2s is used.
	RetryDelay time.Duration

	// SkipConnectivityCheck, when true, skips the initial HTTP/GraphQL reachability
	// check. Ignored for WebSocket, where the connection is established by Dial.
	SkipConnectivityCheck bool

	// GraphQLConnectivityQuery overrides the query used to verify a GraphQL server
	// is reachable. If empty, DefaultGraphQLConnectivityQuery is used.
	GraphQLConnectivityQuery string

	// TracePropagator, when set, injects the calling context's active span into
	// every outgoing GraphQL and HTTP request as headers (e.g.
	// propagation.TraceContext{} for the W3C traceparent/tracestate headers),
	// so a downstream service that continues the same propagator sees this
	// request as a child span — distributed tracing across the wire with no
	// per-request wiring beyond passing a context that already carries a span
	// (e.g. one opened by your tracing SDK). Nil (the default) injects nothing.
	TracePropagator propagation.TextMapPropagator
}

// defaultOpts holds the zero-value options applied to a freshly created Network.
var defaultOpts = ConnectionOptions{}
