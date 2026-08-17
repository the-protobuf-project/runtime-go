// Package options defines configuration structures for the HybridServer.
package options

// Options defines the complete set of configuration options for a HybridServer.
type Options struct {
	// ServiceName is the name of the service, used for constructing
	// environment variable override prefixes.
	ServiceName string
	// Description is a brief description of the service.
	Description string
	// Version is the service version string.
	Version string
	// GRPC holds the configuration for the gRPC server endpoint.
	GRPC GRPCOptions
	// HTTP holds the configuration for the HTTP gateway endpoint.
	HTTP HTTPOptions
	// MCP holds the configuration for the MCP server.
	MCP MCPOptions
	// A2A holds the configuration for the Agent2Agent server.
	A2A A2AOptions
	// EnableHTTP controls whether the HTTP gateway is started.
	EnableHTTP bool
	// EnableHealth controls whether the standard gRPC health check service is enabled.
	EnableHealth bool
	// EnableMCP controls whether the MCP server is started.
	EnableMCP bool
	// EnableA2A controls whether the Agent2Agent server is started.
	EnableA2A bool
	// Environment specifies the server's operating mode (e.g., "production").
	Environment ServerEnvironment
	// ExperimentalHttp3 enables experimental HTTP/3 support on the HTTP port + 1.
	// This requires a valid TLS certificate to be configured.
	ExperimentalHttp3 bool
}

// GRPCOptions defines the network host and port for the gRPC server.
type GRPCOptions struct {
	// Host is the network interface the gRPC server will listen on.
	Host string
	// Port is the port number for the gRPC server.
	Port int
}

// HTTPOptions defines the network host and port for the HTTP gateway.
type HTTPOptions struct {
	// Host is the network interface the HTTP gateway will listen on.
	Host string
	// Port is the port number for the HTTP gateway.
	Port int
}

// MCPTransport represents the transport protocol for the MCP server.
type MCPTransport string

const (
	// MCPTransportStdio runs the MCP server over standard input/output.
	MCPTransportStdio MCPTransport = "stdio"
	// MCPTransportStreamableHTTP runs the MCP server over Streamable HTTP.
	MCPTransportStreamableHTTP MCPTransport = "streamable-http"
	// MCPTransportSSE runs the MCP server over Server-Sent Events.
	MCPTransportSSE MCPTransport = "sse"
)

// MCPOptions defines the configuration for the MCP server.
type MCPOptions struct {
	// Host is the network interface the MCP server will listen on.
	// Ignored for stdio. Defaults to "0.0.0.0".
	Host string
	// Port is the port number for HTTP-based transports (streamable-http, sse).
	// Ignored for stdio. Defaults to HTTP.Port + 1.
	Port int
	// Transport specifies the MCP transport protocol. Defaults to stdio.
	Transport MCPTransport
}

// A2ATransport represents a transport protocol for the Agent2Agent server.
type A2ATransport string

const (
	// A2ATransportJSONRPC serves A2A as JSON-RPC 2.0 over HTTP, with
	// server-sent events for streaming. Every A2A client speaks it.
	A2ATransportJSONRPC A2ATransport = "jsonrpc"
	// A2ATransportGRPC serves A2A as a gRPC service on the HybridServer's own
	// gRPC port, alongside the process's other services.
	A2ATransportGRPC A2ATransport = "grpc"
	// A2ATransportREST serves the HTTP+JSON binding beneath the base path.
	A2ATransportREST A2ATransport = "rest"
)

// A2AOptions defines the configuration for the Agent2Agent server.
type A2AOptions struct {
	// Host is the network interface the HTTP-based transports listen on.
	// Defaults to "0.0.0.0".
	Host string
	// Port is the port number for the HTTP-based transports (jsonrpc, rest).
	// Ignored by the gRPC transport, which shares the gRPC server's port.
	// Defaults to MCP.Port + 1.
	Port int
	// Transport is the single-transport form. Transports is the general one;
	// when both are set Transports wins. Defaults to jsonrpc.
	Transport  A2ATransport
	Transports []A2ATransport
	// BasePath is where JSON-RPC mounts and the prefix REST is served under.
	// Defaults to "/a2a".
	BasePath string
	// PublicURL is the base URL clients should be told to use, for a server
	// behind a proxy or inside a container. Without it the agent card
	// advertises the listen address, which is wrong past any hop.
	PublicURL string
}

// ServerEnvironment is a type-safe string representing the server's operating mode.
type ServerEnvironment string

const (
	// Development mode is intended for local development, often with relaxed security.
	Development ServerEnvironment = "development"
	// Debug mode enables verbose logging and other debugging aids.
	Debug ServerEnvironment = "debug"
	// Staging mode mimics the production environment for pre-deployment testing.
	Staging ServerEnvironment = "staging"
	// Production mode is for live deployments, with performance and security optimized.
	Production ServerEnvironment = "production"
)

// IsValid checks if the ServerEnvironment value is one of the predefined constants.
func (e ServerEnvironment) IsValid() bool {
	switch e {
	case Development, Debug, Staging, Production:
		return true
	default:
		return false
	}
}
