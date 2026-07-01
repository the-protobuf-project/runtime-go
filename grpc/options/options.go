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
	// EnableHTTP controls whether the HTTP gateway is started.
	EnableHTTP bool
	// EnableHealth controls whether the standard gRPC health check service is enabled.
	EnableHealth bool
	// EnableMCP controls whether the MCP server is started.
	EnableMCP bool
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
