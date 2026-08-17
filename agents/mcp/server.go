package mcp

import (
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
)

// MCPServerConfig holds configuration for starting an MCP server.
// Set Transports (or Transport) to choose one or more wire protocols.
// When multiple transports are specified they run concurrently in the
// same process -- e.g. ["stdio", "streamable-http"].
type MCPServerConfig struct {
	// Name is the MCP server name reported during initialization.
	Name string
	// Version is the MCP server version reported during initialization.
	Version string
	// Transport selects a single wire protocol (for backward compatibility).
	// Ignored when Transports is non-empty.
	Transport Transport
	// Transports selects one or more wire protocols to serve concurrently.
	// Takes precedence over Transport.
	Transports []Transport
	// Addr is the listen address for HTTP-based transports (default ":8080").
	Addr string
	// ServerOptions are passed to mcp.NewServer.
	ServerOptions *mcp.ServerOptions
	// StreamableHTTPOptions are passed to mcp.NewStreamableHTTPHandler.
	StreamableHTTPOptions *mcp.StreamableHTTPOptions
	// SSEOptions are passed to mcp.NewSSEHandler.
	SSEOptions *mcp.SSEOptions
	// BasePath is the HTTP path prefix for the MCP endpoint (default "/mcp").
	BasePath string
	// GeneratedBasePath is the proto-derived default BasePath. If set, it takes precedence over BasePath.
	GeneratedBasePath string
	// HeaderMappings configures HTTP header to gRPC metadata forwarding.
	// Each entry maps an HTTP header name to a gRPC metadata key.
	// Use DefaultHeaderMappings() for common headers.
	HeaderMappings []HeaderMapping
	// OnReady is called after BasePath is resolved, just before the server starts listening.
	// Use this to log or inspect the final endpoint.
	OnReady func(cfg *MCPServerConfig)
	// HealthCheckPath, when non-empty, registers an HTTP GET endpoint that performs
	// a gRPC health check via HealthCheckConn. Returns 200 if SERVING, 503 otherwise.
	// Use with HealthCheckConn for load balancer / k8s probes. Default: "/health".
	HealthCheckPath string
	// HealthCheckConn is the gRPC connection used for health checks when HealthCheckPath is set.
	// The backend gRPC server should register grpc_health_v1.HealthServer.
	HealthCheckConn *grpc.ClientConn
	// ReadTimeout is the maximum duration for reading the entire request. Zero means no limit.
	// For progress-enabled tools, keep at 0 so long-running requests are not interrupted.
	ReadTimeout time.Duration
	// WriteTimeout is the maximum duration before timing out writes of the response. Zero means no limit.
	// For progress-enabled tools, keep at 0 so streaming progress notifications do not time out.
	WriteTimeout time.Duration
	// UnaryInterceptor, when set, is wrapped around every unary tool call the
	// served handlers dispatch — the hosting server's chance to push its gRPC
	// unary interceptor chain (validation, auth, tracing) down to MCP so both
	// surfaces enforce identical middleware. Generated Serve*MCP functions
	// forward it to the handler registration; see Config.UnaryInterceptor.
	UnaryInterceptor grpc.UnaryServerInterceptor
	// Mux, when non-nil, mounts the HTTP transports on this shared mux at
	// BasePath instead of starting a listener of the server's own — the seam a
	// hosting process uses to serve many MCP services from ONE port, routed by
	// their distinct base paths. StartServer then blocks until ctx is
	// canceled; the host owns the http.Server lifecycle. Ignored by the stdio
	// transport, which cannot share.
	Mux *http.ServeMux
}

// NewMCPServer creates an mcp.Server from a MCPServerConfig.
func NewMCPServer(cfg *MCPServerConfig) *mcp.Server {
	opts := cfg.ServerOptions
	if opts == nil {
		opts = &mcp.ServerOptions{}
	}
	return mcp.NewServer(&mcp.Implementation{Name: cfg.Name, Version: cfg.Version}, opts)
}

// ParseTransports splits a comma-separated transport string into a []Transport slice.
// Use with MCP_TRANSPORT env var:
//
//	transports := ParseTransports(os.Getenv("MCP_TRANSPORT"))
//	if len(transports) == 0 {
//	    transports = []Transport{TransportStreamableHTTP}
//	}
func ParseTransports(s string) []Transport {
	parts := strings.Split(s, ",")
	out := make([]Transport, 0, len(parts))
	for _, p := range parts {
		if t := Transport(strings.TrimSpace(p)); t != "" {
			out = append(out, t)
		}
	}
	return out
}
