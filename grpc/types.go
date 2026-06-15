package grpc

import (
	"context"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	mcpRuntime "github.com/machanirobotics/grpc-mcp-gateway/runtime"
	"google.golang.org/grpc"
)

// GRPCServiceFunc defines the signature for a function that registers a gRPC
// service with a server instance.
type GRPCServiceFunc func(*grpc.Server)

// HTTPServiceFunc defines the signature for a function that registers an HTTP
// gateway handler. It takes the gateway's ServeMux, the backend gRPC server's
// endpoint address, and the gRPC dial options needed to connect to it.
type HTTPServiceFunc func(*runtime.ServeMux, string, []grpc.DialOption) error

// MCPServiceFunc defines the signature for a blocking function that serves
// an MCP service. The MCPServerConfig is built by the HybridServer from
// MCPOptions; the generated ServeFooMCP function auto-sets BasePath.
type MCPServiceFunc func(ctx context.Context, cfg *mcpRuntime.MCPServerConfig) error

// MCPServerConfig is re-exported for convenience so callers don't need to
// import grpc-mcp-gateway/runtime directly.
type MCPServerConfig = mcpRuntime.MCPServerConfig

// MCPOption is re-exported for convenience.
type MCPOption = mcpRuntime.Option

// ElicitField is re-exported for convenience.
type ElicitField = mcpRuntime.ElicitField

// WithElicitHook returns an MCPOption that runs hook before each elicitation.
func WithElicitHook(hook func(ctx context.Context, toolName string, fields []ElicitField) ([]ElicitField, error)) MCPOption {
	return mcpRuntime.WithElicitHook(hook)
}

// Option is a functional option used for configuring a HybridServer.
type Option func(*HybridServer)

// GRPCServer is an alias for *grpc.Server, re-exported for convenience.
type GRPCServer = grpc.Server

// ServeMux is an alias for *runtime.ServeMux, re-exported for convenience.
type ServeMux = runtime.ServeMux

// DialOption is an alias for grpc.DialOption, re-exported for convenience.
type DialOption = grpc.DialOption
