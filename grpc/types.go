package grpc

import (
	"context"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
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
type MCPServiceFunc func(ctx context.Context, cfg *mcp.MCPServerConfig) error

// MCPServerConfig is re-exported for convenience so callers don't need to
// import runtime-go/agents/mcp directly.
type MCPServerConfig = mcp.MCPServerConfig

// MCPOption is re-exported for convenience.
type MCPOption = mcp.Option

// ElicitField is re-exported for convenience.
type ElicitField = mcp.ElicitField

// WithElicitHook returns an MCPOption that runs hook before each elicitation.
func WithElicitHook(hook func(ctx context.Context, toolName string, fields []ElicitField) ([]ElicitField, error)) MCPOption {
	return mcp.WithElicitHook(hook)
}

// A2AServiceFunc defines the signature for a blocking function that serves an
// Agent2Agent service. The A2AServerConfig is built by the HybridServer from
// A2AOptions — transports, shared mux, listen address, and the gRPC server the
// gRPC transport registers on — so a service func supplies the agent and lets
// the server place it.
//
// It mirrors MCPServiceFunc so a generated ServeFooA2A can be registered the
// same way a generated ServeFooMCP is. [WithA2AAgent] wraps a hand-written
// agent into one.
type A2AServiceFunc func(ctx context.Context, cfg *A2AServerConfig) error

// A2AServerConfig is re-exported for convenience so callers don't need to
// import runtime-go/agents/a2a directly.
type A2AServerConfig = a2a.ServerConfig

// A2AAgent is the interface an agent implements: a request in, a stream of
// events out. Re-exported for convenience.
type A2AAgent = a2a.Executor

// A2ASkill is one distinct capability an agent advertises on its card.
// Re-exported for convenience.
type A2ASkill = a2a.Skill

// Option is a functional option used for configuring a HybridServer.
type Option func(*HybridServer)

// GRPCServer is an alias for *grpc.Server, re-exported for convenience.
type GRPCServer = grpc.Server

// ServeMux is an alias for *runtime.ServeMux, re-exported for convenience.
type ServeMux = runtime.ServeMux

// DialOption is an alias for grpc.DialOption, re-exported for convenience.
type DialOption = grpc.DialOption
