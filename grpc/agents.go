package grpc

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/agents/mcp"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// startAgents builds one [agents.Runtime] for every agent-facing protocol this
// server speaks and starts it.
//
// One runtime rather than one per protocol is what keeps the two honest with
// each other: the identity on an agent card and the identity an MCP client is
// told are the same string because they came from the same config, and the
// shutdown that drains one drains the other.
//
// It is called from startGRPCServer rather than from Start, and the ordering is
// load-bearing. A2A's gRPC binding registers on this server, and gRPC refuses a
// registration once Serve has been called — so the runtime has to be up before
// the server starts accepting. MCP's listener opening a moment earlier than it
// used to costs nothing.
//
// The MCP and A2A ports stay separate, as they have always been: each service
// is registered with a listen address of its own rather than sharing the
// runtime's. A process that wants them on one port builds a runtime directly
// instead of going through a HybridServer.
func (s *HybridServer) startAgents() error {
	var services []agents.Service

	if s.opts.EnableMCP && len(s.mcpServiceFuncs) > 0 {
		shared.Telemetry().Logger.Debugf("agents: registering %d MCP service(s) on %s:%d, transport %q",
			len(s.mcpServiceFuncs), s.opts.MCP.Host, s.opts.MCP.Port, s.opts.MCP.Transport)
		services = append(services, mcp.ServiceGroup(
			mcpFuncs(s.mcpServiceFuncs),
			mcp.ServiceAddr(fmt.Sprintf("%s:%d", s.opts.MCP.Host, s.opts.MCP.Port)),
			mcp.ServiceTransports(s.mcpTransport()),
			// Push the server's unary interceptor chain (including the one
			// WithValidation resolved just above) down to MCP tool dispatch, so
			// an MCP call runs the same middleware as the wire RPC behind it.
			mcp.ServiceInterceptors(s.unaryInts...),
		))
	}

	if s.opts.EnableA2A && len(s.a2aServiceFuncs) > 0 {
		shared.Telemetry().Logger.Debugf("agents: registering %d A2A service(s) on %s:%d",
			len(s.a2aServiceFuncs), s.opts.A2A.Host, s.opts.A2A.Port)
		addr := fmt.Sprintf("%s:%d", s.opts.A2A.Host, s.opts.A2A.Port)
		for _, fn := range s.a2aServiceFuncs {
			services = append(services, a2a.ServiceFrom(a2a.ServiceFunc(fn),
				a2a.ServiceAddr(addr),
				a2a.ServiceTransports(s.a2aTransports()...),
				a2a.ServiceBasePath(s.opts.A2A.BasePath),
			))
		}
	}

	if len(services) == 0 {
		shared.Telemetry().Logger.Debugf("agents: nothing registered, no runtime started")
		return nil
	}

	rt := agents.New(agents.Config{
		Name:        s.opts.ServiceName,
		Description: s.opts.Description,
		Version:     s.opts.Version,
		Host:        s.opts.A2A.Host,
		Port:        s.opts.A2A.Port,
		PublicURL:   s.opts.A2A.PublicURL,
		GRPCServer:  s.grpcServer,
	}).Register(services...)

	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("starting agent protocols: %w", err)
	}

	s.agentRuntime = rt
	s.agentCancel = cancel
	shared.Telemetry().Logger.Debugf("agents: runtime up with %d endpoint(s)", len(rt.Endpoints()))
	return nil
}

// stopAgents drains the agent runtime, if one came up.
func (s *HybridServer) stopAgents() error {
	rt, cancel := s.agentRuntime, s.agentCancel
	s.agentRuntime, s.agentCancel = nil, nil

	if cancel != nil {
		cancel()
	}
	if rt == nil {
		return nil
	}
	shared.Telemetry().Logger.Info("Shutting down agent protocols...")
	return rt.Shutdown(context.Background())
}

// agentEndpoints reports where every agent protocol came up, for the banner.
func (s *HybridServer) agentEndpoints() []agents.Endpoint {
	if s.agentRuntime == nil {
		return nil
	}
	return s.agentRuntime.Endpoints()
}

// mcpFuncs adapts the registered service funcs to the runtime's shape. The two
// signatures are identical; the conversion exists so grpc's public type stays
// grpc's.
func mcpFuncs(fns []MCPServiceFunc) []mcp.ServiceFunc {
	out := make([]mcp.ServiceFunc, 0, len(fns))
	for _, fn := range fns {
		out = append(out, mcp.ServiceFunc(fn))
	}
	return out
}

// mcpTransport is the configured MCP transport, defaulting to streamable HTTP.
func (s *HybridServer) mcpTransport() mcp.Transport {
	if s.opts.MCP.Transport == "" {
		shared.Telemetry().Logger.Debugf("agents: no MCP transport specified, defaulting to streamable-http")
		return mcp.TransportStreamableHTTP
	}
	return mcp.Transport(s.opts.MCP.Transport)
}

// a2aTransports translates the server's configured transports into the
// runtime's own, defaulting to JSON-RPC when nothing is named.
func (s *HybridServer) a2aTransports() []a2a.Transport {
	configured := s.opts.A2A.Transports
	if len(configured) == 0 && s.opts.A2A.Transport != "" {
		configured = []options.A2ATransport{s.opts.A2A.Transport}
	}
	if len(configured) == 0 {
		shared.Telemetry().Logger.Debugf("agents: no A2A transport specified, defaulting to jsonrpc")
		return []a2a.Transport{a2a.TransportJSONRPC}
	}
	out := make([]a2a.Transport, 0, len(configured))
	for _, t := range configured {
		out = append(out, a2a.Transport(t))
	}
	return out
}
