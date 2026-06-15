package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/machanirobotics/grpc-mcp-gateway/runtime"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// mcpEndpointInfo carries resolved endpoint details from the OnReady callback.
type mcpEndpointInfo struct {
	transport string // The transport type (e.g., "streamable-http", "stdio", etc.)
	url       string // The full URL of the resolved endpoint (e.g., "http://localhost:65021/mcp")
	port      int    // The port this MCP service is bound to.
}

// startMCPServer builds an MCPServerConfig from the user-provided options and
// launches every registered MCPServiceFunc in its own goroutine with a shared
// cancellable context. Each service gets its own port, starting from
// opts.MCP.Port and incrementing by 1 for each additional service — mirroring
// how gRPC registers multiple services on the same server but keeping MCP
// services on separate listeners.
func (s *HybridServer) startMCPServer() {
	shared.Pulse.Logger.Debugf("MCP: starting %d service(s), base port %d, transport %q",
		len(s.mcpServiceFuncs), s.opts.MCP.Port, s.opts.MCP.Transport)

	ctx, cancel := context.WithCancel(context.Background())
	s.mcpCancel = cancel

	readyCh := make(chan mcpEndpointInfo, len(s.mcpServiceFuncs))

	for i, fn := range s.mcpServiceFuncs {
		port := s.opts.MCP.Port + i
		shared.Pulse.Logger.Debugf("MCP: building config for service [%d] on port %d", i, port)
		cfg := s.buildMCPConfigForPort(port)
		cfg.OnReady = func(resolved *runtime.MCPServerConfig) {
			if ep, err := runtime.ServerEndpoint(resolved); err == nil {
				shared.Pulse.Logger.Debugf("MCP: service ready — transport=%s url=%s port=%d",
					ep.Transport, ep.URL, port)
				readyCh <- mcpEndpointInfo{transport: ep.Transport, url: ep.URL, port: port}
			} else {
				shared.Pulse.Logger.Debugf("MCP: ServerEndpoint error for port %d: %v", port, err)
			}
		}
		shared.Pulse.Logger.Debugf("MCP: launching service goroutine [%d] on port %d", i, port)
		go func(f MCPServiceFunc, c *runtime.MCPServerConfig) {
			if err := f(ctx, c); err != nil && ctx.Err() == nil {
				shared.Pulse.Logger.Errorf("MCP server error: %v", err)
			}
		}(fn, cfg)
	}

	shared.Pulse.Logger.Debugf("MCP: waiting for %d service(s) to become ready (timeout 5s each)",
		len(s.mcpServiceFuncs))
	for range s.mcpServiceFuncs {
		select {
		case ep := <-readyCh:
			s.mcpEndpoints = append(s.mcpEndpoints, ep)
		case <-time.After(5 * time.Second):
			shared.Pulse.Logger.Warn("MCP service did not become ready in time")
		}
	}
	shared.Pulse.Logger.Debugf("MCP: all services started (%d endpoint(s) resolved)", len(s.mcpEndpoints))
}

// buildMCPConfigForPort constructs an MCPServerConfig bound to the given port.
func (s *HybridServer) buildMCPConfigForPort(port int) *runtime.MCPServerConfig {
	transport := runtime.Transport(s.opts.MCP.Transport)
	if transport == "" {
		shared.Pulse.Logger.Debugf("MCP: no transport specified, defaulting to streamable-http")
		transport = runtime.TransportStreamableHTTP
	}

	addr := fmt.Sprintf("%s:%d", s.opts.MCP.Host, port)
	shared.Pulse.Logger.Debugf("MCP: config — name=%q version=%q transport=%s addr=%s",
		s.opts.ServiceName, s.opts.Version, transport, addr)

	return &runtime.MCPServerConfig{
		Name:       s.opts.ServiceName,
		Version:    s.opts.Version,
		Transports: []runtime.Transport{transport},
		Addr:       addr,
	}
}
