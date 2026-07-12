package grpc

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/the-protobuf-project/grpc-mcp-gateway/runtime"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// mcpEndpointInfo carries resolved endpoint details from the OnReady callback.
type mcpEndpointInfo struct {
	transport string // The transport type (e.g., "streamable-http", "stdio", etc.)
	url       string // The full URL of the resolved endpoint (e.g., "http://localhost:65021/mcp")
	port      int    // The port this MCP service is bound to.
}

// startMCPServer serves every registered MCPServiceFunc from ONE listener on
// opts.MCP.Port: each service mounts its handlers on a shared mux at its
// proto-derived base path (paths are unique per service), mirroring how gRPC
// multiplexes services on one server. The service funcs run in goroutines tied
// to a shared cancellable context; the HTTP server itself is owned here and
// drained in Stop.
func (s *HybridServer) startMCPServer() {
	shared.Pulse.Logger.Debugf("MCP: starting %d service(s) on port %d, transport %q",
		len(s.mcpServiceFuncs), s.opts.MCP.Port, s.opts.MCP.Transport)

	ctx, cancel := context.WithCancel(context.Background())
	s.mcpCancel = cancel

	mux := http.NewServeMux()
	readyCh := make(chan mcpEndpointInfo, len(s.mcpServiceFuncs))

	for i, fn := range s.mcpServiceFuncs {
		cfg := s.buildMCPConfig(mux)
		cfg.OnReady = func(resolved *runtime.MCPServerConfig) {
			if ep, err := runtime.ServerEndpoint(resolved); err == nil {
				shared.Pulse.Logger.Debugf("MCP: service ready — transport=%s url=%s", ep.Transport, ep.URL)
				readyCh <- mcpEndpointInfo{transport: ep.Transport, url: ep.URL, port: s.opts.MCP.Port}
			} else {
				shared.Pulse.Logger.Debugf("MCP: ServerEndpoint error: %v", err)
			}
		}
		shared.Pulse.Logger.Debugf("MCP: launching service goroutine [%d]", i)
		go func(f MCPServiceFunc, c *runtime.MCPServerConfig) {
			if err := f(ctx, c); err != nil && ctx.Err() == nil {
				shared.Pulse.Logger.Errorf("MCP server error: %v", err)
			}
		}(fn, cfg)
	}

	shared.Pulse.Logger.Debugf("MCP: waiting for %d service(s) to mount (timeout 5s each)",
		len(s.mcpServiceFuncs))
	for range s.mcpServiceFuncs {
		select {
		case ep := <-readyCh:
			s.mcpEndpoints = append(s.mcpEndpoints, ep)
		case <-time.After(5 * time.Second):
			shared.Pulse.Logger.Warn("MCP service did not become ready in time")
		}
	}

	// All services are mounted; open the one listener that fronts them all.
	s.mcpHTTPServer = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", s.opts.MCP.Host, s.opts.MCP.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.mcpHTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			shared.Pulse.Logger.Errorf("MCP HTTP server stopped: %v", err)
		}
	}()
	shared.Pulse.Logger.Debugf("MCP: shared server listening on %s (%d endpoint(s))",
		s.mcpHTTPServer.Addr, len(s.mcpEndpoints))
}

// buildMCPConfig constructs the per-service MCPServerConfig: shared mux and
// addr (one port for all services), the server's unary interceptor chain, and
// the service identity.
func (s *HybridServer) buildMCPConfig(mux *http.ServeMux) *runtime.MCPServerConfig {
	transport := runtime.Transport(s.opts.MCP.Transport)
	if transport == "" {
		shared.Pulse.Logger.Debugf("MCP: no transport specified, defaulting to streamable-http")
		transport = runtime.TransportStreamableHTTP
	}

	return &runtime.MCPServerConfig{
		Name:       s.opts.ServiceName,
		Version:    s.opts.Version,
		Transports: []runtime.Transport{transport},
		Addr:       fmt.Sprintf("%s:%d", s.opts.MCP.Host, s.opts.MCP.Port),
		Mux:        mux,
		// Push the server's unary interceptor chain (incl. WithValidation,
		// resolved during startGRPCServer) down to MCP tool dispatch, so MCP
		// calls run the same middleware as wire RPCs.
		UnaryInterceptor: runtime.ChainUnaryInterceptors(s.unaryInts...),
	}
}
