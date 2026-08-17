package mcp

import (
	"context"
	"sync"

	"github.com/the-protobuf-project/runtime-go/agents"
	"google.golang.org/grpc"
)

// ServiceFunc is a blocking function that serves one MCP service against a
// config the caller builds — the shape protoc-gen-mcp generates as
// ServeFooMCP.
type ServiceFunc func(ctx context.Context, cfg *MCPServerConfig) error

// service is an [agents.Service] over one or more [ServiceFunc]s.
type service struct {
	funcs      []ServiceFunc
	transports []Transport
	addr       string
	basePath   string
	headers    []HeaderMapping
	unaryInts  []grpc.UnaryServerInterceptor
}

// ServiceOption adjusts what [Service] registers.
type ServiceOption func(*service)

// ServiceTransports chooses the transports the services answer on. Without it they
// speak [TransportStreamableHTTP], which is what a client that is not an IDE
// launching a subprocess expects.
func ServiceTransports(transports ...Transport) ServiceOption {
	return func(s *service) { s.transports = transports }
}

// ServiceAddr gives these services a listen address of their own rather than the
// runtime's shared one. Use it to keep MCP on a port separate from its
// siblings; without it they share, each under its own base path.
func ServiceAddr(addr string) ServiceOption {
	return func(s *service) { s.addr = addr }
}

// ServiceBasePath overrides the HTTP path the services mount under. A generated
// service carries its own proto-derived path and does not need this.
func ServiceBasePath(path string) ServiceOption {
	return func(s *service) { s.basePath = path }
}

// ServiceHeaders forwards HTTP headers into gRPC metadata for tool calls.
// See [DefaultHeaderMappings].
func ServiceHeaders(mappings ...HeaderMapping) ServiceOption {
	return func(s *service) { s.headers = mappings }
}

// ServiceInterceptors pushes a unary interceptor chain down to tool dispatch,
// so an MCP call runs the same middleware — validation, auth, tracing — as the
// wire RPC behind it.
func ServiceInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServiceOption {
	return func(s *service) { s.unaryInts = interceptors }
}

// Service registers MCP on an [agents.Runtime].
//
// fn is a generated ServeFooMCP. It mounts at its own proto-derived base path
// on the runtime's shared listener, which is also how it shares that listener
// with an A2A service registered alongside it:
//
//	rt := agents.New(agents.Config{Name: "my-service", Version: "1.0.0", Port: 9000})
//	rt.Register(mcp.Service(yourpb.ServeYourServiceMCP))
//	err := rt.Serve(ctx)
func Service(fn ServiceFunc, opts ...ServiceOption) agents.Service {
	return ServiceGroup([]ServiceFunc{fn}, opts...)
}

// ServiceGroup is [Service] for several generated functions at once. They share
// one listener and one mux, each mounting at its own proto-derived base path,
// which is what lets several MCP services live on one port.
func ServiceGroup(fns []ServiceFunc, opts ...ServiceOption) agents.Service {
	s := &service{funcs: fns, transports: []Transport{TransportStreamableHTTP}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Protocol implements [agents.Service].
func (s *service) Protocol() agents.Protocol { return agents.MCP }

// Requires implements [agents.Service]. MCP never registers on a gRPC server —
// it dispatches to one in-process — so it asks only for a place to listen, and
// only when a transport it speaks actually listens.
func (s *service) Requires() agents.Requirements {
	return agents.Requirements{Addr: s.addr, HTTP: s.servesHTTP()}
}

// servesHTTP reports whether any configured transport needs a listener. Stdio
// is the one that does not: it talks over the process's own pipes.
func (s *service) servesHTTP() bool {
	for _, t := range s.transports {
		if t != TransportStdio {
			return true
		}
	}
	return false
}

// Serve implements [agents.Service]. Every registered function runs in its own
// goroutine against a config built from the placement, and this blocks until
// ctx ends — the functions are blocking by construction, and the runtime owns
// the listener they all mount onto.
func (s *service) Serve(ctx context.Context, p agents.Placement, ready func([]agents.Endpoint)) error {
	if len(s.funcs) == 0 {
		ready(nil)
		<-ctx.Done()
		return nil
	}

	var (
		mu        sync.Mutex
		endpoints []agents.Endpoint
		mounted   sync.WaitGroup
	)
	mounted.Add(len(s.funcs))

	errCh := make(chan error, len(s.funcs))
	for _, fn := range s.funcs {
		cfg := s.config(p)
		var once sync.Once
		cfg.OnReady = func(resolved *MCPServerConfig) {
			once.Do(func() {
				if ep, err := ServerEndpoint(resolved); err == nil {
					mu.Lock()
					endpoints = append(endpoints, agents.Endpoint{
						Protocol:  agents.MCP,
						Transport: ep.Transport,
						URL:       ep.URL,
						Detail:    resolved.BasePath,
					})
					mu.Unlock()
				}
				mounted.Done()
			})
		}

		go func(f ServiceFunc, c *MCPServerConfig) {
			// A function that returns before reporting ready would otherwise
			// leave the wait below hanging until the runtime's timeout.
			defer once.Do(mounted.Done)
			errCh <- f(ctx, c)
		}(fn, cfg)
	}

	mounted.Wait()

	// A function that failed on the way up is the start's problem; one that
	// fails later is this service's alone, and ctx ending is not a failure.
	select {
	case err := <-errCh:
		if err != nil && ctx.Err() == nil {
			return err
		}
	default:
	}

	mu.Lock()
	out := endpoints
	mu.Unlock()
	ready(out)

	<-ctx.Done()
	return nil
}

// config builds the per-function server config from where the runtime put us.
func (s *service) config(p agents.Placement) *MCPServerConfig {
	return &MCPServerConfig{
		Name:             p.Name,
		Version:          p.Version,
		Transports:       s.transports,
		Addr:             p.Addr,
		BasePath:         s.basePath,
		Mux:              p.Mux,
		HeaderMappings:   s.headers,
		UnaryInterceptor: ChainUnaryInterceptors(s.unaryInts...),
	}
}
