package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// HybridServer encapsulates a gRPC server and its optional HTTP/1.1 and
// experimental HTTP/3 REST gateways. It is designed to be configured using
// functional options and provides managed start, stop, and restart capabilities.
type HybridServer struct {
	opts             options.Options    // server configuration options
	grpcServer       *grpc.Server       // gRPC server instance
	httpServer       *http.Server       // HTTP/1.1 server
	mux              *runtime.ServeMux  // grpc-gateway mux
	http3Server      *http3.Server      // experimental HTTP/3 server
	mcpCancel        context.CancelFunc // cancels MCP server goroutines
	mcpHTTPServer    *http.Server       // shared MCP listener fronting every service (one port)
	mcpEndpoints     []mcpEndpointInfo  // resolved MCP endpoints (populated on start)
	grpcServiceFuncs []GRPCServiceFunc  // registered gRPC service functions
	httpServiceFuncs []HTTPServiceFunc  // registered HTTP gateway functions
	mcpServiceFuncs  []MCPServiceFunc   // registered MCP service functions
	cert             *tls.Certificate   // TLS certificate for secure connections
	unaryInts        []grpc.UnaryServerInterceptor // caller-supplied unary interceptors (chained in order)
	enableValidation bool               // prepend the protovalidate interceptor at Start (WithValidation)
	dashboardFS      fs.FS              // caller-provided FS containing *.json dashboard files
	dashboardFSDir   string             // sub-directory inside dashboardFS to scan (e.g. ".grafana")

	// restMarshal / restUnmarshal configure the grpc-gateway JSON codec. They
	// default to camelCase field names with EmitUnpopulated and are applied when
	// the mux is built (after functional options run), so options such as
	// WithRESTSnakeCase can override them.
	restMarshal   protojson.MarshalOptions
	restUnmarshal protojson.UnmarshalOptions
}

// NewHybridServer constructs a new HybridServer with the given base options.
// It automatically applies environment variable overrides and then applies any
// additional functional options for further configuration.
func NewHybridServer(opts options.Options, extraOpts ...Option) *HybridServer {
	shared.Pulse.Logger.Debugf("NewHybridServer: service=%q version=%q env=%s",
		opts.ServiceName, opts.Version, opts.Environment)

	applyEnvOverrides(&opts)
	shared.Pulse.Logger.Debugf("NewHybridServer: env overrides applied — gRPC=%s:%d HTTP=%s:%d MCP=%s:%d",
		opts.GRPC.Host, opts.GRPC.Port,
		opts.HTTP.Host, opts.HTTP.Port,
		opts.MCP.Host, opts.MCP.Port)

	s := &HybridServer{
		opts: opts,
		// Defaults: EmitUnpopulated ensures default values (false, 0, "", [])
		// appear in JSON responses so clients receive the full Grafana dashboard
		// shape; camelCase field names match the historical gateway behavior.
		restMarshal: protojson.MarshalOptions{
			EmitUnpopulated: true,
			UseProtoNames:   false, // camelCase JSON names by default
		},
		restUnmarshal: protojson.UnmarshalOptions{
			DiscardUnknown: true,
		},
	}

	shared.Pulse.Logger.Debugf("NewHybridServer: applying %d functional option(s)", len(extraOpts))
	for _, o := range extraOpts {
		o(s)
	}

	// Build the gateway mux after options run so codec overrides (e.g.
	// WithRESTSnakeCase / WithRESTMarshaler) take effect.
	s.mux = runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   s.restMarshal,
			UnmarshalOptions: s.restUnmarshal,
		}),
		// Map gRPC codes to proper HTTP statuses with a consistent JSON envelope.
		// Routing errors (404/405/400) are funneled through this handler by
		// grpc-gateway's default routing error handler.
		runtime.WithErrorHandler(httpErrorHandler),
	)
	shared.Pulse.Logger.Debugf("NewHybridServer: grpc-gateway mux created (EmitUnpopulated=%t, UseProtoNames=%t)",
		s.restMarshal.EmitUnpopulated, s.restMarshal.UseProtoNames)

	if opts.ExperimentalHttp3 {
		shared.Pulse.Logger.Debugf("NewHybridServer: HTTP/3 experimental enabled — pre-creating http3.Server on port %d", opts.HTTP.Port+1)
		s.http3Server = &http3.Server{
			Addr:    fmt.Sprintf("%s:%d", opts.HTTP.Host, opts.HTTP.Port+1),
			Handler: s.mux,
		}
	}

	return s
}

// WithGRPCServers returns a server Option that registers one or more gRPC
// services. These registration functions are called during server startup.
func WithGRPCServers(services ...GRPCServiceFunc) Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithGRPCServers: appending %d gRPC service func(s)", len(services))
		s.grpcServiceFuncs = append(s.grpcServiceFuncs, services...)
	}
}

// WithHTTPGateways returns a server Option that registers one or more HTTP
// gateway handlers. These handlers proxy RESTful JSON requests to their
// corresponding gRPC services.
func WithHTTPGateways(services ...HTTPServiceFunc) Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithHTTPGateways: appending %d HTTP gateway func(s)", len(services))
		s.httpServiceFuncs = append(s.httpServiceFuncs, services...)
	}
}

// WithUnaryInterceptors returns a server Option that installs one or more
// unary server interceptors on the gRPC server, chained in the order given
// (after the built-in OpenTelemetry stats handler). Use it for cross-cutting
// request middleware such as protovalidate request validation or auth.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithUnaryInterceptors: appending %d unary interceptor(s)", len(interceptors))
		s.unaryInts = append(s.unaryInts, interceptors...)
	}
}

// WithRESTSnakeCase returns a server Option that makes the HTTP/REST gateway
// emit and accept snake_case JSON field names (proto field names) instead of
// the default camelCase. This matches the field naming used by the MCP layer.
func WithRESTSnakeCase() Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithRESTSnakeCase: enabling snake_case JSON field names")
		s.restMarshal.UseProtoNames = true
	}
}

// WithRESTMarshaler returns a server Option that fully overrides the protojson
// marshal and unmarshal options used by the HTTP/REST gateway, for callers that
// need finer control than WithRESTSnakeCase (e.g. toggling EmitUnpopulated).
func WithRESTMarshaler(marshal protojson.MarshalOptions, unmarshal protojson.UnmarshalOptions) Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithRESTMarshaler: overriding gateway codec (EmitUnpopulated=%t, UseProtoNames=%t, DiscardUnknown=%t)",
			marshal.EmitUnpopulated, marshal.UseProtoNames, unmarshal.DiscardUnknown)
		s.restMarshal = marshal
		s.restUnmarshal = unmarshal
	}
}

// WithMCPServices returns a server Option that registers one or more MCP
// service functions. Each function is started in its own goroutine and bound
// to its own port, incrementing from opts.MCP.Port.
func WithMCPServices(services ...MCPServiceFunc) Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithMCPServices: appending %d MCP service func(s)", len(services))
		s.mcpServiceFuncs = append(s.mcpServiceFuncs, services...)
	}
}

// WithCertificates returns a server Option that loads a TLS certificate and
// key from the specified files. This enables TLS for both gRPC and HTTP servers.
// The function will panic if the certificate files cannot be loaded.
func WithCertificates(certFile, keyFile string) Option {
	return func(s *HybridServer) {
		shared.Pulse.Logger.Debugf("WithCertificates: loading cert=%s key=%s", certFile, keyFile)
		cert := mustLoadCertificate(certFile, keyFile)
		s.cert = &cert
		shared.Pulse.Logger.Debugf("WithCertificates: certificate loaded successfully")
	}
}

// WithGrafanaFS registers an fs.FS (typically an embed.FS) whose dir directory
// is scanned for *.json Grafana dashboard files at server startup. Every JSON
// file found is parsed and loaded into the MemoryDashboardStore automatically.
//
// Typical usage:
//
//	//go:embed all:.grafana
//	var dashboardFiles embed.FS
//
//	server := grpc.NewHybridServer(opts, ...)
//	server.WithGrafanaFS(dashboardFiles, ".grafana")
func (s *HybridServer) WithGrafanaFS(fsys fs.FS, dir string) *HybridServer {
	shared.Pulse.Logger.Debugf("WithGrafanaFS: registering dashboard FS, dir=%q", dir)
	s.dashboardFS = fsys
	s.dashboardFSDir = dir
	return s
}

// Start validates the server configuration and launches the gRPC server and any
// enabled gateways (HTTP/1.1, experimental HTTP/3). Each server component runs
// in its own goroutine. Once all components are up a startup summary table is
// printed to stdout.
func (s *HybridServer) Start() error {
	shared.Pulse.Logger.Debugf("Start: validating server options")
	if err := s.validateOptions(); err != nil {
		return err
	}
	shared.Pulse.Logger.Debugf("Start: options valid — gRPC=%s:%d enableHTTP=%v enableMCP=%v",
		s.opts.GRPC.Host, s.opts.GRPC.Port,
		s.opts.EnableHTTP, s.opts.EnableMCP)

	shared.Pulse.Logger.Debugf("Start: starting gRPC server")
	if err := s.startGRPCServer(); err != nil {
		return err
	}

	if s.opts.ExperimentalHttp3 {
		shared.Pulse.Logger.Debugf("Start: starting experimental HTTP/3 server")
		s.startHTTP3ExperimentalServer()
	}

	if s.opts.EnableMCP {
		shared.Pulse.Logger.Debugf("Start: starting MCP server(s)")
		s.startMCPServer()
	} else {
		shared.Pulse.Logger.Debugf("Start: MCP disabled (EnableMCP=false)")
	}

	if s.opts.EnableHTTP {
		shared.Pulse.Logger.Debugf("Start: starting HTTP/1.1 gateway")
		if err := s.startHTTPGateway(); err != nil {
			return err
		}
	} else {
		shared.Pulse.Logger.Debugf("Start: HTTP/1.1 gateway disabled (EnableHTTP=false)")
	}

	s.printStartupBanner(s.mcpEndpoints)
	return nil
}

func (s *HybridServer) Close() error {
	shared.Pulse.Logger.Debugf("Close: initiating graceful close")
	go func() {
		_ = shared.Close()
	}()
	return s.Stop()
}

// Stop gracefully shuts down all running servers, allowing in-flight requests
// to complete before closing connections.
func (s *HybridServer) Stop() error {
	shared.Pulse.Logger.Info("Shutting down servers...")
	if s.grpcServer != nil {
		shared.Pulse.Logger.Debugf("Stop: calling GracefulStop on gRPC server")
		s.grpcServer.GracefulStop()
		shared.Pulse.Logger.Debugf("Stop: gRPC server stopped")
	}
	if s.httpServer != nil {
		shared.Pulse.Logger.Debugf("Stop: shutting down HTTP/1.1 server")
		if err := s.httpServer.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
		shared.Pulse.Logger.Debugf("Stop: HTTP/1.1 server stopped")
	}
	if s.mcpCancel != nil {
		shared.Pulse.Logger.Info("Shutting down MCP server...")
		s.mcpCancel()
		shared.Pulse.Logger.Debugf("Stop: MCP context canceled")
	}
	if s.mcpHTTPServer != nil {
		shared.Pulse.Logger.Debugf("Stop: shutting down shared MCP HTTP server")
		if err := s.mcpHTTPServer.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("failed to shutdown MCP HTTP server: %w", err)
		}
		s.mcpHTTPServer = nil
		shared.Pulse.Logger.Debugf("Stop: MCP HTTP server stopped")
	}
	return nil
}

// Restart gracefully stops and then starts the server again. This is useful
// for applying configuration reloads or performing hot restarts without killing
// the main process.
func (s *HybridServer) Restart() error {
	shared.Pulse.Logger.Info("Restarting servers...")
	shared.Pulse.Logger.Debugf("Restart: stopping all components before restart")
	if err := s.Stop(); err != nil {
		return fmt.Errorf("failed to stop servers during restart: %w", err)
	}
	shared.Pulse.Logger.Debugf("Restart: all components stopped, starting again")
	return s.Start()
}
