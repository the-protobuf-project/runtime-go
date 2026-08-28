package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/quic-go/quic-go/http3"
	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// HybridServer encapsulates a gRPC server and its optional HTTP/1.1 and
// experimental HTTP/3 REST gateways. It is designed to be configured using
// functional options and provides managed start, stop, and restart capabilities.
type HybridServer struct {
	opts             options.Options               // server configuration options
	grpcServer       *grpc.Server                  // gRPC server instance
	httpServer       *http.Server                  // HTTP/1.1 server
	mux              *runtime.ServeMux             // grpc-gateway mux
	httpHandler      http.Handler                  // replaces the gateway mux when set (WithHTTPHandler)
	http3Server      *http3.Server                 // experimental HTTP/3 server
	agentRuntime     *agents.Runtime               // the one runtime serving MCP and A2A (nil when neither is on)
	agentCancel      context.CancelFunc            // stops the agent protocols' goroutines
	grpcServiceFuncs []GRPCServiceFunc             // registered gRPC service functions
	httpServiceFuncs []HTTPServiceFunc             // registered HTTP gateway functions
	mcpServiceFuncs  []MCPServiceFunc              // registered MCP service functions
	a2aServiceFuncs  []A2AServiceFunc              // registered A2A service functions
	cert             *tls.Certificate              // TLS certificate for secure connections
	unaryInts        []grpc.UnaryServerInterceptor // caller-supplied unary interceptors (chained in order)
	enableValidation bool                          // prepend the protovalidate interceptor at Start (WithValidation)
	dashboardFS      fs.FS                         // caller-provided FS containing *.json dashboard files
	dashboardFSDir   string                        // sub-directory inside dashboardFS to scan (e.g. ".grafana")

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
	shared.Telemetry().Logger.Debugf("NewHybridServer: service=%q version=%q env=%s",
		opts.ServiceName, opts.Version, opts.Environment)

	applyEnvOverrides(&opts)
	shared.Telemetry().Logger.Debugf("NewHybridServer: env overrides applied — gRPC=%s:%d HTTP=%s:%d MCP=%s:%d",
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

	shared.Telemetry().Logger.Debugf("NewHybridServer: applying %d functional option(s)", len(extraOpts))
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
	shared.Telemetry().Logger.Debugf("NewHybridServer: grpc-gateway mux created (EmitUnpopulated=%t, UseProtoNames=%t)",
		s.restMarshal.EmitUnpopulated, s.restMarshal.UseProtoNames)

	if opts.ExperimentalHttp3 {
		shared.Telemetry().Logger.Debugf("NewHybridServer: HTTP/3 experimental enabled — pre-creating http3.Server on port %d", opts.HTTP.Port+1)
		s.http3Server = &http3.Server{
			Addr:    fmt.Sprintf("%s:%d", opts.HTTP.Host, opts.HTTP.Port+1),
			Handler: s.serveHandler(),
		}
	}

	return s
}
