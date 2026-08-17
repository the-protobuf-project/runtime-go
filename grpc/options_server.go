package grpc

import (
	"context"
	"io/fs"

	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// WithGRPCServers returns a server Option that registers one or more gRPC
// services. These registration functions are called during server startup.
func WithGRPCServers(services ...GRPCServiceFunc) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithGRPCServers: appending %d gRPC service func(s)", len(services))
		s.grpcServiceFuncs = append(s.grpcServiceFuncs, services...)
	}
}

// WithHTTPGateways returns a server Option that registers one or more HTTP
// gateway handlers. These handlers proxy RESTful JSON requests to their
// corresponding gRPC services.
func WithHTTPGateways(services ...HTTPServiceFunc) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithHTTPGateways: appending %d HTTP gateway func(s)", len(services))
		s.httpServiceFuncs = append(s.httpServiceFuncs, services...)
	}
}

// WithUnaryInterceptors returns a server Option that installs one or more
// unary server interceptors on the gRPC server, chained in the order given
// (after the built-in OpenTelemetry stats handler). Use it for cross-cutting
// request middleware such as protovalidate request validation or auth.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithUnaryInterceptors: appending %d unary interceptor(s)", len(interceptors))
		s.unaryInts = append(s.unaryInts, interceptors...)
	}
}

// WithRESTSnakeCase returns a server Option that makes the HTTP/REST gateway
// emit and accept snake_case JSON field names (proto field names) instead of
// the default camelCase. This matches the field naming used by the MCP layer.
func WithRESTSnakeCase() Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithRESTSnakeCase: enabling snake_case JSON field names")
		s.restMarshal.UseProtoNames = true
	}
}

// WithRESTMarshaler returns a server Option that fully overrides the protojson
// marshal and unmarshal options used by the HTTP/REST gateway, for callers that
// need finer control than WithRESTSnakeCase (e.g. toggling EmitUnpopulated).
func WithRESTMarshaler(marshal protojson.MarshalOptions, unmarshal protojson.UnmarshalOptions) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithRESTMarshaler: overriding gateway codec (EmitUnpopulated=%t, UseProtoNames=%t, DiscardUnknown=%t)",
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
		shared.Telemetry().Logger.Debugf("WithMCPServices: appending %d MCP service func(s)", len(services))
		s.mcpServiceFuncs = append(s.mcpServiceFuncs, services...)
	}
}

// WithA2AServices returns a server Option that registers one or more
// Agent2Agent service functions. Each is started in its own goroutine with a
// config the server builds: the HTTP transports share one listener on
// opts.A2A.Port, and the gRPC transport registers on the server's own gRPC port
// alongside every other service.
//
// This is the registration a generated ServeFooA2A uses. For a hand-written
// agent, [WithA2AAgent] is the shorter form.
func WithA2AServices(services ...A2AServiceFunc) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithA2AServices: appending %d A2A service func(s)", len(services))
		s.a2aServiceFuncs = append(s.a2aServiceFuncs, services...)
	}
}

// WithA2AAgent returns a server Option that serves agent over Agent2Agent,
// advertising skills on its agent card.
//
// The agent's identity — name, description, version — comes from the server's
// own options rather than being restated here, so the card and the service
// cannot disagree about what this process is.
//
//	srv := grpc.NewHybridServer(opts, grpc.WithA2AAgent(
//	    a2a.TextAgent(func(ctx context.Context, text string) (string, error) {
//	        return "you said: " + text, nil
//	    }),
//	    grpc.A2ASkill{ID: "echo", Name: "Echo", Description: "Repeats the input"},
//	))
func WithA2AAgent(agent A2AAgent, skills ...A2ASkill) Option {
	return WithA2AServices(func(ctx context.Context, cfg *A2AServerConfig) error {
		cfg.Skills = skills
		return a2a.StartServer(ctx, cfg, agent)
	})
}

// WithCertificates returns a server Option that loads a TLS certificate and
// key from the specified files. This enables TLS for both gRPC and HTTP servers.
// The function will panic if the certificate files cannot be loaded.
func WithCertificates(certFile, keyFile string) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithCertificates: loading cert=%s key=%s", certFile, keyFile)
		cert := mustLoadCertificate(certFile, keyFile)
		s.cert = &cert
		shared.Telemetry().Logger.Debugf("WithCertificates: certificate loaded successfully")
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
	shared.Telemetry().Logger.Debugf("WithGrafanaFS: registering dashboard FS, dir=%q", dir)
	s.dashboardFS = fsys
	s.dashboardFSDir = dir
	return s
}
