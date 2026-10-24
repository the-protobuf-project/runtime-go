package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
)

// startGRPCServer initializes and runs the gRPC server component. It configures
// the listener, automatically enables TLS if a certificate is provided, and
// registers health, reflection, and user-defined services. The server itself is
// launched in a separate goroutine to avoid blocking.
func (s *HybridServer) startGRPCServer() error {
	grpcAddr := fmt.Sprintf("%s:%d", s.opts.GRPC.Host, s.opts.GRPC.Port)
	shared.Telemetry().Logger.Debugf("gRPC: binding TCP listener on %s", grpcAddr)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC address: %w", err)
	}
	shared.Telemetry().Logger.Debugf("gRPC: TCP listener ready on %s", grpcAddr)

	var serverOpts []grpc.ServerOption
	if s.cert != nil {
		shared.Telemetry().Logger.Debugf("gRPC: TLS enabled — configuring TLS 1.3 credentials")
		tlsConf := &tls.Config{
			Certificates: []tls.Certificate{*s.cert},
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{"h2"},
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConf)))
	} else {
		shared.Telemetry().Logger.Debugf("gRPC: no certificate — running plaintext (no TLS)")
	}

	shared.Telemetry().Logger.Debugf("gRPC: setting up OpenTelemetry exporter")
	otelServerOpts := setupOtelExporter()
	serverOpts = append(serverOpts, otelServerOpts...)
	// Resolve WithValidation into the interceptor chain here, before either
	// consumer reads it: the gRPC server below, and startMCPServer (which runs
	// after this in Start) pushing the same chain down to MCP tool dispatch.
	if s.enableValidation {
		validate, err := NewValidationInterceptor()
		if err != nil {
			return err
		}
		s.unaryInts = append([]grpc.UnaryServerInterceptor{validate}, s.unaryInts...)
		s.enableValidation = false // resolved; Restart must not prepend twice
	}
	if len(s.unaryInts) > 0 {
		shared.Telemetry().Logger.Debugf("gRPC: chaining %d unary interceptor(s)", len(s.unaryInts))
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(s.unaryInts...))
	}
	shared.Telemetry().Logger.Debugf("gRPC: %d server option(s) configured (TLS + OTel)", len(serverOpts))

	s.grpcServer = grpc.NewServer(serverOpts...)
	shared.Telemetry().Logger.Debugf("gRPC: server instance created")

	s.registerHealth()
	s.registerReflection()
	s.registerGRPCServices()

	// The agent protocols start here rather than from Start because A2A's gRPC
	// binding is a service like any other, and gRPC refuses a registration once
	// Serve has been called.
	if err := s.startAgents(); err != nil {
		return err
	}

	// The REST gateway reaches this server over an in-memory listener rather
	// than the TCP port. Stop closes the TCP listener first, to turn new
	// clients away, while gateway requests already accepted still need a gRPC
	// backend to finish against; this listener stays open until GracefulStop.
	s.draining.Store(false)
	s.grpcListener = lis
	s.gatewayListener = bufconn.Listen(gatewayBufferSize)

	shared.Telemetry().Logger.Debugf("gRPC: starting server goroutines on %s and the in-memory gateway listener", grpcAddr)
	go s.serveGRPC(s.grpcServer, lis, grpcAddr)
	go s.serveGRPC(s.grpcServer, s.gatewayListener, "the in-memory gateway listener")

	return nil
}

// serveGRPC serves the gRPC server on lis until it stops. Serve fails when
// Stop closes the TCP listener ahead of GracefulStop; that is expected, and
// only a failure outside shutdown is fatal.
func (s *HybridServer) serveGRPC(srv *grpc.Server, lis net.Listener, name string) {
	if err := srv.Serve(lis); err != nil && !s.draining.Load() {
		shared.Telemetry().Logger.Fatalf("gRPC server on %s stopped: %v", name, err)
	}
}

// gatewayDialOption routes the REST gateway's connections to the in-memory
// listener, whatever endpoint it is handed.
func (s *HybridServer) gatewayDialOption() grpc.DialOption {
	lis := s.gatewayListener
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
}

// registerHealth enables the standard gRPC health checking service if specified
// in the server options. It sets the default status for the main service to SERVING.
func (s *HybridServer) registerHealth() {
	if s.opts.EnableHealth {
		shared.Telemetry().Logger.Debugf("gRPC: registering health check service (grpc.health.v1)")
		healthServer := health.NewServer()
		grpc_health_v1.RegisterHealthServer(s.grpcServer, healthServer)
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		shared.Telemetry().Logger.Debugf("gRPC: health check status set to SERVING")
	} else {
		shared.Telemetry().Logger.Debugf("gRPC: health check disabled (EnableHealth=false)")
	}
}

// registerReflection enables the gRPC reflection service, which allows clients
// like grpcurl to query the server's available services. This is only enabled
// in Debug or Development environments for security.
func (s *HybridServer) registerReflection() {
	if s.opts.Environment == options.Debug || s.opts.Environment == options.Development {
		shared.Telemetry().Logger.Debugf("gRPC: registering reflection service (env=%s)", s.opts.Environment)
		reflection.Register(s.grpcServer)
	} else {
		shared.Telemetry().Logger.Debugf("gRPC: reflection disabled (env=%s)", s.opts.Environment)
	}
}

// registerGRPCServices iterates through the list of user-provided service
// registration functions and applies them to the gRPC server instance.
func (s *HybridServer) registerGRPCServices() {
	shared.Telemetry().Logger.Debugf("gRPC: registering %d user service(s)", len(s.grpcServiceFuncs))
	for i, svc := range s.grpcServiceFuncs {
		shared.Telemetry().Logger.Debugf("gRPC: applying service registration function [%d]", i)
		svc(s.grpcServer)
	}
}
