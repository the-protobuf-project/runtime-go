package grpc

import (
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
)

// startGRPCServer initializes and runs the gRPC server component. It configures
// the listener, automatically enables TLS if a certificate is provided, and
// registers health, reflection, and user-defined services. The server itself is
// launched in a separate goroutine to avoid blocking.
func (s *HybridServer) startGRPCServer() error {
	grpcAddr := fmt.Sprintf("%s:%d", s.opts.GRPC.Host, s.opts.GRPC.Port)
	shared.Pulse.Logger.Debugf("gRPC: binding TCP listener on %s", grpcAddr)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC address: %w", err)
	}
	shared.Pulse.Logger.Debugf("gRPC: TCP listener ready on %s", grpcAddr)

	var serverOpts []grpc.ServerOption
	if s.cert != nil {
		shared.Pulse.Logger.Debugf("gRPC: TLS enabled — configuring TLS 1.3 credentials")
		tlsConf := &tls.Config{
			Certificates: []tls.Certificate{*s.cert},
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{"h2"},
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConf)))
	} else {
		shared.Pulse.Logger.Debugf("gRPC: no certificate — running plaintext (no TLS)")
	}

	shared.Pulse.Logger.Debugf("gRPC: setting up OpenTelemetry exporter")
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
		shared.Pulse.Logger.Debugf("gRPC: chaining %d unary interceptor(s)", len(s.unaryInts))
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(s.unaryInts...))
	}
	shared.Pulse.Logger.Debugf("gRPC: %d server option(s) configured (TLS + OTel)", len(serverOpts))

	s.grpcServer = grpc.NewServer(serverOpts...)
	shared.Pulse.Logger.Debugf("gRPC: server instance created")

	s.registerHealth()
	s.registerReflection()
	s.registerGRPCServices()

	shared.Pulse.Logger.Debugf("gRPC: starting server goroutine on %s", grpcAddr)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			shared.Pulse.Logger.Fatalf("gRPC server stopped: %v", err)
		}
	}()

	return nil
}

// registerHealth enables the standard gRPC health checking service if specified
// in the server options. It sets the default status for the main service to SERVING.
func (s *HybridServer) registerHealth() {
	if s.opts.EnableHealth {
		shared.Pulse.Logger.Debugf("gRPC: registering health check service (grpc.health.v1)")
		healthServer := health.NewServer()
		grpc_health_v1.RegisterHealthServer(s.grpcServer, healthServer)
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		shared.Pulse.Logger.Debugf("gRPC: health check status set to SERVING")
	} else {
		shared.Pulse.Logger.Debugf("gRPC: health check disabled (EnableHealth=false)")
	}
}

// registerReflection enables the gRPC reflection service, which allows clients
// like grpcurl to query the server's available services. This is only enabled
// in Debug or Development environments for security.
func (s *HybridServer) registerReflection() {
	if s.opts.Environment == options.Debug || s.opts.Environment == options.Development {
		shared.Pulse.Logger.Debugf("gRPC: registering reflection service (env=%s)", s.opts.Environment)
		reflection.Register(s.grpcServer)
	} else {
		shared.Pulse.Logger.Debugf("gRPC: reflection disabled (env=%s)", s.opts.Environment)
	}
}

// registerGRPCServices iterates through the list of user-provided service
// registration functions and applies them to the gRPC server instance.
func (s *HybridServer) registerGRPCServices() {
	shared.Pulse.Logger.Debugf("gRPC: registering %d user service(s)", len(s.grpcServiceFuncs))
	for i, svc := range s.grpcServiceFuncs {
		shared.Pulse.Logger.Debugf("gRPC: applying service registration function [%d]", i)
		svc(s.grpcServer)
	}
}
