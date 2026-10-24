package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// startHTTPGateway initializes and runs the HTTP reverse-proxy gateway.
// This gateway exposes registered gRPC services as RESTful JSON endpoints.
// It connects to the backend gRPC server and starts an HTTPS server if a
// TLS certificate is provided. The server is launched in a separate goroutine.
func (s *HybridServer) startHTTPGateway() error {
	httpAddr := fmt.Sprintf("%s:%d", s.opts.HTTP.Host, s.opts.HTTP.Port)
	grpcEndpoint := fmt.Sprintf("%s:%d", s.opts.GRPC.Host, s.opts.GRPC.Port)
	shared.Telemetry().Logger.Debugf("HTTP/1.1: gateway will proxy to gRPC endpoint %s", grpcEndpoint)

	var dialOpts []grpc.DialOption
	if s.cert != nil {
		shared.Telemetry().Logger.Debugf("HTTP/1.1: dialing gRPC with TLS (InsecureSkipVerify for dev)")
		tlsConf := &tls.Config{
			InsecureSkipVerify: true, // dev only; swap for RootCAs in prod
			ServerName:         "localhost",
			NextProtos:         []string{"h2"},
			MinVersion:         tls.VersionTLS13,
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else {
		shared.Telemetry().Logger.Debugf("HTTP/1.1: dialing gRPC with plaintext (no TLS)")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Whatever endpoint the handlers dial, their connections land on the
	// in-memory gateway listener, which outlives HTTP draining on Stop.
	dialOpts = append(dialOpts, s.gatewayDialOption())

	shared.Telemetry().Logger.Debugf("HTTP/1.1: registering %d HTTP gateway handler(s)", len(s.httpServiceFuncs))
	for i, svc := range s.httpServiceFuncs {
		shared.Telemetry().Logger.Debugf("HTTP/1.1: registering gateway handler [%d]", i)
		if err := svc(s.mux, grpcEndpoint, dialOpts); err != nil {
			return fmt.Errorf("failed to register HTTP gateway: %w", err)
		}
	}

	shared.Telemetry().Logger.Debugf("HTTP/1.1: registering GET /health endpoint")
	if err := s.registerHealthzHandler(s.mux); err != nil {
		return fmt.Errorf("failed to register /health endpoint: %w", err)
	}

	// Bind before returning, so a taken port fails Start and Stop always has a
	// listener to close.
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on HTTP address: %w", err)
	}

	s.httpServer = &http.Server{
		Addr:              httpAddr,
		Handler:           s.serveHandler(),
		ReadHeaderTimeout: 30 * time.Second,
	}
	if s.cert != nil {
		shared.Telemetry().Logger.Debugf("HTTP/1.1: starting HTTPS server on %s (TLS 1.3)", httpAddr)
		s.httpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{*s.cert},
			MinVersion:   tls.VersionTLS13,
		}
		go func(srv *http.Server) {
			if err := srv.ServeTLS(lis, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				shared.Telemetry().Logger.Fatalf("HTTPS server stopped: %v", err)
			}
		}(s.httpServer)
	} else {
		shared.Telemetry().Logger.Debugf("HTTP/1.1: starting HTTP server on %s (plaintext)", httpAddr)
		go func(srv *http.Server) {
			if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				shared.Telemetry().Logger.Fatalf("HTTP server stopped: %v", err)
			}
		}(s.httpServer)
	}
	return nil
}

// registerHealthzHandler adds GET /health to the grpc-gateway mux.
//
// The handler is the same one [serveHandler] mounts for a replaced gateway, so the endpoint
// answers identically whichever HTTP layer is in front of it.
func (s *HybridServer) registerHealthzHandler(gw *runtime.ServeMux) error {
	healthz := healthzHandler()
	return gw.HandlePath("GET", "/health",
		func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
			healthz(w, r)
		},
	)
}
