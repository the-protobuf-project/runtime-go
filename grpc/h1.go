package grpc

import (
	"crypto/tls"
	"fmt"
	"math/rand"
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
	shared.Pulse.Logger.Debugf("HTTP/1.1: gateway will proxy to gRPC endpoint %s", grpcEndpoint)

	var dialOpts []grpc.DialOption
	if s.cert != nil {
		shared.Pulse.Logger.Debugf("HTTP/1.1: dialing gRPC with TLS (InsecureSkipVerify for dev)")
		tlsConf := &tls.Config{
			InsecureSkipVerify: true, // dev only; swap for RootCAs in prod
			ServerName:         "localhost",
			NextProtos:         []string{"h2"},
			MinVersion:         tls.VersionTLS13,
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else {
		shared.Pulse.Logger.Debugf("HTTP/1.1: dialing gRPC with plaintext (no TLS)")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	shared.Pulse.Logger.Debugf("HTTP/1.1: registering %d HTTP gateway handler(s)", len(s.httpServiceFuncs))
	for i, svc := range s.httpServiceFuncs {
		shared.Pulse.Logger.Debugf("HTTP/1.1: registering gateway handler [%d]", i)
		if err := svc(s.mux, grpcEndpoint, dialOpts); err != nil {
			return fmt.Errorf("failed to register HTTP gateway: %w", err)
		}
	}

	if s.cert != nil {
		shared.Pulse.Logger.Debugf("HTTP/1.1: starting HTTPS server on %s (TLS 1.3)", httpAddr)
		s.httpServer = &http.Server{
			Addr:              httpAddr,
			Handler:           s.mux,
			ReadHeaderTimeout: 30 * time.Second,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{*s.cert},
				MinVersion:   tls.VersionTLS13,
			},
		}
		go func() {
			if err := s.httpServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				shared.Pulse.Logger.Fatalf("HTTPS server stopped: %v", err)
			}
		}()
	} else {
		shared.Pulse.Logger.Debugf("HTTP/1.1: starting HTTP server on %s (plaintext)", httpAddr)
		s.httpServer = &http.Server{
			Addr:              httpAddr,
			Handler:           s.mux,
			ReadHeaderTimeout: 30 * time.Second,
		}
		go func() {
			if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				shared.Pulse.Logger.Fatalf("HTTP server stopped: %v", err)
			}
		}()
	}

	shared.Pulse.Logger.Debugf("HTTP/1.1: registering GET /health endpoint")
	if err := s.registerHealthzHandler(s.mux); err != nil {
		return fmt.Errorf("failed to register /health endpoint: %w", err)
	}
	return nil
}

// registerHealthzHandler adds GET /health to the grpc-gateway mux. It always
// returns 200 with a small JSON payload and a randomized friendly message.
func (s *HybridServer) registerHealthzHandler(gw *runtime.ServeMux) error {
	messages := []string{
		"I'm doing fine, bro. Don't worry. 🌱",
		"Still alive and kicking. 🚀",
		"All systems nominal. 👍",
		"Healthy as a horse. 🐴",
		"Feeling great, thanks for asking. 😎",
	}

	return gw.HandlePath("GET", "/health",
		func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
			shared.Pulse.Logger.Debugf("HTTP/1.1: GET /health from %s", r.RemoteAddr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"status":"ok","message":"%s"}`, messages[rand.Intn(len(messages))])
		},
	)
}
