package grpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"

	"github.com/quic-go/quic-go/http3"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// startHTTP3ExperimentalServer initializes and runs an experimental HTTP/3 server,
// serving the same grpc-gateway endpoints over the QUIC protocol.
//
// This server listens on all network interfaces on the configured HTTP port + 1.
// A valid TLS certificate must be provided, as HTTP/3 requires TLS 1.3.
// The server is launched in a separate goroutine.
func (s *HybridServer) startHTTP3ExperimentalServer() {
	addr := fmt.Sprintf(":%d", s.opts.HTTP.Port+1)
	shared.Telemetry().Logger.Debugf("HTTP/3: initializing experimental QUIC server on %s", addr)

	tlsConf := &tls.Config{MinVersion: tls.VersionTLS13}
	if s.cert != nil {
		shared.Telemetry().Logger.Debugf("HTTP/3: loading TLS certificate into QUIC config")
		tlsConf.Certificates = []tls.Certificate{*s.cert}
	} else {
		shared.Telemetry().Logger.Warn("HTTP/3: no certificate configured; server will fail to start")
	}

	shared.Telemetry().Logger.Debugf("HTTP/3: configuring ALPN tokens for QUIC")
	http3.ConfigureTLSConfig(tlsConf)

	s.http3Server = &http3.Server{
		Addr:      addr,
		Handler:   s.mux,
		TLSConfig: tlsConf,
	}
	shared.Telemetry().Logger.Debugf("HTTP/3: server instance created, reusing grpc-gateway mux")

	shared.Telemetry().Logger.Debugf("HTTP/3: ensuring dashboard gateway on shared mux")

	go func() {
		shared.Telemetry().Logger.Warnf("Starting experimental HTTP/3 server on %s", addr)
		if err := s.http3Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = shared.Telemetry().Logger.Errorf("HTTP/3 server failed: %v", err)
		}
	}()
}
