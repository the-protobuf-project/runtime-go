// Package grpc provides HybridServer — a single server that simultaneously
// speaks gRPC, HTTP/1.1 JSON gateway (via grpc-gateway), optional HTTP/3
// (QUIC), and an MCP (Model Context Protocol) endpoint, all sharing the same
// port and TLS configuration.
//
// # Overview
//
// Create a server with [NewHybridServer], register gRPC and HTTP gateway
// handlers, then call [HybridServer.Start]. The server blocks until a SIGINT
// or SIGTERM signal is received, then drains connections and exits cleanly:
//
//	srv := grpc.NewHybridServer(options.Options{
//	    ServiceName: "my-service",
//	    Environment: options.Production,
//	    GRPC:        options.GRPCOptions{Host: "0.0.0.0", Port: 50051},
//	    HTTP:        options.HTTPOptions{Host: "0.0.0.0", Port: 8080},
//	})
//
//	srv.RegisterGRPC(func(s *grpc.Server) {
//	    pb.RegisterGreeterServer(s, &myGreeter{})
//	})
//	srv.RegisterHTTP(func(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
//	    return pb.RegisterGreeterHandlerFromEndpoint(ctx, mux, endpoint, opts)
//	})
//
//	srv.Start() // blocks until signal
//
// # TLS
//
// Pass [options.TLSOptions] via [options.Options.TLS] to load certificate/key
// files. When TLS is configured, the gRPC and HTTP servers share the same
// credentials. HTTP/3 is only started when TLS is present.
//
// # OpenTelemetry
//
// When [options.Options.OTel] is non-nil, the server registers gRPC
// interceptors that emit trace spans and metrics for every RPC. The exporter
// endpoint is configured through the OTel options; the server name in spans
// comes from ServiceName.
//
// # Health checking and reflection
//
// The gRPC health service (grpc.health.v1) and server reflection are
// registered automatically. A /healthz HTTP endpoint is also added on the
// HTTP mux.
//
// # MCP (Model Context Protocol)
//
// Set [options.Options.MCP] to enable an HTTP-based MCP endpoint alongside
// the gateway. This is experimental and subject to change.
//
// # HTTP/3 (QUIC) — experimental
//
// Set [options.Options.H3] to enable HTTP/3. Requires TLS. The Alt-Svc header
// is injected into HTTP/1.1 responses so compliant clients can upgrade.
package grpc
