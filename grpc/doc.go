// Package grpc provides HybridServer — a single server that simultaneously
// speaks gRPC, HTTP/1.1 JSON gateway (via grpc-gateway), optional HTTP/3
// (QUIC), an MCP (Model Context Protocol) endpoint, and an A2A (Agent2Agent)
// endpoint, all sharing the same port and TLS configuration.
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
//	    GRPC:        options.Endpoint{Host: "0.0.0.0", Port: 50051},
//	    HTTP:        options.Endpoint{Host: "0.0.0.0", Port: 8080},
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
// # A2A (Agent2Agent)
//
// Set [options.Options.EnableA2A] and register an agent with [WithA2AAgent] to
// serve one alongside everything else:
//
//	srv := grpc.NewHybridServer(opts, grpc.WithA2AAgent(
//	    a2a.TextAgent(func(ctx context.Context, text string) (string, error) {
//	        return "you said: " + text, nil
//	    }),
//	    grpc.A2ASkill{ID: "echo", Name: "Echo", Description: "Repeats the input"},
//	))
//
// Which transports it answers on is [options.A2AOptions.Transports]. The
// JSON-RPC and REST bindings share one listener on [options.A2AOptions.Port],
// with the public agent card at /.well-known/agent-card.json. The gRPC binding
// is different in kind: it registers on this server's own gRPC port, so an
// agent is a service among the others and runs through the same interceptor
// chain. That is also why A2A services mount during gRPC setup rather than
// after it — gRPC refuses a registration once it is serving.
//
// The agent's identity on its card comes from the server's own ServiceName,
// Description and Version, so the card cannot disagree with the service.
// See [github.com/the-protobuf-project/runtime-go/agents/a2a] for the runtime
// and [WithA2AServices] for the generated-entrypoint form.
//
// # HTTP/3 (QUIC) — experimental
//
// Set [options.Options.H3] to enable HTTP/3. Requires TLS. The Alt-Svc header
// is injected into HTTP/1.1 responses so compliant clients can upgrade.
package grpc
