// Package mcp provides the Go runtime for the code protoc-gen-mcp
// generates: server configuration, transport handling, and MCP integration
// helpers.
//
// It lives here rather than alongside the generator so that the-protobuf-project/mcp
// ships only the plugin and its annotations, and a service binary depends on
// the runtime alone.
//
// It depends on no build of the MCP annotations. The one proto type it reads,
// MCPProgress, is accepted as the [ProgressMessage] interface, so any generated
// MCPProgress satisfies it whatever version of the schema produced it. That is
// not only convenience: Go's protobuf registry rejects two packages claiming
// the same extension numbers, so pinning one build here would make any binary
// that links a differently-built MCPProgress panic during package init.
//
// The upstream SDK types a caller needs are re-exported from sdk.go, so
// generated code and service binaries import this package alone and never name
// the SDK. See that file for why the re-exports are type aliases.
//
// This package and the upstream SDK it is built on are both named mcp. The
// files that reach for the SDK directly import it unaliased, which is legal
// because a package never refers to itself by name; unqualified identifiers are
// this package's and mcp.Tool is the SDK's.
//
// # Install
//
//	go get github.com/the-protobuf-project/runtime-go/agents/mcp
//
// # Quick Start
//
// Use with generated Serve*MCP functions from protoc-gen-mcp. The runtime owns
// the listener and blocks, which is what a process serving MCP and nothing else
// wants:
//
//	cfg := &mcp.MCPServerConfig{
//	    Name:             "my-service",
//	    Version:          "1.0.0",
//	    Transports:       []mcp.Transport{mcp.TransportStreamableHTTP},
//	    Addr:             ":8082",
//	    GeneratedBasePath: yourpb.YourServiceMCPDefaultBasePath,
//	}
//	err := yourpb.ServeYourServiceMCP(ctx, yourGRPCServerImpl, cfg)
//
// A service that already answers gRPC and HTTP mounts the same function on the
// grpc HybridServer instead, which builds the config from its own options and
// shares one listener across every registered service:
//
//	srv := grpc.NewHybridServer(opts, grpc.WithMCPServices(yourpb.ServeYourServiceMCP))
//
// # Transports
//
//   - TransportStdio: stdin/stdout (for IDE integrations, local tools)
//   - TransportStreamableHTTP: modern HTTP transport (default for production)
//   - TransportSSE: legacy SSE transport
//
// Parse from env: mcp.ParseTransports(os.Getenv("MCP_TRANSPORT"))
//
// # Error Handling
//
// Convert gRPC errors to MCP tool results in your tool handlers:
//
//	resp, err := srv.MyRPC(ctx, req)
//	if err != nil {
//	    return mcp.HandleError(err)
//	}
//
// # Elicitation
//
// Run confirmation dialogs before tool execution. Elicitation is a multi
// round-trip request (SEP-2322): the first call returns a pending result that
// asks the client for input, and the client retries the tool call with the
// answer, so the handler body runs twice.
//
//	result, pending, err := mcp.RunElicitation(req, "Are you sure?", []mcp.ElicitField{
//	    {Name: "confirm", Required: true, Type: "string", EnumValues: []string{"yes", "no"}},
//	})
//	if err != nil {
//	    return nil, err
//	}
//	if pending != nil {
//	    return pending, nil
//	}
//	if result.Action != "accept" {
//	    return mcp.TextResult("Action canceled by user."), nil
//	}
package mcp
