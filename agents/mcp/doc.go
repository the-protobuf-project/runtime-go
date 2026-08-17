// Package mcp provides the Go runtime for the code protoc-gen-mcp
// generates: server configuration, transport handling, and MCP integration
// helpers.
//
// It lives here rather than alongside the generator so that the-protobuf-project/mcp
// ships only the plugin and its annotations, and a service binary depends on
// the runtime alone.
//
// The one proto type it reads comes from the Buf Schema Registry, at
// buf.build/gen/go/the-protobuf-project/mcp/protocolbuffers/go, which is why
// this module needs no checkout of the generator's repository to build. That
// package is named protobuf, which says nothing at a call site, so it is
// imported here as mcppb.
//
// This package and the upstream SDK it is built on are both named mcp, so the
// SDK is imported as mcpsdk throughout. Inside this package a bare mcp. never
// appears and mcpsdk.Tool is unambiguously the SDK's; callers see the reverse,
// importing this package as mcp and rarely reaching for the SDK at all.
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
