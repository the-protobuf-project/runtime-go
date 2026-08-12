// Package mcp is the runtime that protoc-gen-mcp's generated code calls into:
// server configuration, transport handling, and the MCP integration helpers a
// generated Serve*MCP function needs.
//
// The generated code is what usually imports this — you write proto, run
// protoc-gen-mcp, and hand the resulting Serve*MCP function a config. Reach for
// the package directly when you are building the config, mapping gRPC errors
// onto tool results, or asking the user a question mid-tool-call.
//
// The MCP SDK ([github.com/modelcontextprotocol/go-sdk/mcp]) is imported here as
// sdk, since this package took the mcp name.
//
// # Install
//
//	go get github.com/the-protobuf-project/runtime-go/mcp
//
// # Quick Start
//
// Use with generated Serve*MCP functions from protoc-gen-mcp:
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
// To serve MCP alongside gRPC and the HTTP gateway, do not build the config
// yourself — hand the generated function to
// [github.com/the-protobuf-project/runtime-go/grpc.WithMCPServices], which fills
// the config in from the server's own options and mounts every service on one
// shared listener.
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
