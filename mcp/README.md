# mcp

The Go runtime behind [grpc-mcp-gateway](https://github.com/the-protobuf-project/grpc-mcp-gateway) — server configuration, transport handling, and the MCP integration helpers that `protoc-gen-mcp`'s generated code calls into.

## Install

```bash
go get github.com/the-protobuf-project/runtime-go/mcp
```

The package is named `mcp`, matching its import path. Because the MCP SDK's package is also called `mcp`, this package imports it as `sdk` internally, and returns SDK types (`*sdk.CallToolResult`, `*sdk.Tool`, `*sdk.Server`) that your code will see under whatever name you import the SDK as.

## Overview

- **MCPServerConfig** — Configuration for MCP server name, version, transports, address, and base path
- **StartServer** — Start an MCP server with stdio, streamable-http, or SSE transports
- **Transport** — `stdio`, `streamable-http`, or `sse`
- **HandleError** — Convert gRPC errors to MCP tool error results
- **Schema helpers** — `MustParseSchema`, `MustCreateTool`, `PrepareToolWithExtras`
- **Elicitation** — `RunElicitation`, `ElicitField` for confirmation dialogs
- **Metadata** — `ForwardMetadata`, `HeadersMiddleware`, `DefaultHeaderMappings` for HTTP→gRPC header forwarding
- **App/Resource** — `DefaultPromptHandler`, `DefaultResourceHandler`, `DefaultAppResourceHandler`, `AppResourceURI`, `SetToolAppMeta`
- **Cancellation** — Generated streaming tool handlers honor [MCP cancellation](https://modelcontextprotocol.io/specification/2025-03-26/basic/utilities/cancellation): when the client sends `notifications/cancelled`, the SDK cancels the request context; the gRPC stream returns `context.Canceled`; the handler returns without sending a response

## Quick Start

Generated `Serve*MCP` functions accept your gRPC server implementation and config:

```go
package main

import (
    "context"
    "log"

    "github.com/the-protobuf-project/runtime-go/mcp"
    "github.com/your/module/yourpb"
)

func main() {
    srv := newYourServer()  // implements YourServiceMCPServer (gRPC methods)

    cfg := &mcp.MCPServerConfig{
        Name:                 "my-service",
        Version:              "1.0.0",
        Transports:           []mcp.Transport{mcp.TransportStreamableHTTP},
        Addr:                 ":8082",
        GeneratedBasePath:     yourpb.YourServiceMCPDefaultBasePath,
    }

    err := yourpb.ServeYourServiceMCP(context.Background(), srv, cfg)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Alongside gRPC and the HTTP gateway

To serve MCP next to gRPC and REST rather than on its own, register the same generated function with [`grpc`](../grpc)'s `HybridServer` and skip the config entirely — it is built from the server's `MCP` options, and every registered service mounts on one shared listener:

```go
srv := grpc.NewHybridServer(opts, grpc.WithMCPServices(yourpb.ServeYourServiceMCP))
```

## Transports

| Transport           | Value             | Use case                          |
| ------------------- | ----------------- | --------------------------------- |
| `TransportStdio`    | `stdio`           | Local tools, IDE integrations     |
| `TransportStreamableHTTP` | `streamable-http` | Production, modern MCP clients    |
| `TransportSSE`      | `sse`             | Legacy SSE clients                |

Multiple transports run concurrently. Parse from env:

```go
transports := mcp.ParseTransports(os.Getenv("MCP_TRANSPORT"))
// e.g. MCP_TRANSPORT=stdio,streamable-http
```

## Configuration

### MCPServerConfig

| Field                 | Description                                      |
| --------------------- | ------------------------------------------------ |
| `Name`                | MCP server name (reported to clients)            |
| `Version`             | Server version                                   |
| `Transport` / `Transports` | Single or multiple transports                 |
| `Addr`                | Listen address for HTTP (default `:8080`)        |
| `BasePath`            | HTTP path prefix (default `/mcp`)                |
| `GeneratedBasePath`   | Proto-derived path (takes precedence)           |
| `HeaderMappings`      | HTTP header → gRPC metadata forwarding          |
| `ReadTimeout`         | Max duration for reading request (0 = no limit)  |
| `WriteTimeout`        | Max duration for writing response (0 = no limit; keep 0 for progress) |
| `OnReady`             | Callback before server starts                    |

### Header forwarding

Forward HTTP headers to gRPC metadata:

```go
cfg := &mcp.MCPServerConfig{
    HeaderMappings: mcp.DefaultHeaderMappings(),
    // Or custom: []mcp.HeaderMapping{
    //     {HTTPHeader: "Authorization", GRPCKey: "authorization"},
    // },
}
```

## Error handling

Convert gRPC errors to MCP tool results:

```go
result, err := myToolHandler(ctx, req)
if err != nil {
    return mcp.HandleError(err)  // Returns (*sdk.CallToolResult, error)
}
```

## Elicitation

Run confirmation dialogs before tool execution. Elicitation is a multi
round-trip request ([SEP-2322]): protocol version `2026-07-28` forbids the
server from asking the client anything while it is serving a request, so the
first pass returns a pending result carrying the question and the client
retries the tool call with the answer. The handler body therefore runs twice.

```go
fields := []mcp.ElicitField{
    {Name: "confirm", Description: "Confirm action", Required: true, Type: "string", EnumValues: []string{"yes", "no"}},
}
result, pending, err := mcp.RunElicitation(req, "Are you sure?", fields)
if err != nil {
    return nil, err
}
if pending != nil {
    return pending, nil // ask the client; it will retry this call with the answer
}
if result.Action != "accept" {
    return mcp.TextResult("Action canceled by user."), nil
}
```

Clients that negotiated an older protocol version still work unchanged — the
go-sdk's server middleware performs the round trip on their behalf and
reinvokes the handler.

[SEP-2322]: https://modelcontextprotocol.io/seps/2322-multi-round-trip-requests

## Links

- **Generator**: [github.com/the-protobuf-project/grpc-mcp-gateway](https://github.com/the-protobuf-project/grpc-mcp-gateway) — the `protoc-gen-mcp` plugin that emits code against this runtime
- **Examples**: [examples/go](https://github.com/the-protobuf-project/grpc-mcp-gateway/tree/main/examples/go)
- **License**: Apache-2.0
