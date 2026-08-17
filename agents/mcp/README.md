# MCP

The runtime is linked into a service binary; the generator and the annotations
it reads live in [the-protobuf-project/mcp](https://github.com/the-protobuf-project/mcp)
and are not a dependency of the served binary.

## MCP usage

Two ways, and neither wraps the other.

### Directly

When the process serves MCP and nothing else — a local tool over stdio, a
sidecar, a binary an IDE launches — the runtime owns the listener and blocks.
Hand a generated `Serve*MCP` function your gRPC server implementation and a
config:

```go
import (
    "github.com/the-protobuf-project/runtime-go/agents/mcp"
    "github.com/your/module/yourpb"
)

srv := newYourServer() // implements YourServiceMCPServer (the gRPC methods)

cfg := &mcp.MCPServerConfig{
    Name:              "my-service",
    Version:           "1.0.0",
    Transports:        []mcp.Transport{mcp.TransportStreamableHTTP},
    Addr:              ":8082",
    GeneratedBasePath: yourpb.YourServiceMCPDefaultBasePath,
}

err := yourpb.ServeYourServiceMCP(ctx, srv, cfg)
```

### On a runtime

When the process serves more than MCP, `mcp.Service` hands the generated
function to a runtime and lets it decide where to sit:

```go
rt.Register(mcp.Service(yourpb.ServeYourServiceMCP))
```

`mcp.ServiceGroup` takes several at once — they share a listener, each at its
own proto-derived base path. Through the HybridServer the same registration is
`grpc.WithMCPServices(...)`, which re-exports `MCPServerConfig`, `MCPOption` and
`ElicitField` so a program taking that route never imports `agents/mcp`

## MCP transports

| Transport | Value | Use case |
| --- | --- | --- |
| `TransportStdio` | `stdio` | Local tools, IDE integrations |
| `TransportStreamableHTTP` | `streamable-http` | Production, modern MCP clients |
| `TransportSSE` | `sse` | Legacy SSE clients |

Several run concurrently. Parse the set from the environment:

```go
transports := mcp.ParseTransports(os.Getenv("MCP_TRANSPORT"))
// e.g. MCP_TRANSPORT=stdio,streamable-http
```

## MCP configuration

`MCPServerConfig` is the server's settings:

| Field | Description |
| --- | --- |
| `Name` | Server name, as reported to clients |
| `Version` | Server version |
| `Transport` / `Transports` | One transport, or several |
| `Addr` | Listen address for HTTP (default `:8080`) |
| `BasePath` | HTTP path prefix (default `/mcp`) |
| `GeneratedBasePath` | Proto-derived path; takes precedence |
| `HeaderMappings` | HTTP header → gRPC metadata forwarding |
| `ReadTimeout` | Cap on reading a request; 0 means no limit |
| `WriteTimeout` | Cap on writing a response; keep 0 or long-running progress is cut off |
| `OnReady` | Called with the resolved config before the server starts |

### Header forwarding

An MCP call arrives over HTTP and leaves as a gRPC call, so anything the
backend authenticates on has to cross that boundary explicitly:

```go
cfg := &mcp.MCPServerConfig{
    HeaderMappings: mcp.DefaultHeaderMappings(),
    // Or name them: []mcp.HeaderMapping{
    //     {HTTPHeader: "Authorization", GRPCKey: "authorization"},
    // },
}
```

## What the MCP runtime provides

- **Server** — `MCPServerConfig`, `StartServer`, `NewMCPServer`, `ServerEndpoint`
- **Transports** — `Transport`, `ParseTransports`, the three constants above
- **Errors** — `HandleError` turns a gRPC status into an MCP tool error result
- **Schemas** — `MustParseSchema`, `MustCreateTool`, `PrepareToolWithExtras`
- **Elicitation** — `RunElicitation`, `ElicitField`, `ElicitSchema`
- **Metadata** — `ForwardMetadata`, `HeadersMiddleware`, `DefaultHeaderMappings`
- **Prompts and resources** — `DefaultPromptHandler`, `DefaultResourceHandler`,
  `DefaultAppResourceHandler`, `AppResourceURI`, `SetToolAppMeta`, and
  `WithResourceHandler` to serve a declared resource's real content while its
  metadata keeps coming from the proto
- **Progress** — `SendProgressFromProto`, `SendDoneProgress`, `WithProgressToken`,
  and the `ProgressMessage` interface any generated `MCPProgress` satisfies
- **Streaming** — `InProcessServerStream`, which lets a generated handler call a
  gRPC streaming method in-process with no network hop

Cancellation is handled for you: on `notifications/cancelled` the SDK cancels
the request context, the gRPC stream returns `context.Canceled`, and the handler
returns without sending a response.

## MCP error handling

```go
resp, err := srv.MyRPC(ctx, req)
if err != nil {
    return mcp.HandleError(err) // (*mcp.CallToolResult, error)
}
```

## MCP elicitation

A confirmation dialog before a tool runs. It is a multi round-trip request
([SEP-2322]): protocol version `2026-07-28` forbids the server from asking the
client anything while it is serving a request, so the first pass returns a
pending result carrying the question and the client retries the tool call with
the answer. **The handler body therefore runs twice** — anything it does before
the elicitation must be safe to do again.

```go
fields := []mcp.ElicitField{
    {Name: "confirm", Description: "Confirm action", Required: true, Type: "string", EnumValues: []string{"yes", "no"}},
}
result, pending, err := mcp.RunElicitation(req, "Are you sure?", fields)
if err != nil {
    return nil, err
}
if pending != nil {
    return pending, nil // ask the client; it retries this call with the answer
}
if result.Action != "accept" {
    return mcp.TextResult("Action canceled by user."), nil
}
```

Clients that negotiated an older protocol version still work unchanged — the
go-sdk's server middleware performs the round trip on their behalf and
reinvokes the handler.

[SEP-2322]: https://modelcontextprotocol.io/seps/2322-multi-round-trip-requests

## Protocol types

The SDK types a caller needs are re-exported from `sdk.go` — `Server`,
`Resource`, `ResourceTemplate`, `Annotations`, `Icon`, `Prompt`,
`CallToolRequest`/`Result`, `ElicitParams`/`Result`, the client-side types, and
so on. Generated code and service binaries therefore import this package alone:

```go
import "github.com/the-protobuf-project/runtime-go/agents/mcp"

func register(s *mcp.Server) { ... }
```

Two reasons, beyond the cosmetic one that both packages are called `mcp` and so
one of them would need an import alias at every call site. A consumer's `go.mod`
no longer takes a direct dependency on the SDK, so generated code and this
runtime cannot drift onto different SDK versions. And this package depends on no
build of the MCP annotations at all — `SendProgressFromProto` accepts the
[`ProgressMessage`] interface rather than a generated `*MCPProgress`, because
Go's protobuf registry rejects two packages claiming the same extension numbers
and pinning one build here would make a binary that links a differently-built
`MCPProgress` panic during package init.

The re-exports are type *aliases*, so a value crosses between this package and
the SDK with no conversion and a caller who does reach for the SDK directly
still interoperates. Files that reach for the SDK directly import it unaliased.

[`ProgressMessage`]: https://pkg.go.dev/github.com/the-protobuf-project/runtime-go/agents/mcp#ProgressMessage

## Links

- **Generator and annotations**: [the-protobuf-project/mcp](https://github.com/the-protobuf-project/mcp)
- **Examples**: [examples/go](https://github.com/the-protobuf-project/mcp/tree/main/examples/go)
- **Back to** [`agents`](../README.md)
