# Agents

The protocols a model-driven client speaks to a service. Build one runtime,
register the protocols it should answer, start it.

| Protocol | Package | Reached by | Status |
| --- | --- | --- | --- |
| Model Context Protocol | [`agents/mcp`](./mcp) | a model, calling your tools | implemented |
| Agent2Agent | [`agents/a2a`](./a2a) | another agent, delegating a task | implemented |

[`agents/shared`](./shared) holds what both would otherwise copy — currently
the HTTP-header-to-gRPC-metadata forwarding each needs at the same boundary.

## Usage

```go
import (
    "github.com/the-protobuf-project/runtime-go/agents"
    "github.com/the-protobuf-project/runtime-go/agents/a2a"
    "github.com/the-protobuf-project/runtime-go/agents/mcp"
)

rt := agents.New(agents.Config{
    Name:        "my-service",
    Description: "does two things at once",
    Version:     "1.0.0",
    Port:        9000,
})

rt.Register(
    mcp.Service(yourpb.ServeYourServiceMCP),
    a2a.Service(agent, a2a.Skill{ID: "echo", Name: "Echo"}),
)

err := rt.Serve(ctx)
```

Two protocols, one port, each under its own base path, one shutdown that drains
both. `rt.Endpoints()` reports where each came up.

`Serve` blocks until the context ends and then drains — the shape for a process
that does nothing else. A host with its own lifecycle uses `Start`, which
returns as soon as everything is answering, and `Shutdown` when it is done.

### Why one object

The protocols are unrelated. A process serving both still has to make the same
four decisions either way — what it calls itself, where it listens, what owns
the listener, what drains it — and made twice, they drift. The identity on an
A2A card and the identity an MCP client is told are the same string here
because they came from the same `Config`, not because somebody kept them in
step.

The runtime is where those four decisions live and nothing else. The protocol
packages are not layered underneath it: `Register` takes an `agents.Service`,
and each package builds one out of its own vocabulary.

### Placement

Services sharing an address share a listener and a mux. A service that names
its own address gets its own:

```go
rt.Register(
    mcp.Service(yourpb.ServeYourServiceMCP, mcp.ServiceAddr(":8082")),
    a2a.Service(agent), // still on the runtime's port
)
```

Not every service listens. An A2A agent serving only its gRPC binding registers
on `Config.GRPCServer` and mounts nothing, and no listener is opened for it:

```go
rt := agents.New(agents.Config{
    Name: "my-service", Version: "1.0.0",
    GRPCServer: myGRPCServer, // must not be serving yet
})
rt.Register(a2a.Service(agent, skills...), a2a.ServiceTransports(a2a.TransportGRPC))
```

### Inside the grpc HybridServer

Both protocols mount on a mux rather than opening a listener, which is what
lets [`grpc`](../grpc) serve them off ports it already has. Registration there
is unchanged:

```go
srv := grpc.NewHybridServer(opts,
    grpc.WithMCPServices(yourpb.ServeYourServiceMCP),
    grpc.WithA2AAgent(agent, grpc.A2ASkill{ID: "echo", Name: "Echo"}),
)
```

The HybridServer builds one `agents.Runtime` from its own options and keeps MCP
and A2A on the separate ports it has always used, by giving each service an
address of its own. Ports default to consecutive: A2A takes MCP's plus one, and
MCP takes HTTP's plus one.

## Install

```bash
go get github.com/the-protobuf-project/runtime-go/agents
```

The protocol packages live in the same module, so one `go get` brings them all;
name the ones you import.

## Per-protocol docs

- [`agents/mcp`](./mcp/README.md) — serving tools to a model
- [`agents/a2a`](./a2a/README.md) — serving an agent to other agents

## License

Apache-2.0
