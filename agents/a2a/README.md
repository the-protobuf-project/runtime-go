# A2A

Agent2Agent is the other direction: not a model reaching your tools, but
another agent delegating work to yours. It is a facade over
[a2aproject/a2a-go](https://github.com/a2aproject/a2a-go) v2, adding the part
that is the same in every service — one config that resolves the transports,
the listen address and the agent card together, so the card cannot advertise an
endpoint the server is not serving.

## An agent

An agent is an `Executor`: a request in, a stream of events out. The full
interface streams over a task's lifetime; an agent that answers in one shot
uses `TextAgent`:

```go
import "github.com/the-protobuf-project/runtime-go/agents/a2a"

agent := a2a.TextAgent(func(ctx context.Context, text string) (string, error) {
    return "you said: " + text, nil
})

cfg := &a2a.ServerConfig{
    Name:        "echo",
    Description: "Repeats what it is told",
    Version:     "1.0.0",
    Addr:        ":9000",
    Skills: []a2a.Skill{{
        ID: "echo", Name: "Echo", Description: "Repeats the input",
        Tags: []string{"demo"},
    }},
}

err := a2a.StartServer(ctx, cfg, agent)
```

That serves JSON-RPC at `/a2a` and the public card at
`/.well-known/agent-card.json`, and blocks until `ctx` is canceled.

On a runtime it is shorter, because the identity comes from the `Config` rather
than being restated — skills are the part a runtime cannot know:

```go
rt.Register(a2a.Service(agent, a2a.Skill{
    ID: "echo", Name: "Echo", Description: "Repeats the input",
    Tags: []string{"demo"},
}))
```

`a2a.ServiceWith` takes options — transports, base path, a prebuilt card — and
`a2a.ServiceFrom` registers a `ServiceFunc` instead of an executor, for a
generated entrypoint or a host that wants the assembled config in hand.

For anything that streams — work with progress, artifacts, a task that outlives
one call — implement `Executor` directly and yield events:

```go
agent := a2a.ExecutorFunc(func(ctx context.Context, ec *a2a.ExecutorContext) a2a.Events {
    return func(yield func(a2a.Event, error) bool) {
        if !yield(a2a.StatusEvent(ec, a2a.StateWorking, nil), nil) {
            return
        }
        for _, chunk := range work(a2a.RequestText(ec)) {
            if !yield(a2a.ArtifactEvent(ec, a2a.TextPart(chunk)), nil) {
                return
            }
        }
        yield(a2a.StatusEvent(ec, a2a.StateCompleted, nil), nil)
    }
})
```

## A2A transports

| Transport | Value | Where it lands |
| --- | --- | --- |
| `TransportJSONRPC` | `jsonrpc` | HTTP listener, at the base path. Every client speaks it, so it is the default |
| `TransportGRPC` | `grpc` | The caller's own `grpc.Server` — never a listener of its own |
| `TransportREST` | `rest` | HTTP listener, the HTTP+JSON binding beneath the base path |

Several run at once, and every one the agent serves is declared on its card.
`a2a.ParseTransports(os.Getenv("A2A_TRANSPORT"))` reads the set from the
environment; entries naming nothing known are dropped rather than fatal.

## The agent card

`BuildCard` assembles the card from the same config that starts the server, so
the two cannot drift. Set `PublicURL` when there is a proxy or a container in
front — without it the card advertises the listen address, which is right
locally and wrong past any hop. A caller who already has a card (signed, or
from a registry) sets `ServerConfig.Card` and it is served untouched.

## A2A errors

`HandleError` turns an error from the agent's own work into the terminal event
that reports it, and keeps a gRPC status code, because the code is the part a
caller can act on:

| Code | Task state |
| --- | --- |
| `Canceled` | `canceled` — the client stopped it; it did not fail |
| `Unauthenticated`, `PermissionDenied` | `rejected` — the server declined the work |
| anything else | `failed` |

```go
resp, err := s.backend.DoWork(ctx, req)
if err != nil {
    yield(a2a.HandleError(execCtx, err), nil)
    return
}
```

## A2A on the gRPC port

The gRPC binding does not take a listener. It registers on whatever
`grpc.Server` it is given, so an agent becomes a service among the others on
that port, running through the same interceptor chain:

```go
rt := agents.New(agents.Config{
    Name: "my-service", Version: "1.0.0",
    GRPCServer: myGRPCServer,
})
rt.Register(a2a.ServiceWith(agent, skills, []a2a.ServiceOption{
    a2a.ServiceTransports(a2a.TransportGRPC),
}))
```

The server must not be serving yet — gRPC refuses a registration once it has
started. That is why the HybridServer starts its agent runtime during gRPC
setup rather than after it.

## Naming

This package and the upstream core package are both called `a2a`. Inside here
the name refers to the SDK, and this package's own identifiers are written
unqualified. Callers see the simple case: they import `a2a` and rarely reach
for the SDK at all.


## Links

- **A2A protocol**: [a2a-protocol.org](https://a2a-protocol.org)
- **Go SDK**: [a2aproject/a2a-go](https://github.com/a2aproject/a2a-go)
- **Back to** [`agents`](../README.md)
