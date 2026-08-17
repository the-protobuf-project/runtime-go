// Package a2a serves an agent over the Agent2Agent protocol: the agent card
// that describes it, the transports it answers on, and the execution loop that
// turns a request into a stream of events.
//
// It is a facade over github.com/a2aproject/a2a-go, in the same relationship
// [mcp] has with the Model Context Protocol SDK. What it adds is the part that
// is the same in every service: one config that resolves the transports, the
// listen address and the card together, so the card cannot advertise an
// endpoint the server is not serving.
//
// # Install
//
//	go get github.com/the-protobuf-project/runtime-go/agents/a2a
//
// # An agent
//
// An agent is an [Executor]. The full interface streams events over a task's
// lifetime; an agent that answers in one shot uses [TextAgent] instead:
//
//	agent := a2a.TextAgent(func(ctx context.Context, text string) (string, error) {
//	    return "you said: " + text, nil
//	})
//
//	cfg := &a2a.ServerConfig{
//	    Name:        "echo",
//	    Description: "Repeats what it is told",
//	    Version:     "1.0.0",
//	    Addr:        ":9000",
//	    Skills: []a2a.Skill{{
//	        ID: "echo", Name: "Echo", Description: "Repeats the input",
//	        Tags: []string{"demo"},
//	    }},
//	}
//	err := a2a.StartServer(ctx, cfg, agent)
//
// That serves JSON-RPC at /a2a and the public card at
// /.well-known/agent-card.json, and blocks until ctx is canceled.
//
// # Transports
//
//   - [TransportJSONRPC] — JSON-RPC 2.0 over HTTP, with SSE for streaming. Every
//     client speaks it, so it is the default.
//   - [TransportGRPC] — A2A as a gRPC service, registered on a [grpc.Server] the
//     caller already has.
//   - [TransportREST] — the HTTP+JSON binding, served beneath the base path.
//
// Several run at once, and each one the agent serves is declared on the card.
// Parse the set from the environment with [ParseTransports].
//
// # Serving alongside gRPC
//
// A service that already answers gRPC and HTTP does not want a second server.
// [ServerConfig.Mux] and [ServerConfig.GRPCServer] are the seams for that: with
// either set this package mounts handlers and never opens a listener, and
// [StartServer] blocks until the host's context ends. That is how the grpc
// HybridServer's WithA2AServices mounts an agent on the port it already has.
//
// # Errors
//
// [HandleError] turns an error from the agent's own work into the terminal
// event that reports it, preserving a gRPC status code as both the task state
// and the message text — so a caller learns whether it was refused, canceled,
// or genuinely broken.
//
// # Naming
//
// The upstream core package is also called a2a. Inside this package that name
// refers to the SDK, and this package's own identifiers are written unqualified.
//
// [mcp]: https://pkg.go.dev/github.com/the-protobuf-project/runtime-go/agents/mcp
package a2a
