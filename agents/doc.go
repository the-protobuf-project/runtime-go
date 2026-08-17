// Package agents serves the protocols a model-driven client speaks to a
// service: build one [Runtime], register the protocols it should answer, start
// it.
//
//	rt := agents.New(agents.Config{
//	    Name:    "my-service",
//	    Version: "1.0.0",
//	    Port:    9000,
//	})
//
//	rt.Register(
//	    mcp.Service(yourpb.ServeYourServiceMCP),
//	    a2a.Service(agent, a2a.Skill{ID: "echo", Name: "Echo"}),
//	)
//
//	err := rt.Serve(ctx)
//
// That is two protocols on one port, each under its own base path, with one
// shutdown that drains both.
//
// # Why one object
//
// The protocols are unrelated — one is a model reaching your tools, the other
// is an agent delegating a task — but a process serving both has to make the
// same four decisions either way: what it calls itself, where it listens, what
// owns the listener, and what drains it on the way out. Made twice, they drift.
// The identity on an A2A card and the identity an MCP client is told are the
// same string here because they came from the same [Config], not because
// someone kept them in step.
//
// A runtime is where those four decisions live, and nothing else. The protocol
// packages are not layered under it: [Runtime.Register] takes a [Service], and
// each package builds one from its own vocabulary.
//
//   - github.com/the-protobuf-project/runtime-go/agents/mcp — the Model Context
//     Protocol runtime, for the code protoc-gen-mcp generates. mcp.Service takes
//     a generated ServeFooMCP.
//   - github.com/the-protobuf-project/runtime-go/agents/a2a — the Agent2Agent
//     runtime. a2a.Service takes an executor and the skills it advertises.
//   - github.com/the-protobuf-project/runtime-go/agents/shared — the part both
//     of them would otherwise copy.
//
// # Placement
//
// A service says what it needs with [Requirements] and is handed a [Placement].
// It does not choose where it sits, which is what lets the runtime put two
// protocols behind one listener without either knowing about the other.
//
// The default is to share: services naming no address of their own answer on
// [Config.Host] and [Config.Port] together. A service that names one — through
// mcp.ServiceAddr or a2a.ServiceAddr — gets a listener and mux of its own, and
// services naming the same address share with each other. That is how the grpc
// HybridServer keeps MCP and A2A on separate ports while still driving a single
// runtime.
//
// Not every service listens. A2A serving only its gRPC binding registers on
// [Config.GRPCServer] and mounts nothing, and the runtime opens no listener it
// would have nothing to put behind.
//
// # Starting
//
// [Runtime.Start] mounts every service, then opens listeners, then returns —
// non-blocking, for a host with its own lifecycle. [Runtime.Serve] is Start, a
// wait on the context, and a drain, for a process that does nothing else.
//
// The order inside Start is not an implementation detail. Handlers are
// registered before any port accepts, so nothing answers 404 to whoever got
// there first; and a gRPC binding must be registered before its server starts
// serving, because gRPC refuses one afterwards.
package agents
