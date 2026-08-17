package agents

import (
	"context"
	"errors"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

// Protocol names what a [Service] speaks.
type Protocol string

const (
	// MCP is the Model Context Protocol — a model reaching your tools.
	MCP Protocol = "mcp"

	// A2A is Agent2Agent — another agent delegating a task to yours.
	A2A Protocol = "a2a"
)

// Errors a [Runtime] returns before it serves anything.
var (
	// ErrNoServices is returned by [Runtime.Start] when nothing was registered.
	// A runtime with no services would bind a port and answer nothing, which is
	// worth failing on rather than discovering from the other end.
	ErrNoServices = errors.New("agents: no services registered")

	// ErrAlreadyStarted is returned by a second [Runtime.Start]. Starting twice
	// would mount every service again onto muxes that already have them, which
	// http.ServeMux answers with a panic rather than a duplicate route.
	ErrAlreadyStarted = errors.New("agents: runtime already started")
)

// Config is a runtime's settings. Only the identity fields have no working
// default, because an agent or tool server that will not say what it is cannot
// be described to a client.
type Config struct {
	// Name, Description and Version are the identity every registered protocol
	// advertises. Declaring them once here is the point: an MCP server and an
	// A2A card in the same process describing themselves differently is a bug
	// nobody notices until a client is confused by it.
	Name        string
	Description string
	Version     string

	// Host and Port are where the shared listener binds. Services that name no
	// address of their own answer here, each under its own base path.
	Host string
	Port int

	// PublicURL is the base URL clients should be told to use, for a process
	// behind a proxy or inside a container. Protocols that advertise their own
	// address — an A2A card, an MCP endpoint report — use it in place of the
	// listen address, which is right locally and wrong past any hop.
	PublicURL string

	// Mux, when set, mounts every sharing service on it instead of opening a
	// listener. The host owns the server then, and [Runtime.Start] returns
	// without binding anything.
	Mux *http.ServeMux

	// GRPCServer is where protocols with a gRPC binding register. A2A's gRPC
	// transport needs it; MCP does not use it.
	//
	// It must not be serving yet: gRPC refuses a registration once Serve has
	// been called, so a runtime sharing a server has to start before it does.
	GRPCServer *grpc.Server

	// ReadTimeout and WriteTimeout bound the listeners this runtime owns. Zero
	// means no limit, which is what streaming protocols need — an agent working
	// for a minute before its first artifact would otherwise be cut off.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// ReadyTimeout is how long [Runtime.Start] waits for one service to report
	// itself mounted. Zero means [DefaultReadyTimeout].
	ReadyTimeout time.Duration
}

// DefaultHost is where the shared listener binds when a config names no host.
const DefaultHost = "0.0.0.0"

// DefaultReadyTimeout bounds the wait for a service to mount itself. It is
// generous because mounting is not supposed to take measurable time — a service
// that hits this is stuck, not slow.
const DefaultReadyTimeout = 5 * time.Second

// shutdownTimeout bounds the drain of a listener this runtime owns.
const shutdownTimeout = 5 * time.Second

// Identity is what a service calls itself, passed to every protocol so they all
// say the same thing.
type Identity struct {
	Name        string
	Description string
	Version     string
}

// Requirements is what a [Service] needs from the runtime placing it. The zero
// value asks for nothing, which is what a service that neither listens nor
// registers would need — and there is no such service, so every implementation
// sets at least one field.
type Requirements struct {
	// Addr is a listen address this service must have to itself, as host:port.
	// Empty means it shares the runtime's, which is the usual answer: two
	// protocols on one port under different base paths is the whole reason a
	// runtime groups them.
	//
	// Services naming the same address share one listener and one mux with each
	// other, so a caller separating protocols by port gets exactly the ports it
	// asked for and no more.
	Addr string

	// HTTP reports whether this service mounts HTTP handlers.
	//
	// It is asked rather than assumed because not every protocol listens. A2A
	// serving only its gRPC binding mounts nothing, and a listener opened for it
	// would bind a port with nothing behind it.
	HTTP bool

	// GRPC reports whether this service registers on [Config.GRPCServer].
	// A runtime with no gRPC server refuses a service that needs one, rather
	// than starting it into a nil pointer.
	GRPC bool
}

// Placement is where the runtime decided a service sits. A service is handed
// one and mounts itself into it; it does not choose.
type Placement struct {
	// Identity is the runtime's, unchanged. Every protocol reports the same.
	Identity

	// Addr is the address the listener for this service's group binds, as
	// host:port. It is what a protocol should advertise when it has no
	// [Placement.PublicURL].
	Addr string

	// Mux is where HTTP handlers go. It is shared with every sibling at the
	// same address, so a service must mount under a path of its own.
	Mux *http.ServeMux

	// GRPCServer is where a gRPC binding registers, and is nil when the runtime
	// has none.
	GRPCServer *grpc.Server

	// PublicURL is the runtime's, for protocols that advertise an address.
	PublicURL string
}

// Endpoint is one place a protocol answers, as it should be reported to a human
// reading a startup summary or to a client reading a manifest.
type Endpoint struct {
	// Protocol is what answers here.
	Protocol Protocol

	// Transport names the binding within that protocol — "streamable-http",
	// "stdio", "jsonrpc", "grpc".
	Transport string

	// URL is where a client connects. For a gRPC binding it is a host:port dial
	// target rather than a URL, which is what a gRPC client expects.
	URL string

	// Detail is anything else worth printing: the agent card's address, the
	// base path a tool server mounted under. Free text, for people.
	Detail string
}

// Service is one protocol registered on a [Runtime].
//
// It is an interface rather than a struct because the two protocols share
// nothing but their shape: MCP serves generated functions against a config it
// builds, A2A serves an executor over transports it resolves. What they have in
// common is exactly this — they can say what they need, and they can mount
// themselves into what they are given.
type Service interface {
	// Protocol names what this speaks.
	Protocol() Protocol

	// Requires reports what this service needs from the runtime.
	Requires() Requirements

	// Serve mounts the protocol into p and blocks until ctx is done.
	//
	// It must call ready exactly once, with the endpoints it answers on, before
	// it blocks — the runtime opens listeners only after every service has
	// mounted, so a service that never reports ready holds up the start and one
	// that reports late is not listened to.
	//
	// Returning an error before ready fails the start. Returning one after is
	// logged by the runtime and ends that service alone.
	Serve(ctx context.Context, p Placement, ready func([]Endpoint)) error
}
