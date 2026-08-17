package a2a

import (
	"errors"

	"github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// Transport names a wire protocol an agent can be reached over. The values are
// the lowercase forms a config file or environment variable would carry; the
// protocol names that go on the card are derived from them.
type Transport string

const (
	// TransportJSONRPC is JSON-RPC 2.0 over HTTP, with server-sent events for
	// streaming. It is the transport every A2A client is required to speak, and
	// the default here for that reason.
	TransportJSONRPC Transport = "jsonrpc"

	// TransportGRPC serves A2A as a gRPC service. It registers on a
	// [grpc.Server] the caller already has rather than opening one, because a
	// process speaking gRPC has a server and a port already.
	TransportGRPC Transport = "grpc"

	// TransportREST is the HTTP+JSON binding: the same methods as plain REST
	// paths, for clients that would rather not speak JSON-RPC.
	TransportREST Transport = "rest"
)

// Defaults applied to a [ServerConfig] that does not name them.
const (
	// DefaultBasePath is where the JSON-RPC endpoint mounts. REST, when
	// enabled, takes the subtree beneath it.
	DefaultBasePath = "/a2a"

	// DefaultHost is the host advertised on a card when nothing better is
	// known. It is a loopback address on purpose: an agent that has not been
	// told its public URL is being developed, not deployed.
	DefaultHost = "127.0.0.1"

	// DefaultPort is the port the HTTP transports listen on.
	DefaultPort = 9000

	// DefaultMode is the content type a card declares when the caller names
	// none.
	DefaultMode = "text/plain"
)

// AgentCardPath is where a public agent card is served. It is fixed by the
// protocol — clients look here first and nowhere else.
const AgentCardPath = a2asrv.WellKnownAgentCardPath

// GRPCServiceName is the fully-qualified name [TransportGRPC] registers under.
// A host listing the services on its server needs it, and taking it from the
// generated descriptor means it cannot drift from what was actually registered.
var GRPCServiceName = a2apb.A2AService_ServiceDesc.ServiceName

// Errors returned by [StartServer] before it serves anything.
var (
	// ErrNoTransports is returned when a config resolves to no transport at
	// all, which would otherwise start a server nothing can reach.
	ErrNoTransports = errors.New("a2a: no transports configured")

	// ErrNoGRPCServer is returned when the gRPC transport is asked for without
	// a server to register on. This runtime never opens a gRPC listener of its
	// own: A2A is one service among the process's others, and giving it a
	// private port would put it somewhere clients of the rest cannot see.
	ErrNoGRPCServer = errors.New("a2a: TransportGRPC requires ServerConfig.GRPCServer")

	// ErrBasePathConflict is returned when JSON-RPC and REST are both enabled
	// under a base path of "/", where the exact route and the subtree route
	// would be the same pattern.
	ErrBasePathConflict = errors.New(`a2a: JSON-RPC and REST cannot share a base path of "/"; give JSON-RPC its own`)
)
