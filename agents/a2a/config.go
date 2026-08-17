package a2a

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/the-protobuf-project/runtime-go/agents/shared"
	"google.golang.org/grpc"
)

// ServerConfig is everything needed to serve one agent. The zero value is not
// usable — an agent with no name and no skills is not describable — but every
// field below Name has a working default.
type ServerConfig struct {
	// Name and Description are the agent's identity as clients see it on the
	// card. Version is the agent's own, not the protocol's.
	Name        string
	Description string
	Version     string

	// Transport is the single-transport form, for the common case. Transports
	// is the general one. When both are set Transports wins; when neither is,
	// the agent speaks [TransportJSONRPC].
	Transport  Transport
	Transports []Transport

	// Addr is the listen address for the HTTP transports, as host:port.
	// Ignored when the agent speaks only gRPC, and ignored when Mux is set —
	// the host owns the listener then.
	Addr string

	// BasePath is where JSON-RPC mounts, and the prefix REST is served under.
	// Defaults to [DefaultBasePath].
	BasePath string

	// GeneratedBasePath is the path a code generator derived from the agent's
	// proto. It takes precedence over BasePath, on the same reasoning as the
	// MCP runtime: a generated path is part of a contract clients already hold,
	// and a hand-set one is a preference.
	GeneratedBasePath string

	// PublicURL is the base URL clients should use, for an agent behind a proxy
	// or inside a container. Without it the card advertises the listen address,
	// which is right locally and wrong behind any hop.
	PublicURL string

	// Skills are the distinct things this agent can do. A card carries them
	// verbatim, and a client with no skills to read has no reason to call.
	Skills []Skill

	// Capabilities declares the optional protocol features supported —
	// streaming, push notifications, state transition history.
	Capabilities Capabilities

	// DefaultInputModes and DefaultOutputModes are the content types skills
	// accept and produce unless a skill overrides them. Both default to
	// [DefaultMode].
	DefaultInputModes  []string
	DefaultOutputModes []string

	// Provider, DocumentationURL and IconURL are optional card decoration.
	Provider         *Provider
	DocumentationURL string
	IconURL          string

	// Card, when set, is served as-is and every card-shaped field above is
	// ignored. For an agent whose card is signed, generated, or read from a
	// registry, that is the only correct behavior — rebuilding it here would
	// invalidate the signature.
	Card *Card

	// HeaderMappings forwards HTTP headers into gRPC metadata for the executor
	// to use. See [shared.HeaderMapping].
	HeaderMappings []shared.HeaderMapping

	// ReadTimeout and WriteTimeout bound the owned HTTP server. Zero means no
	// limit, which is what streaming needs: an agent that works for a minute
	// before its first artifact would otherwise have the response cut off.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Mux, when non-nil, mounts the HTTP transports on this shared mux instead
	// of opening a listener. [StartServer] then blocks until ctx ends and the
	// host owns the server's lifecycle — the seam a process serving several
	// protocols from one port uses.
	Mux *http.ServeMux

	// GRPCServer is where [TransportGRPC] registers. Required for that
	// transport and unused by the others.
	GRPCServer *grpc.Server

	// HandlerOptions are passed through to the SDK's request handler, for
	// everything this config does not name: task stores, push notification
	// senders, call interceptors, concurrency limits.
	HandlerOptions []a2asrv.RequestHandlerOption

	// TransportOptions are passed through to the HTTP transport handlers —
	// keep-alive interval and panic handling.
	TransportOptions []a2asrv.TransportOption

	// ServeAgentCard mounts the public card at [AgentCardPath]. It defaults to
	// true when this server owns its mux.
	//
	// The well-known path is singular per host, so on a shared mux the first
	// agent to mount takes it and any later one silently does not — asking for
	// it is a preference, not a claim. Set [ServerConfig.PublicURL] or read
	// [Endpoint.CardURL] to find out where a given agent's card actually is.
	ServeAgentCard bool

	// OnReady is called with the resolved config once every transport is
	// mounted and before the listener opens, so a host can log or record where
	// the agent ended up.
	OnReady func(resolved *ServerConfig)
}

// resolvedTransports is the transport set this config asks for, in the order
// they should appear on the card — the first is the preferred one.
func (c *ServerConfig) resolvedTransports() []Transport {
	if len(c.Transports) > 0 {
		return c.Transports
	}
	if c.Transport != "" {
		return []Transport{c.Transport}
	}
	return []Transport{TransportJSONRPC}
}

// resolvedBasePath is where JSON-RPC mounts, generated path first.
func (c *ServerConfig) resolvedBasePath() string {
	return ResolveBasePath(c, "")
}

// resolvedAddr is the listen address, with the defaults filled in.
func (c *ServerConfig) resolvedAddr() string {
	if c.Addr != "" {
		return c.Addr
	}
	return fmt.Sprintf("%s:%d", DefaultHost, DefaultPort)
}

// has reports whether t is among the configured transports.
func (c *ServerConfig) has(t Transport) bool {
	return slices.Contains(c.resolvedTransports(), t)
}

// protocol is the name this transport goes by on a card.
func (t Transport) protocol() TransportProtocol {
	switch t {
	case TransportGRPC:
		return a2a.TransportProtocolGRPC
	case TransportREST:
		return a2a.TransportProtocolHTTPJSON
	default:
		return a2a.TransportProtocolJSONRPC
	}
}

// ParseTransports splits a comma-separated transport list, as an environment
// variable would carry it. Unknown and empty entries are dropped rather than
// rejected, so one bad value in A2A_TRANSPORT does not take the process down;
// a list that resolves to nothing falls back to the default at [StartServer].
//
//	transports := ParseTransports(os.Getenv("A2A_TRANSPORT"))
//	// e.g. A2A_TRANSPORT=jsonrpc,grpc
func ParseTransports(s string) []Transport {
	parts := strings.Split(s, ",")
	out := make([]Transport, 0, len(parts))
	for _, p := range parts {
		switch t := Transport(strings.ToLower(strings.TrimSpace(p))); t {
		case TransportJSONRPC, TransportGRPC, TransportREST:
			out = append(out, t)
		}
	}
	return out
}
