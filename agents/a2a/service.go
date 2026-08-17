package a2a

import (
	"context"
	"slices"

	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/shared"
)

// ServiceFunc is a blocking function that serves one agent against a config the
// caller builds — the shape a generated ServeFooA2A would take, and the seam a
// host uses to adjust a config the runtime assembled.
//
// It is handed a config with [ServerConfig.OnReady] already set and must leave
// it in place, or the runtime placing this service never learns it mounted.
type ServiceFunc func(ctx context.Context, cfg *ServerConfig) error

// service is an [agents.Service] over one agent, or over a function serving
// one.
type service struct {
	fn         ServiceFunc
	agent      Executor
	skills     []Skill
	transports []Transport
	addr       string
	basePath   string
	card       *Card
	caps       Capabilities
	headers    []shared.HeaderMapping
	serveCard  *bool
}

// ServiceOption adjusts what [Service] registers.
type ServiceOption func(*service)

// ServiceTransports chooses the bindings the agent answers on. Without it the
// agent speaks [TransportJSONRPC], which every A2A client is required to
// support.
func ServiceTransports(transports ...Transport) ServiceOption {
	return func(s *service) { s.transports = transports }
}

// ServiceAddr gives the agent a listen address of its own rather than the
// runtime's shared one. Without it the agent shares, under its base path.
func ServiceAddr(addr string) ServiceOption {
	return func(s *service) { s.addr = addr }
}

// ServiceBasePath overrides where JSON-RPC mounts and REST is served beneath.
// Defaults to [DefaultBasePath].
func ServiceBasePath(path string) ServiceOption {
	return func(s *service) { s.basePath = path }
}

// ServiceCapabilities declares the optional protocol features the agent
// supports — streaming, push notifications, state transition history.
func ServiceCapabilities(caps Capabilities) ServiceOption {
	return func(s *service) { s.caps = caps }
}

// ServiceCard serves a card the caller built instead of one assembled from the
// runtime's identity and the registered skills. For a card that is signed or
// came from a registry that is the only correct behavior — rebuilding it would
// invalidate the signature.
func ServiceCard(card *Card) ServiceOption {
	return func(s *service) { s.card = card }
}

// ServiceHeaders forwards HTTP headers into gRPC metadata for the executor.
// See [shared.DefaultHeaderMappings].
func ServiceHeaders(mappings ...shared.HeaderMapping) ServiceOption {
	return func(s *service) { s.headers = mappings }
}

// ServeAgentCard chooses whether this agent asks for the public card path.
//
// It is on by default, and asking is not the same as getting: the well-known
// path admits one card per host, so on a shared listener the first agent to
// mount takes it. Turn it off for an agent that should never claim it, whatever
// the mounting order turns out to be.
func ServeAgentCard(serve bool) ServiceOption {
	return func(s *service) { s.serveCard = &serve }
}

// Service registers an agent on an [agents.Runtime].
//
// The agent's identity comes from the runtime rather than being restated here,
// so its card and the process's other protocols cannot disagree about what this
// service is. What the card cannot get from the runtime is what the agent can
// actually do, which is what skills are:
//
//	rt := agents.New(agents.Config{Name: "my-service", Version: "1.0.0", Port: 9000})
//	rt.Register(a2a.Service(agent, a2a.Skill{ID: "echo", Name: "Echo"}))
//	err := rt.Serve(ctx)
func Service(agent Executor, skills ...Skill) agents.Service {
	return ServiceWith(agent, skills, nil)
}

// ServiceWith is [Service] with options.
func ServiceWith(agent Executor, skills []Skill, opts []ServiceOption) agents.Service {
	return newService(&service{agent: agent, skills: skills}, opts)
}

// ServiceFrom registers a [ServiceFunc] rather than an agent, for a generated
// entrypoint or a host that wants the assembled config in hand before serving.
func ServiceFrom(fn ServiceFunc, opts ...ServiceOption) agents.Service {
	return newService(&service{fn: fn}, opts)
}

func newService(s *service, opts []ServiceOption) agents.Service {
	s.transports = []Transport{TransportJSONRPC}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Protocol implements [agents.Service].
func (s *service) Protocol() agents.Protocol { return agents.A2A }

// Requires implements [agents.Service]. Which of the two it asks for depends on
// the transports: the gRPC binding registers on the runtime's server and needs
// no listener of its own, and an agent serving only that one should not cause a
// port to be bound.
func (s *service) Requires() agents.Requirements {
	return agents.Requirements{
		Addr: s.addr,
		HTTP: slices.ContainsFunc(s.transports, func(t Transport) bool { return t != TransportGRPC }),
		GRPC: slices.Contains(s.transports, TransportGRPC),
	}
}

// Serve implements [agents.Service]. It hands [StartServer] a config built from
// the placement and lets it mount, which is the same path a standalone agent
// takes — the runtime is only deciding where.
func (s *service) Serve(ctx context.Context, p agents.Placement, ready func([]agents.Endpoint)) error {
	cfg := &ServerConfig{
		Name:           p.Name,
		Description:    p.Description,
		Version:        p.Version,
		Transports:     s.transports,
		Addr:           p.Addr,
		BasePath:       s.basePath,
		PublicURL:      p.PublicURL,
		Skills:         s.skills,
		Capabilities:   s.caps,
		Card:           s.card,
		HeaderMappings: s.headers,
		Mux:            p.Mux,
		GRPCServer:     p.GRPCServer,
		ServeAgentCard: s.serveCard == nil || *s.serveCard,
	}
	cfg.OnReady = func(resolved *ServerConfig) {

		eps := ServerEndpoints(resolved)
		out := make([]agents.Endpoint, 0, len(eps))
		for _, ep := range eps {
			out = append(out, agents.Endpoint{
				Protocol:  agents.A2A,
				Transport: string(ep.Transport),
				URL:       ep.URL,
				Detail:    ep.CardURL,
			})
		}
		ready(out)
	}

	if s.fn != nil {
		return s.fn(ctx, cfg)
	}
	return StartServer(ctx, cfg, s.agent)
}
