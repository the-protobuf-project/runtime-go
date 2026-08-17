package a2a

import (
	"net"
	"net/url"
	"strconv"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// The card vocabulary, re-exported so a program describing an agent imports
// this package and nothing else. They are aliases rather than wrappers: the
// card is a wire format the protocol defines, and a copy of it here would be
// one more thing to keep in step for no gain.
type (
	// Card is an agent's self-describing manifest — identity, capabilities,
	// skills, and where to reach it. Clients fetch it before anything else.
	Card = a2a.AgentCard

	// Skill is one distinct thing an agent can do. A card with no skills is
	// valid and useless: skills are how a client decides to call you at all.
	Skill = a2a.AgentSkill

	// Capabilities declares the optional protocol features an agent supports.
	Capabilities = a2a.AgentCapabilities

	// Provider identifies the organization behind an agent.
	Provider = a2a.AgentProvider

	// Interface is a transport-and-URL pair. An agent reachable over several
	// transports lists one of these per transport.
	Interface = a2a.AgentInterface

	// TransportProtocol is the protocol name as it appears on a card. It is
	// deliberately not an enum — the spec lets agents declare their own.
	TransportProtocol = a2a.TransportProtocol
)

// ProtocolVersion is the A2A protocol version this runtime implements.
const ProtocolVersion = string(a2a.Version)

// BuildCard assembles the public agent card from cfg.
//
// A caller who already has a [Card] — from a config file, a registry, or a
// signed manifest — sets [ServerConfig.Card] and gets it back untouched.
// Everything else here exists so the common case does not have to: the
// identity, the skills, and the addresses the agent actually listens on are all
// things the server config knows, and restating them in a card by hand is how
// the card and the server drift apart.
//
// The advertised URLs come from [ServerConfig.PublicURL] when it is set,
// because a server behind a proxy or inside a container cannot infer the
// address a client will use. Without one they fall back to the listen address,
// which is right for local development and wrong the moment there is a hop.
func BuildCard(cfg *ServerConfig) *Card {
	if cfg.Card != nil {
		return cfg.Card
	}

	card := &Card{
		Name:               cfg.Name,
		Description:        cfg.Description,
		Version:            cfg.Version,
		Provider:           cfg.Provider,
		Skills:             cfg.Skills,
		Capabilities:       cfg.Capabilities,
		DefaultInputModes:  cfg.DefaultInputModes,
		DefaultOutputModes: cfg.DefaultOutputModes,
		DocumentationURL:   cfg.DocumentationURL,
		IconURL:            cfg.IconURL,
	}
	if card.Skills == nil {
		// The field is required on the wire, and a null there is a client-side
		// failure in a place that is hard to trace back to here.
		card.Skills = []Skill{}
	}
	if len(card.DefaultInputModes) == 0 {
		card.DefaultInputModes = []string{DefaultMode}
	}
	if len(card.DefaultOutputModes) == 0 {
		card.DefaultOutputModes = []string{DefaultMode}
	}

	// Every transport the agent serves is declared, in configured order. A
	// client picks from this list, so a transport that is served but unlisted
	// is one nobody will use, and one listed but unserved is a failed dial.
	for _, t := range cfg.resolvedTransports() {
		card.SupportedInterfaces = append(card.SupportedInterfaces,
			a2a.NewAgentInterface(cfg.publicURLFor(t), t.protocol()))
	}

	return card
}

// publicURLFor is where a client should reach this agent over t.
func (c *ServerConfig) publicURLFor(t Transport) string {
	base := c.PublicURL
	if base == "" {
		base = "http://" + hostPortOrDefault(c.resolvedAddr())
	}

	if t == TransportGRPC {
		// A gRPC target is host:port, not a URL. A scheme here is what turns a
		// client's dial into a confusing name-resolution error.
		if u, err := url.Parse(base); err == nil && u.Host != "" {
			return u.Host
		}
		return base
	}

	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return base + c.resolvedBasePath()
	}
	// REST routes on paths of its own beneath the base path; JSON-RPC answers
	// at the base path itself. Both are reached through the same prefix, which
	// is what a client needs to be told.
	u.Path = c.resolvedBasePath()
	return u.String()
}

// hostPortOrDefault fills in a host for an address that names only a port, so
// ":9000" advertises somewhere a client can actually connect.
func hostPortOrDefault(addr string) string {
	if addr == "" {
		return net.JoinHostPort(DefaultHost, strconv.Itoa(DefaultPort))
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	// "0.0.0.0" is a bind instruction, not a destination: it means every
	// interface here, and nothing at all to a client that dials it.
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = DefaultHost
	}
	return net.JoinHostPort(host, port)
}
