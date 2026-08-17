package a2a

import (
	"net/url"
	"path"
	"strings"
)

// Endpoint is one resolved place an agent can be reached, as it will appear on
// the card.
type Endpoint struct {
	// Transport is the configured transport this endpoint serves.
	Transport Transport

	// Protocol is the name that transport goes by on the wire.
	Protocol TransportProtocol

	// URL is where a client connects. For gRPC it is a host:port dial target
	// rather than a URL, which is what a gRPC client expects.
	URL string

	// CardURL is where the public agent card is served, empty when this server
	// does not serve one.
	CardURL string
}

// ServerEndpoint returns the endpoint a client should prefer — the first
// configured transport. Use it to log where an agent came up before starting:
//
//	if ep, err := ServerEndpoint(cfg); err == nil {
//	    log.Printf("A2A listening on %s (%s)", ep.URL, ep.Protocol)
//	}
//
// It returns [ErrNoTransports] when the config names none.
func ServerEndpoint(cfg *ServerConfig) (*Endpoint, error) {
	eps := ServerEndpoints(cfg)
	if len(eps) == 0 {
		return nil, ErrNoTransports
	}
	return &eps[0], nil
}

// ServerEndpoints returns every endpoint the config resolves to, in the order
// they are advertised. An agent serving both JSON-RPC and gRPC has two, and a
// host printing a startup summary wants both lines.
func ServerEndpoints(cfg *ServerConfig) []Endpoint {
	transports := cfg.resolvedTransports()
	out := make([]Endpoint, 0, len(transports))
	for _, t := range transports {
		out = append(out, Endpoint{
			Transport: t,
			Protocol:  t.protocol(),
			URL:       cfg.publicURLFor(t),
			CardURL:   cfg.cardURL(),
		})
	}
	return out
}

// cardURL is where this server publishes its card, or "" when it publishes
// none — a shared-mux agent whose host mounts the card instead.
func (c *ServerConfig) cardURL() string {
	if !c.ServeAgentCard && c.Mux != nil {
		return ""
	}
	base := c.PublicURL
	if base == "" {
		base = "http://" + hostPortOrDefault(c.resolvedAddr())
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return base + AgentCardPath
	}
	// The card is at the host root by protocol, not under the agent's base
	// path — clients look for exactly one place and this is it.
	u.Path = AgentCardPath
	return u.String()
}

// ResolveBasePath is where the HTTP transports mount, in precedence order:
// a generated path, then [ServerConfig.GeneratedBasePath], then
// [ServerConfig.BasePath], then [DefaultBasePath].
//
// Generated wins over configured on the same reasoning as the MCP runtime: a
// path a generator derived from the agent's proto is part of a contract clients
// already hold, while a hand-set one is a preference. A caller who genuinely
// needs to move a generated endpoint changes it at the source.
//
// The result always begins with a slash and never ends with one, so callers can
// join onto it without guessing.
func ResolveBasePath(cfg *ServerConfig, generatedDefault string) string {
	for _, candidate := range []string{generatedDefault, cfg.GeneratedBasePath, cfg.BasePath} {
		if candidate = strings.TrimSpace(candidate); candidate != "" {
			return normalizeBasePath(candidate)
		}
	}
	return DefaultBasePath
}

// normalizeBasePath makes a path rooted and unslashed, so "/" stays "/" and
// "a2a/" becomes "/a2a".
func normalizeBasePath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = path.Clean(p)
	if p == "." {
		return "/"
	}
	return p
}
