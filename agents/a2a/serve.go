package a2a

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/the-protobuf-project/runtime-go/agents/shared"
)

// NewRequestHandler builds the transport-agnostic handler every transport
// wraps. Most callers want [StartServer] instead; this is for a host that
// mounts the transports itself.
//
// The card is attached as the extended agent card, which is what an
// authenticated client receives when it asks for more detail than the public
// card carries.
func NewRequestHandler(cfg *ServerConfig, executor a2asrv.AgentExecutor) a2asrv.RequestHandler {
	opts := make([]a2asrv.RequestHandlerOption, 0, len(cfg.HandlerOptions)+1)
	opts = append(opts, a2asrv.WithExtendedAgentCard(BuildCard(cfg)))
	opts = append(opts, cfg.HandlerOptions...)
	return a2asrv.NewHandler(executor, opts...)
}

// AgentCardHandler serves cfg's public card. A host sharing one mux across
// several agents mounts exactly one of these at [AgentCardPath] itself, since
// the well-known path admits one card per host.
func AgentCardHandler(cfg *ServerConfig) http.Handler {
	return a2asrv.NewStaticAgentCardHandler(BuildCard(cfg))
}

// StartServer serves the agent over every configured transport and blocks
// until ctx is canceled.
//
// What it owns depends on the config. With [ServerConfig.Mux] set it mounts
// handlers and nothing else — the host opened the listener and will close it.
// Without one it opens an HTTP server on [ServerConfig.Addr] and drains it on
// the way out, returning nil for an ordinary shutdown. The gRPC transport is
// never owned either way: it registers on [ServerConfig.GRPCServer] and that
// server's lifecycle is the caller's.
//
// Defaults resolve onto a copy, so a caller reusing one config across several
// servers never sees another server's resolved state written back into it.
func StartServer(ctx context.Context, cfg *ServerConfig, executor a2asrv.AgentExecutor) error {
	resolved := *cfg
	cfg = &resolved

	transports := cfg.resolvedTransports()
	if len(transports) == 0 {
		return ErrNoTransports
	}
	cfg.Transports = transports
	cfg.Addr = cfg.resolvedAddr()
	cfg.BasePath = cfg.resolvedBasePath()
	cfg.GeneratedBasePath = ""

	ownsMux := cfg.Mux == nil
	if ownsMux {
		cfg.ServeAgentCard = true
	}

	handler := NewRequestHandler(cfg, executor)

	if cfg.has(TransportGRPC) {
		if cfg.GRPCServer == nil {
			return ErrNoGRPCServer
		}
		a2agrpc.NewHandler(handler).RegisterWith(cfg.GRPCServer)
	}

	serveJSONRPC, serveREST := cfg.has(TransportJSONRPC), cfg.has(TransportREST)
	if serveJSONRPC && serveREST && cfg.BasePath == "/" {
		return ErrBasePathConflict
	}

	if !serveJSONRPC && !serveREST {
		// gRPC only: everything is mounted on the caller's server already, and
		// there is nothing here to listen on. Stay alive so the caller's
		// lifetime still governs this one.
		if cfg.OnReady != nil {
			cfg.OnReady(cfg)
		}
		<-ctx.Done()
		return nil
	}

	mux := cfg.Mux
	if ownsMux {
		mux = http.NewServeMux()
	}

	wrap := func(h http.Handler) http.Handler {
		return shared.HeadersMiddleware(cfg.HeaderMappings, h)
	}
	if serveJSONRPC {
		mux.Handle(cfg.BasePath, wrap(a2asrv.NewJSONRPCHandler(handler, cfg.TransportOptions...)))
	}
	if serveREST {
		// The REST handler routes on absolute paths of its own (/v1/...,
		// /tasks/...), so it gets the subtree with the prefix stripped back off.
		prefix := strings.TrimSuffix(cfg.BasePath, "/")
		rest := http.StripPrefix(prefix, a2asrv.NewRESTHandler(handler, cfg.TransportOptions...))
		mux.Handle(prefix+"/", wrap(rest))
	}
	if cfg.ServeAgentCard && !cardPathTaken(mux) {
		mux.Handle(AgentCardPath, AgentCardHandler(cfg))
	}

	if cfg.OnReady != nil {
		cfg.OnReady(cfg)
	}

	if !ownsMux {
		<-ctx.Done()
		return nil
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		// The drain has to outlive the context that triggered it, or it would
		// be canceled before a single in-flight request finished.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	}
}

// shutdownTimeout bounds the graceful drain of an owned HTTP server once the
// caller's context is canceled.
const shutdownTimeout = 5 * time.Second

// cardPathTaken reports whether something already answers at [AgentCardPath] on
// mux.
//
// The well-known path admits one card per host by protocol, so two agents on
// one mux cannot both have it — and http.ServeMux answers a duplicate
// registration with a panic. Probing first makes the rule "the first agent to
// mount publishes the card" instead of "the second agent kills the process",
// which is the same outcome a host would get by coordinating this itself.
func cardPathTaken(mux *http.ServeMux) bool {
	probe := &http.Request{
		Method: http.MethodGet,
		Host:   "localhost",
		URL:    &url.URL{Path: AgentCardPath},
	}
	_, pattern := mux.Handler(probe)
	return pattern == AgentCardPath
}
