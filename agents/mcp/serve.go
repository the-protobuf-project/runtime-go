package mcp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// shutdownTimeout bounds the graceful drain of an HTTP transport once the
// caller's context is canceled.
const shutdownTimeout = 5 * time.Second

// StartServer starts the MCP server using the configured transport(s).
// Multiple transports run concurrently -- HTTP-based transports share a
// single net/http server while stdio gets its own mcpsdk.Server instance.
// This call blocks until the context is canceled (HTTP transports are then
// drained gracefully and nil is returned) or a serve error occurs.
func StartServer(ctx context.Context, cfg *MCPServerConfig, register func(s *mcpsdk.Server)) error {
	// Defaults are resolved on a copy so a caller sharing one config across
	// several servers never observes another server's resolved state.
	resolved := *cfg
	cfg = &resolved

	transports := cfg.Transports
	if len(transports) == 0 {
		t := cfg.Transport
		if t == "" {
			t = TransportStreamableHTTP
		}
		transports = []Transport{t}
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.GeneratedBasePath != "" {
		cfg.BasePath = cfg.GeneratedBasePath
	} else if cfg.BasePath == "" {
		cfg.BasePath = "/mcp"
	}

	var httpTransports []Transport
	hasStdio := false
	for _, t := range transports {
		switch t {
		case TransportStdio:
			hasStdio = true
		case TransportSSE, TransportStreamableHTTP:
			httpTransports = append(httpTransports, t)
		default:
			return fmt.Errorf("runtime: unsupported transport %q", t)
		}
	}

	// Notify caller that BasePath is resolved.
	if cfg.OnReady != nil {
		cfg.OnReady(cfg)
	}

	// Shared-mux mode: mount on the host's mux and let it own the listener.
	if cfg.Mux != nil && len(httpTransports) > 0 {
		httpServer := NewMCPServer(cfg)
		register(httpServer)
		mountHTTPTransports(cfg.Mux, httpServer, cfg, httpTransports)
		if hasStdio {
			stdioServer := NewMCPServer(cfg)
			register(stdioServer)
			return serveStdio(ctx, stdioServer)
		}
		<-ctx.Done()
		return nil
	}

	// Start HTTP transport(s) if requested. Header-mapping middleware is
	// applied per mounted handler inside buildHTTPMux.
	if len(httpTransports) > 0 {
		httpServer := NewMCPServer(cfg)
		register(httpServer)

		srv := &http.Server{
			Addr:         cfg.Addr,
			Handler:      buildHTTPMux(httpServer, cfg, httpTransports),
			ReadTimeout:  cfg.ReadTimeout,  // 0 = no limit; progress requests must not time out
			WriteTimeout: cfg.WriteTimeout, // 0 = no limit; streaming progress must not time out
		}
		if hasStdio {
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Printf("runtime: HTTP server error: %v", err)
				}
			}()
			// The stdio path below blocks on ctx; drain HTTP when it ends.
			go func() {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
				defer cancel()
				_ = srv.Shutdown(shutdownCtx)
			}()
		} else {
			errCh := make(chan error, 1)
			go func() { errCh <- srv.ListenAndServe() }()
			select {
			case err := <-errCh:
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					return err
				}
				return nil
			case <-ctx.Done():
				shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
				defer cancel()
				_ = srv.Shutdown(shutdownCtx)
				return ctx.Err()
			}
		}
	}

	if hasStdio {
		stdioServer := NewMCPServer(cfg)
		register(stdioServer)
		return serveStdio(ctx, stdioServer)
	}
	return fmt.Errorf("runtime: no transports configured")
}

// buildHTTPMux registers HTTP-based transports on a fresh ServeMux.
func buildHTTPMux(server *mcpsdk.Server, cfg *MCPServerConfig, transports []Transport) *http.ServeMux {
	mux := http.NewServeMux()
	mountHTTPTransports(mux, server, cfg, transports)
	return mux
}

// mountHTTPTransports registers the HTTP-based transport handlers for one MCP
// server on mux at cfg.BasePath. Distinct base paths let many MCP services
// share one mux (and therefore one port); header-mapping middleware is applied
// per handler so shared-mux mounts keep their own mappings.
func mountHTTPTransports(mux *http.ServeMux, server *mcpsdk.Server, cfg *MCPServerConfig, transports []Transport) {
	wrap := func(h http.Handler) http.Handler { return HeadersMiddleware(cfg.HeaderMappings, h) }
	for _, t := range transports {
		switch t {
		case TransportStreamableHTTP:
			h := mcpsdk.NewStreamableHTTPHandler(func(_ *http.Request) *mcpsdk.Server { return server }, cfg.StreamableHTTPOptions)
			mux.Handle(cfg.BasePath, wrap(h))
		case TransportSSE:
			h := mcpsdk.NewSSEHandler(func(_ *http.Request) *mcpsdk.Server { return server }, cfg.SSEOptions)
			mux.Handle(cfg.BasePath+"/", wrap(h))
		}
	}
	if cfg.HealthCheckPath != "" && cfg.HealthCheckConn != nil {
		path := cfg.HealthCheckPath
		if path[0] != '/' {
			path = "/" + path
		}
		mux.Handle(path, HealthCheckHandler(cfg.HealthCheckConn))
	}
}

func serveStdio(ctx context.Context, server *mcpsdk.Server) error {
	log.SetOutput(os.Stderr)
	return server.Run(ctx, &mcpsdk.StdioTransport{})
}
