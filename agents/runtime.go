package agents

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Runtime is the single object: build it once with [New], register the
// protocols it should speak, then start it.
//
// It exists because the alternative is every process wiring the same four
// things by hand — identity that has to match across protocols, a mux they can
// share, a listener somebody has to own, and a shutdown that drains it. A
// runtime is not a layer over the protocol packages so much as the place those
// four decisions are made once.
type Runtime struct {
	cfg      Config
	services []Service

	mu        sync.Mutex
	started   bool
	endpoints []Endpoint
	servers   []*http.Server
	cancel    context.CancelFunc
	serveErr  chan error
}

// New builds a runtime from cfg. Nothing is bound or registered until
// [Runtime.Start].
func New(cfg Config) *Runtime {
	if cfg.Host == "" {
		cfg.Host = DefaultHost
	}
	if cfg.ReadyTimeout == 0 {
		cfg.ReadyTimeout = DefaultReadyTimeout
	}
	return &Runtime{cfg: cfg}
}

// Register adds services to the runtime and returns it, so registration chains
// from [New].
//
// It panics if the runtime has already started. Placement is resolved once, at
// start, so a late registration could only be silently dropped — and a protocol
// that was asked for and never served is worth failing on at the call site
// rather than discovering from a client. Registering nothing is fine; starting
// with nothing is [ErrNoServices].
func (r *Runtime) Register(services ...Service) *Runtime {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		panic("agents: Register called after Start")
	}
	r.services = append(r.services, services...)
	return r
}

// Config reports the settings this runtime was built with, with defaults
// filled in.
func (r *Runtime) Config() Config { return r.cfg }

// Endpoints reports where every registered protocol answers. It is empty until
// [Runtime.Start] returns, and stable afterwards.
func (r *Runtime) Endpoints() []Endpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]Endpoint, len(r.endpoints))
	copy(out, r.endpoints)
	return out
}

// Start mounts every registered service, opens the listeners they need, and
// returns once they are all answering. It does not block.
//
// The ordering is not an implementation detail. Every service mounts first and
// listeners open second, because a port that is accepting before its handlers
// are registered answers 404 to whoever got there first. A gRPC binding is
// stricter still: gRPC refuses a registration once its server is serving, so a
// runtime sharing [Config.GRPCServer] has to complete Start before the host
// calls Serve on it.
//
// ctx governs the services' lifetime. Canceling it stops them; it does not
// drain the listeners, which is [Runtime.Shutdown]'s job.
func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return ErrAlreadyStarted
	}
	if len(r.services) == 0 {
		r.mu.Unlock()
		return ErrNoServices
	}
	r.started = true
	services := r.services
	r.mu.Unlock()

	for _, s := range services {
		if s.Requires().GRPC && r.cfg.GRPCServer == nil {
			return fmt.Errorf("agents: %s needs a gRPC server and Config.GRPCServer is nil", s.Protocol())
		}
	}

	serveCtx, cancel := context.WithCancel(ctx)
	groups := r.group(services)

	errCh := make(chan error, len(services))
	var endpoints []Endpoint

	for _, g := range groups {
		for _, s := range g.services {
			p := Placement{
				Identity:   Identity{Name: r.cfg.Name, Description: r.cfg.Description, Version: r.cfg.Version},
				Addr:       g.addr,
				Mux:        g.mux,
				GRPCServer: r.cfg.GRPCServer,
				PublicURL:  r.cfg.PublicURL,
			}

			readyCh := make(chan []Endpoint, 1)
			go func(s Service, p Placement) {
				var once sync.Once
				err := s.Serve(serveCtx, p, func(eps []Endpoint) {
					once.Do(func() { readyCh <- eps })
				})
				// A service that fails before reporting ready would otherwise
				// hold up Start until the ready timeout, reporting a stall
				// where there was a real error.
				once.Do(func() { close(readyCh) })
				errCh <- err
			}(s, p)

			select {
			case eps := <-readyCh:
				endpoints = append(endpoints, eps...)
			case <-time.After(r.cfg.ReadyTimeout):
				cancel()
				return fmt.Errorf("agents: %s did not mount within %s", s.Protocol(), r.cfg.ReadyTimeout)
			case <-ctx.Done():
				cancel()
				return ctx.Err()
			}

			// The goroutine closes readyCh when Serve returns without ever
			// reporting ready, which lands here as a zero-length read.
			select {
			case err := <-errCh:
				cancel()
				if err == nil {
					err = fmt.Errorf("agents: %s stopped before it mounted", s.Protocol())
				}
				return fmt.Errorf("agents: %s failed to start: %w", s.Protocol(), err)
			default:
			}
		}
	}

	servers, err := r.listen(groups)
	if err != nil {
		cancel()
		return err
	}

	r.mu.Lock()
	r.endpoints = endpoints
	r.servers = servers
	r.cancel = cancel
	r.serveErr = errCh
	r.mu.Unlock()

	return nil
}
