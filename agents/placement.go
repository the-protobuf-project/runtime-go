package agents

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"time"
)

// group is one listen address and the services that share it.
type group struct {
	addr     string
	mux      *http.ServeMux
	services []Service
	http     bool // any service here mounts HTTP handlers
	owned    bool // this runtime opens the listener, rather than a host's mux
}

// group sorts services by the address they need. Everything that named none
// shares the runtime's, which is what makes one process serving two protocols
// bind one port.
func (r *Runtime) group(services []Service) []*group {
	shared := fmt.Sprintf("%s:%d", r.cfg.Host, r.cfg.Port)

	byAddr := map[string]*group{}
	var order []string

	for _, s := range services {
		req := s.Requires()
		addr := req.Addr
		if addr == "" {
			addr = shared
		}

		g, ok := byAddr[addr]
		if !ok {
			g = &group{addr: addr}
			// A host's mux is only the shared group's. A service that asked for
			// an address of its own asked to be somewhere else, and mounting it
			// on the host's mux would put it on the host's port instead.
			if r.cfg.Mux != nil && addr == shared {
				g.mux, g.owned = r.cfg.Mux, false
			} else {
				g.mux, g.owned = http.NewServeMux(), true
			}
			byAddr[addr] = g
			order = append(order, addr)
		}
		g.services = append(g.services, s)
		g.http = g.http || req.HTTP
	}

	sort.Strings(order)
	out := make([]*group, 0, len(order))
	for _, addr := range order {
		out = append(out, byAddr[addr])
	}
	return out
}

// listen opens a server for every group that owns its listener and has
// something on it.
func (r *Runtime) listen(groups []*group) ([]*http.Server, error) {
	var servers []*http.Server
	for _, g := range groups {
		if !g.owned || !g.http {
			continue
		}
		srv := &http.Server{
			Addr:              g.addr,
			Handler:           g.mux,
			ReadTimeout:       r.cfg.ReadTimeout,
			WriteTimeout:      r.cfg.WriteTimeout,
			ReadHeaderTimeout: 10 * time.Second,
		}
		lis, err := listen(g.addr)
		if err != nil {
			// Close whatever already came up, so a failed start leaves no
			// half-bound process behind.
			for _, opened := range servers {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("agents: listening on %s: %w", g.addr, err)
		}
		servers = append(servers, srv)
		go func() {
			if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				// Nothing here can act on this: the listener is gone and the
				// caller is long past Start. Shutdown reports the drain; this
				// is the one path with no one to tell.
				_ = err
			}
		}()
	}
	return servers, nil
}

// listen binds addr. It goes through a ListenConfig rather than net.Listen so
// the bind is cancellable, which matters when a runtime starts under a context
// that is already on its way out.
func listen(addr string) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(context.Background(), "tcp", addr)
}
