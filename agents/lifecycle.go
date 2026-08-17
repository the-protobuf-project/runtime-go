package agents

import (
	"context"
	"errors"
	"fmt"
)

// Serve starts the runtime and blocks until ctx is done, then drains whatever
// it opened. It is [Runtime.Start], a wait, and [Runtime.Shutdown] — the shape
// a process that does nothing else wants.
func (r *Runtime) Serve(ctx context.Context) error {
	if err := r.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()

	// The drain has to outlive the context that triggered it, or it would be
	// canceled before a single in-flight request finished.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	return r.Shutdown(drainCtx)
}

// Shutdown stops every service and drains the listeners this runtime opened. A
// mux it was given is left alone — the host owns that server and closing it
// here would take out whatever else is on it.
//
// It is safe to call on a runtime that never started, and safe to call twice.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	cancel, servers := r.cancel, r.servers
	r.cancel, r.servers = nil, nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var errs []error
	for _, srv := range servers {
		if err := srv.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("agents: draining %s: %w", srv.Addr, err))
		}
	}
	return errors.Join(errs...)
}
