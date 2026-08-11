// Package helpers provides utility functions for managing NATS request contexts
// with optional deadlines or cancellation support.
package helpers

import (
	"context"
	"time"

	"github.com/machanirobotics/loom/go/nats/shared"
)

// NatsContext wraps a context used for NATS operations.
// It includes the context itself, its cancellation function, and
// an optional absolute deadline time.
type NatsContext struct {
	// Ctx is the underlying context used in NATS operations.
	Ctx context.Context
	// CancelFunc cancels the context and should always be called
	// when the context is no longer needed to free resources.
	CancelFunc context.CancelFunc
	// Time is an optional absolute deadline. If zero, no explicit deadline
	// was set when constructing this NatsContext.
	Time time.Time
}

// HandleContext creates a NatsContext according to the provided options.
//
// Behavior:
//   - If a NatsContext with a non-nil Ctx is passed, it is returned as-is.
//   - If a NatsContext with a non-zero Time is passed, a new context with a
//     deadline at that time is created.
//   - If no context or deadline is provided, a cancellable background context
//     is created.
//
// Example:
//
//	// Use default cancellable background context
//	nc := helpers.HandleContext()
//
//	// Use explicit time limit
//	deadline := time.Now().Add(5 * time.Second)
//	nc := helpers.HandleContext(helpers.NatsContext{Time: deadline})
//
//	// Reuse an existing context
//	ctx, cancel := context.WithCancel(context.Background())
//	nc := helpers.HandleContext(helpers.NatsContext{Ctx: ctx, CancelFunc: cancel})
func HandleContext(ctx ...NatsContext) NatsContext {
	// Case 1: reuse provided context
	if len(ctx) > 0 && ctx[0].Ctx != nil {
		shared.Pulse.Logger.Debug("HandleContext reusing provided context")
		return ctx[0]
	}

	// Case 2: create context with deadline
	if len(ctx) > 0 && !ctx[0].Time.IsZero() {
		c, cancel := context.WithDeadline(context.Background(), ctx[0].Time)

		if time.Now().After(ctx[0].Time) {
			shared.Pulse.Logger.Warnf("HandleContext creating expired context (deadline=%v)", ctx[0].Time)
		} else {
			shared.Pulse.Logger.Debugf("HandleContext creating context with deadline=%v", ctx[0].Time)
		}

		return NatsContext{
			Ctx:        c,
			CancelFunc: cancel,
			Time:       ctx[0].Time,
		}
	}

	// Case 3: fallback to cancellable background context
	c, cancel := context.WithCancel(context.Background())
	shared.Pulse.Logger.Debug("HandleContext creating cancellable background context")

	return NatsContext{
		Ctx:        c,
		CancelFunc: cancel,
	}
}
