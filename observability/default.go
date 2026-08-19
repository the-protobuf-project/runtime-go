package observability

import (
	"context"
	"sync"
)

// Telemetry is the one handle a caller needs: logging, metrics and tracing
// together. [Client] implements it.
type Telemetry interface {
	Log() Logger
	Meter() Meter
	Tracer() Tracer
}

// Noop is an inert Telemetry. It is the default until [SetDefault] is called,
// so a library that instruments itself costs a consumer nothing until that
// consumer opts in.
var Noop Telemetry = noopTelemetry{}

type noopTelemetry struct{}

func (noopTelemetry) Log() Logger    { return NoopLogger }
func (noopTelemetry) Meter() Meter   { return NoopMeter }
func (noopTelemetry) Tracer() Tracer { return NoopTracer }

var (
	defaultMu sync.RWMutex
	defaultT  Telemetry = Noop
)

// SetDefault installs the process-wide Telemetry. Call it once, early, from
// main:
//
//	obs := observability.Must("my-app", "1.0.0")
//	observability.SetDefault(obs)
//	defer obs.Close()
//
// Every runtime-go module reads [Log], [Meter] and [Tracer] rather than taking
// a logger or meter parameter, so this one call is what lights them all up.
// A nil argument resets to [Noop] rather than installing something that panics
// on first use.
func SetDefault(t Telemetry) {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if t == nil {
		t = Noop
	}
	defaultT = t
}

// Default returns the process-wide Telemetry, [Noop] if none was set.
func Default() Telemetry {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	return defaultT
}

// Log returns the default logger.
//
// There is deliberately no package-level Meter or Tracer accessor: those names
// are the contract types. Reach them through Default().Meter() and
// Default().Tracer(), which is not a hot path — instruments are created once
// and held, not resolved per measurement.
func Log() Logger { return Default().Log() }

// Trace runs fn inside a span on the default tracer.
func Trace(ctx context.Context, name string, fn func(context.Context, Span) error) error {
	return Default().Tracer().Trace(ctx, name, fn)
}
