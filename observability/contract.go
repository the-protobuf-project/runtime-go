package observability

// The runtime-go/telemetry side of Client. observability.go imports the SDK,
// whose package is also named telemetry, so the contract lives here.

import (
	"context"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Log returns the [telemetry.Logger] for this client. It is safe on a zero
// Client, returning [telemetry.NoopLogger].
func (c *Client) Log() telemetry.Logger {
	if c == nil || c.otel == nil || c.otel.Logger == nil {
		return telemetry.NoopLogger
	}
	c.logOnce.Do(func() { c.log = logger{bind: c.bindLogger} })
	return c.log
}

// Meter returns the [telemetry.Meter] for this client.
//
// The SDK implements the contract itself, so this is a pass-through rather
// than an adapter. It is safe on a zero Client, returning
// [telemetry.NoopMeter].
func (c *Client) Meter() telemetry.Meter {
	if c == nil || c.otel == nil {
		return telemetry.NoopMeter
	}
	return c.otel.Meter()
}

// Profiler returns the [telemetry.Profiler] for this client: pprof labels,
// which the SDK's continuous profiler reads, when the client profiles
// ([Options.ProfilingServer]); [telemetry.NoopProfiler] otherwise, so an
// unprofiled process pays nothing per operation. It is safe on a zero Client.
func (c *Client) Profiler() telemetry.Profiler {
	if c == nil || c.otel == nil || !c.profiling {
		return telemetry.NoopProfiler
	}
	return telemetry.PprofProfiler
}

// Baggage returns the [telemetry.Baggage] for this client: the SDK's W3C
// baggage, which [Client.Tracer]'s Inject and Extract carry across process
// boundaries beside the trace. It is safe on a zero Client, returning
// [telemetry.NoopBaggage].
func (c *Client) Baggage() telemetry.Baggage {
	if c == nil || c.otel == nil || c.otel.Tracing == nil {
		return telemetry.NoopBaggage
	}
	return baggage{c}
}

// baggage adapts the SDK's tracing baggage to [telemetry.Baggage].
type baggage struct{ c *Client }

func (b baggage) Set(ctx context.Context, key, value string) (context.Context, error) {
	return b.c.otel.Tracing.SetBaggage(ctx, key, value)
}

func (b baggage) Members(ctx context.Context) map[string]string {
	return b.c.otel.Tracing.Baggage(ctx)
}

// Signals returns every handle this client offers, for a runtime package's
// options: metrics, traces, logs, the profiler and baggage. It is safe on a
// zero Client, returning signals that are all no-ops.
func (c *Client) Signals() telemetry.Signals {
	return telemetry.Signals{
		Meter:    c.Meter(),
		Tracer:   c.Tracer(),
		Logger:   c.Log(),
		Profiler: c.Profiler(),
		Baggage:  c.Baggage(),
	}
}
