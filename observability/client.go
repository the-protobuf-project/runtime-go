package observability

import (
	"fmt"
	"time"

	"github.com/the-protobuf-project/telemetry/telemetry-go"
)

// Log returns the [Logger] for this client. It is safe on a zero
// Client, returning [NoopLogger].
func (c *Client) Log() Logger {
	if c == nil || c.otel == nil || c.otel.Logger == nil {
		return NoopLogger
	}
	c.logOnce.Do(func() { c.log = newLogger(c.otel) })
	return c.log
}

// Meter returns the [Meter] for this client.
//
// opentelemetry implements the contract itself, so this is a pass-through
// rather than an adapter. It is safe on a zero Client, returning
// [NoopMeter].
func (c *Client) Meter() Meter {
	if c == nil || c.otel == nil {
		return NoopMeter
	}
	return c.otel.Meter()
}

// Otel exposes the underlying SDK client for the parts of it this package does
// not front — tracing and profiling.
//
// It returns nil for a zero Client, so check before use.
func (c *Client) Otel() *telemetry.Telemetry {
	if c == nil {
		return nil
	}
	return c.otel
}

// Close flushes and shuts down the backend. The main application should call it
// on shutdown; it is safe on a zero Client.
//
// It gives up after [Options.CloseTimeout] and reports that as an error rather
// than blocking. The SDK's own shutdown takes no context and waits 20 seconds
// for an OTLP collector it configures by default, so an unbounded Close would
// stall every exit that happens without one.
//
// The flush is left running when it times out: the process is on its way out,
// and a goroutine parked on a socket is cheaper than a delayed exit.
func (c *Client) Close() error {
	if c == nil || c.otel == nil {
		return nil
	}

	done := make(chan error, 1) // buffered so the send never blocks after a timeout
	go func() { done <- c.otel.Close() }()

	timeout := c.closeTimeout
	if timeout <= 0 {
		timeout = DefaultCloseTimeout
	}

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("observability: shutdown did not finish within %v; some telemetry may be unflushed", timeout)
	}
}

// Tracer returns the [Tracer] for this client. It is safe on a zero Client,
// returning [NoopTracer] — tracing that was never configured still runs the
// traced function, it just records nothing.
func (c *Client) Tracer() Tracer {
	if c == nil || c.otel == nil || c.otel.Tracing == nil {
		return NoopTracer
	}
	return sdkTracer{otel: c.otel}
}

// Client implements [Telemetry].
var _ Telemetry = (*Client)(nil)
