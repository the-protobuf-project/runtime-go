package observability

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/the-protobuf-project/opentelementry/opentelementry-go"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Client is a configured telemetry backend. Build one with [Setup] or [Must].
//
// The zero Client is usable: its Log and Meter return the no-op contracts, so
// a caller that never set one up still works.
type Client struct {
	otel         *opentelementry.Opentelementry
	closeTimeout time.Duration

	logOnce sync.Once
	log     telemetry.Logger
}

// Options configure a [Client]. The zero Options is valid — it produces a
// client that logs locally and exports nothing.
type Options struct {
	// Service names the component in traces, metrics, and log records.
	// Required.
	Service string

	// Version is the component's version, reported alongside Service.
	Version string

	// OTLPHost and OTLPPort address a collector. When Host is empty no
	// exporter is configured and records stay local, which is what a test or a
	// developer laptop wants.
	OTLPHost string
	OTLPPort int

	// Tracing enables the tracing pipeline.
	Tracing bool

	// CloseTimeout bounds how long [Client.Close] waits for the backend to
	// flush. Zero means [DefaultCloseTimeout].
	//
	// A bound is needed because the SDK configures an OTLP exporter at
	// localhost:4317 by default — even when no collector was asked for — and
	// its shutdown blocks for 20 seconds trying to reach one. Without this,
	// every binary that exits without a collector running pays that on the way
	// out.
	CloseTimeout time.Duration
}

// DefaultCloseTimeout is how long [Client.Close] waits for a flush when
// [Options.CloseTimeout] is not set. It is short because the alternative to
// giving up is delaying process exit, and telemetry is not worth that.
const DefaultCloseTimeout = 5 * time.Second

// Setup builds a Client.
//
// A failure to configure the exporters is reported, not fatal: the returned
// Client still logs locally, because a library cannot decide that its host
// binary should not start just because a collector is unreachable. The error is
// returned so a caller that does care can act on it.
func Setup(opts Options) (*Client, error) {
	if opts.Service == "" {
		return nil, fmt.Errorf("observability: Options.Service is required")
	}
	if opts.Version == "" {
		opts.Version = "0.0.0"
	}

	build := func(withExporters bool) (*opentelementry.Opentelementry, error) {
		b := opentelementry.New().
			WithService(opts.Service, opts.Version).
			WithLogLevel(opentelementry.ModuleLevel_2)
		if withExporters {
			if opts.OTLPHost != "" {
				b = b.WithOTLP(opts.OTLPHost, opts.OTLPPort)
			}
			if opts.Tracing {
				b = b.WithTracing()
			}
		}
		return b.Build()
	}

	timeout := opts.CloseTimeout
	if timeout <= 0 {
		timeout = DefaultCloseTimeout
	}

	o, err := build(true)
	if err == nil {
		return &Client{otel: o, closeTimeout: timeout}, nil
	}

	// The exporters are what usually fail — no collector, a bad endpoint — and
	// they are also what a library can do without. Retry with the smallest
	// configuration that still yields a logger.
	fallback, ferr := build(false)
	if ferr != nil {
		return nil, fmt.Errorf("observability: telemetry setup failed (%w) and the fallback also failed: %w", err, ferr)
	}
	return &Client{otel: fallback, closeTimeout: timeout},
		fmt.Errorf("observability: exporters unavailable, continuing with local logging only: %w", err)
}

// Must is [Setup] for a package-level variable, where there is no caller to
// return an error to.
//
// It does not panic on an exporter failure — that is reported on stderr and the
// returned Client logs locally. It panics only if no usable client could be
// built at all, which means the process has no logging and cannot report what
// went wrong any other way.
func Must(service, version string) *Client {
	c, err := Setup(Options{Service: service, Version: version})
	if c != nil {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
		return c
	}
	panic(err)
}

// Lazy returns an accessor that builds a Client on first call and reuses it
// after.
//
// This is what a package-level variable should hold. [Must] runs at
// initialization, which means a package that is merely imported — for a
// registration side effect, say — stands up an exporter and starts talking to
// the network before main does. Lazy defers all of that until something
// actually logs.
//
// An exporter failure is reported on stderr once and the returned Client logs
// locally. If no client can be built at all the accessor returns nil, which the
// Client methods tolerate by falling back to the no-op contracts.
func Lazy(service, version string) func() *Client {
	var (
		once   sync.Once
		client *Client
	)
	return func() *Client {
		once.Do(func() {
			c, err := Setup(Options{Service: service, Version: version})
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
			}
			client = c
		})
		return client
	}
}

// Log returns the [telemetry.Logger] for this client. It is safe on a zero
// Client, returning [telemetry.NoopLogger].
func (c *Client) Log() telemetry.Logger {
	if c == nil || c.otel == nil || c.otel.Logger == nil {
		return telemetry.NoopLogger
	}
	c.logOnce.Do(func() { c.log = newLogger(c.otel) })
	return c.log
}

// Meter returns the [telemetry.Meter] for this client.
//
// opentelementry implements the contract itself, so this is a pass-through
// rather than an adapter. It is safe on a zero Client, returning
// [telemetry.NoopMeter].
func (c *Client) Meter() telemetry.Meter {
	if c == nil || c.otel == nil {
		return telemetry.NoopMeter
	}
	return c.otel.Meter()
}

// Otel exposes the underlying SDK client for the parts of it this package does
// not front — tracing and profiling.
//
// It returns nil for a zero Client, so check before use.
func (c *Client) Otel() *opentelementry.Opentelementry {
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
