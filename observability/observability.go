package observability

// This file is the SDK side of the package; contract.go is the runtime-go
// side. Both packages are named telemetry, so each file imports one.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/the-protobuf-project/telemetry/telemetry-go"
)

// Client is a configured telemetry backend. Build one with [Setup] or [Must].
//
// The zero Client is usable: its Log and Meter return the no-op contracts, so
// a caller that never set one up still works.
type Client struct {
	otel         *telemetry.Telemetry
	closeTimeout time.Duration
	// adopted marks a Client over the host application's SDK instance (see
	// [Adopt]). The application owns that instance, so Close leaves it alone.
	adopted bool

	logOnce sync.Once
	log     logger
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

	build := func(withExporters bool) (*telemetry.Telemetry, error) {
		b := telemetry.New().
			WithService(opts.Service, opts.Version).
			WithLogLevel(telemetry.ModuleLevel_2)
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
		if a := adopted.Load(); a != nil {
			return a
		}
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
	if c == nil || c.otel == nil || c.adopted {
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

// adopted is the Client every [Lazy] accessor returns once [Adopt] has run.
var adopted atomic.Pointer[Client]

// Adopt makes every runtime-go module in the process log, trace and measure
// through the host application's SDK instance, instead of each building its
// own on first use.
//
// Without it a process runs one pipeline per module plus the application's:
// each exports under its own service name, and each SDK build installs itself
// as OTel's global tracer and meter provider, so whichever built last decides
// what the instrumentation libraries (otelgrpc, the ORM's gorm plugin) report
// as. A trace of one request then splits across service names, and metrics
// recorded through the globals land under the wrong one.
//
// Call it once, in main, right after building the application's instance and
// before serving. A module that already built its own Client keeps it for
// anything it holds, but every later accessor call returns this one. Close on
// the adopted Client does nothing: the application closes what it built.
func Adopt(sdk *telemetry.Telemetry) {
	if sdk == nil {
		return
	}
	adopted.Store(&Client{otel: sdk, adopted: true})
}

// bindLogger returns the SDK logger bound to ctx, for the contract adapter in
// logger.go. The SDK holds its context rather than taking one per call, so
// binding makes a shallow copy per record -- cheap, and the only way exported
// records carry the right trace and span IDs.
func (c *Client) bindLogger(ctx context.Context) sdkLogger {
	if ctx == nil {
		return c.otel.Logger
	}
	return c.otel.Logger.WithContext(ctx)
}
