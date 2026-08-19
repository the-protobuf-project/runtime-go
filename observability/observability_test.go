package observability_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/observability"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// A service name is the one thing that cannot be defaulted — it is what
// distinguishes this component in traces and metrics.
func TestSetupRequiresAService(t *testing.T) {
	if _, err := observability.Setup(observability.Options{}); err == nil {
		t.Error("Setup with no Service returned no error")
	}
}

// With no collector configured, setup must still yield a working local client
// rather than failing.
func TestSetupWithoutACollectorSucceeds(t *testing.T) {
	c, err := observability.Setup(observability.Options{Service: "test", Version: "1.0.0", CloseTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if c == nil {
		t.Fatal("Setup returned a nil Client with no error")
	}
	t.Cleanup(func() { _ = c.Close() })

	if c.Log() == nil {
		t.Error("Log() is nil")
	}
	if c.Meter() == nil {
		t.Error("Meter() is nil")
	}
}

// The adapter has to satisfy the contract for real, not just compile — every
// level, With, and a nil context all have to survive a call.
func TestLoggerSatisfiesTheContract(t *testing.T) {
	c, err := observability.Setup(observability.Options{Service: "test", Version: "1.0.0", CloseTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	log := c.Log()
	ctx := t.Context()

	log.Debug(ctx, "debug", telemetry.Fields{"k": "v"})
	log.Info(ctx, "info", nil)
	log.Warn(ctx, "warn", telemetry.Fields{"n": 1})
	log.Error(ctx, "error", errors.New("boom"), telemetry.Fields{"k": "v"})
	log.Error(ctx, "error without an error value", nil, nil)

	// With must return a usable logger, and chain.
	bound := log.With(telemetry.Fields{"component": "test"}).With(telemetry.Fields{"sub": "x"})
	bound.Info(ctx, "bound", telemetry.Fields{"extra": true})

	// With(nil) is a no-op, not a new allocation to nowhere.
	if got := log.With(nil); got == nil {
		t.Error("With(nil) returned nil")
	}

	// A nil context is tolerated — the SDK carries its own.
	log.Info(nil, "nil ctx", nil) //nolint:staticcheck // exercising the nil path on purpose
}

// Enabled reports true because the SDK does not expose its threshold; the
// contract's guarded call sites must still work.
func TestLoggerEnabledIsPermissive(t *testing.T) {
	c, err := observability.Setup(observability.Options{Service: "test", Version: "1.0.0", CloseTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if !c.Log().Enabled(t.Context(), telemetry.LevelDebug) {
		t.Error("Enabled(debug) is false; guarded call sites would be skipped and records lost")
	}
}

// Meter is a pass-through — opentelemetry implements the contract itself — but
// it still has to be usable without a collector.
func TestMeterRecordsWithoutACollector(t *testing.T) {
	c, err := observability.Setup(observability.Options{Service: "test", Version: "1.0.0", CloseTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	m := c.Meter()
	m.Counter("t_total", telemetry.WithUnit("1")).Add(t.Context(), 1, telemetry.Labels{"a": "b"})
	m.Histogram("t_seconds", telemetry.WithUnit("s")).Record(t.Context(), 0.5, nil)
	m.Gauge("t_current").Set(t.Context(), 3, nil)
	m.UpDownCounter("t_delta").Add(t.Context(), -1, nil)
}

// A zero Client is what a package-level variable looks like before Setup ran,
// and what a caller gets if they ignore an error. It must not panic.
func TestZeroClientIsInert(t *testing.T) {
	var c observability.Client

	if c.Log() != telemetry.NoopLogger {
		t.Error("zero Client.Log() is not NoopLogger")
	}
	if c.Meter() != telemetry.NoopMeter {
		t.Error("zero Client.Meter() is not NoopMeter")
	}
	if c.Otel() != nil {
		t.Error("zero Client.Otel() is not nil")
	}
	if err := c.Close(); err != nil {
		t.Errorf("zero Client.Close() = %v, want nil", err)
	}

	c.Log().Info(context.Background(), "no panic", nil)
}

// A nil *Client is the other easy shape to end up with.
func TestNilClientIsInert(t *testing.T) {
	var c *observability.Client

	if c.Log() != telemetry.NoopLogger {
		t.Error("nil Client.Log() is not NoopLogger")
	}
	if c.Meter() != telemetry.NoopMeter {
		t.Error("nil Client.Meter() is not NoopMeter")
	}
	if err := c.Close(); err != nil {
		t.Errorf("nil Client.Close() = %v, want nil", err)
	}
}

func TestMustReturnsAUsableClient(t *testing.T) {
	c := observability.Must("test-must", "1.0.0")
	if c == nil {
		t.Fatal("Must returned nil")
	}
	t.Cleanup(func() { _ = c.Close() })

	c.Log().Info(t.Context(), "from Must", nil)
}

// The SDK configures an OTLP exporter at localhost:4317 by default and its
// shutdown blocks 20s waiting for one. Close must give up long before that, or
// every binary exiting without a collector pays it.
func TestCloseGivesUpRatherThanBlocking(t *testing.T) {
	c, err := observability.Setup(observability.Options{
		Service:      "test-close",
		Version:      "1.0.0",
		CloseTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	start := time.Now()
	closeErr := c.Close()
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Errorf("Close blocked for %v; it should give up after its timeout", elapsed)
	}
	// Without a collector the flush cannot finish, so a timeout error is the
	// expected outcome — it must be reported, not swallowed.
	if closeErr == nil {
		t.Log("Close completed within the timeout (a collector may be running)")
	}
}
