package observability_test

import (
	"context"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/observability"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

func TestSignalsCarryBaggageAcrossInject(t *testing.T) {
	c, err := observability.Setup(observability.Options{Service: "signals-test", Tracing: true})
	if c == nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	s := c.Signals()
	ctx, err := s.Baggage.Set(context.Background(), "tenant", "acme")
	if err != nil {
		t.Fatalf("Baggage.Set: %v", err)
	}
	if got := s.Baggage.Members(ctx)["tenant"]; got != "acme" {
		t.Fatalf("Members tenant = %q, want acme", got)
	}

	ctx, op := s.Observer("database").Start(ctx, "postgres", "get", telemetry.SpanKindClient)
	headers := map[string]string{}
	s.Tracer.Inject(ctx, headers)
	op.Finish(telemetry.OutcomeOK)
	if !strings.Contains(headers["baggage"], "tenant=acme") {
		t.Errorf("Inject wrote baggage %q, want tenant=acme", headers["baggage"])
	}

	received := s.Tracer.Extract(context.Background(), headers)
	if got := s.Baggage.Members(received)["tenant"]; got != "acme" {
		t.Errorf("after Extract, tenant = %q, want acme", got)
	}
}

func TestAnUnprofiledClientLabelsNothing(t *testing.T) {
	c, _ := observability.Setup(observability.Options{Service: "signals-test"})
	if c == nil {
		t.Fatal("Setup returned no client")
	}
	t.Cleanup(func() { _ = c.Close() })
	if c.Profiler() != telemetry.NoopProfiler {
		t.Error("a client without ProfilingServer returned a labeling profiler")
	}
	var zero *observability.Client
	if zero.Profiler() != telemetry.NoopProfiler || zero.Baggage() != telemetry.NoopBaggage {
		t.Error("a zero Client's profiler or baggage is not the no-op")
	}
}
