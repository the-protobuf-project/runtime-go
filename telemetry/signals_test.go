package telemetry_test

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	rec "github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

func TestAnOperationReportsEverySignal(t *testing.T) {
	r := rec.New()
	obs := r.Signals().Observer("streams")

	ctx, err := obs.SetBaggage(context.Background(), "tenant", "acme")
	if err != nil {
		t.Fatalf("SetBaggage: %v", err)
	}
	_, op := obs.Start(ctx, "nats", "publish", telemetry.SpanKindProducer)
	op.Finish(telemetry.OutcomeOK)

	if got := r.Sum("runtime.operation.count"); got != 1 {
		t.Errorf("metrics: runtime.operation.count = %v, want 1", got)
	}
	span := r.Span("streams.publish")
	if span == nil {
		t.Fatal("tracing: no streams.publish span")
	}
	if got := span.Attr("baggage.tenant"); got != "acme" {
		t.Errorf("tracing: span baggage.tenant = %v, want acme", got)
	}
	logs := r.Logs()
	if len(logs) != 1 || logs[0].Fields["baggage.tenant"] != "acme" {
		t.Errorf("logging: want one completion log carrying baggage.tenant, got %+v", logs)
	}
	profiles := r.Profiles()
	want := telemetry.Labels{"component": "streams", "backend": "nats", "operation": "publish"}
	if len(profiles) != 1 || len(profiles[0]) != len(want) {
		t.Fatalf("profiling: want one labeled section %v, got %v", want, profiles)
	}
	for k, v := range want {
		if profiles[0][k] != v {
			t.Errorf("profiling: label %s = %q, want %q", k, profiles[0][k], v)
		}
	}
}

func TestBaggageCrossesAProcessBoundary(t *testing.T) {
	r := rec.New()
	producer := r.Signals().Observer("streams")
	ctx, _ := producer.SetBaggage(context.Background(), "tenant", "acme")
	ctx, op := producer.Start(ctx, "nats", "publish", telemetry.SpanKindProducer)
	headers := map[string]string{}
	producer.Tracer().Inject(ctx, headers)
	op.Finish(telemetry.OutcomeOK)

	consumer := r.Signals().Observer("events")
	received := consumer.Tracer().Extract(context.Background(), headers)
	if got := consumer.Baggage(received)["tenant"]; got != "acme" {
		t.Errorf("the consumer's baggage tenant = %q, want acme (headers %v)", got, headers)
	}
}

func TestTheZeroSignalsCarryNoBaggageAndLabelNothing(t *testing.T) {
	obs := telemetry.Signals{}.Observer("cache")
	ctx, err := obs.SetBaggage(context.Background(), "tenant", "acme")
	if err != nil || ctx != context.Background() {
		t.Errorf("SetBaggage without Baggage: ctx changed or err %v", err)
	}
	_, op := obs.Start(ctx, "redis", "get", telemetry.SpanKindClient)
	op.Finish(telemetry.OutcomeOK)
}

func TestThePprofProfilerLabelsTheContextAndRestores(t *testing.T) {
	parent := context.Background()
	labeled, restore := telemetry.PprofProfiler.Label(parent, telemetry.Labels{"component": "database", "operation": "get"})
	defer restore()
	if v, ok := pprof.Label(labeled, "component"); !ok || v != "database" {
		t.Errorf("pprof label component = %q, %v; want database", v, ok)
	}
	if _, ok := pprof.Label(parent, "component"); ok {
		t.Error("the parent context gained a label")
	}
}
