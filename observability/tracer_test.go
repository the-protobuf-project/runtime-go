package observability_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sdk "github.com/the-protobuf-project/telemetry/telemetry-go"

	"github.com/the-protobuf-project/runtime-go/observability"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// traced adopts an SDK instance whose spans land in a recorder, and returns
// the client every accessor now hands out.
func traced(t *testing.T) (*observability.Client, *sdk.SpanRecorder) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENABLED", "false")
	rec := sdk.NewSpanRecorder()
	otel, err := sdk.New().WithService("tracer-test", "1.0.0").WithSpanRecorder(rec).WithShutdownTimeout(time.Second).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(func() { _ = otel.Close() })
	observability.Adopt(otel)
	return observability.Lazy("tracer-test", "1.0.0")(), rec
}

func TestAZeroClientTracesNothing(t *testing.T) {
	var c *observability.Client
	if c.Tracer() != telemetry.NoopTracer {
		t.Fatal("a nil Client should hand out the no-op tracer")
	}
}

func TestATraceCrossesHeadersIntoAChildSpan(t *testing.T) {
	c, rec := traced(t)
	tr := c.Tracer()

	ctx, send := tr.Start(t.Context(), "streams.send",
		telemetry.WithSpanKind(telemetry.SpanKindProducer),
		telemetry.WithSpanAttributes(map[string]any{"messaging.destination.name": "order.placed"}))
	headers := map[string]string{}
	tr.Inject(ctx, headers)
	send.SetOK()
	send.End()

	received := time.Now().Add(-time.Millisecond)
	_, process := tr.Start(tr.Extract(context.Background(), headers), "streams.process",
		telemetry.WithSpanKind(telemetry.SpanKindConsumer), telemetry.WithStartTime(received))
	process.SetError(errors.New("handler failed"))
	process.End()

	spans := rec.Spans()
	if len(spans) != 2 {
		t.Fatalf("spans %d", len(spans))
	}
	s, p := spans[0], spans[1]
	if s.Kind != sdk.SpanKindProducer || p.Kind != sdk.SpanKindConsumer {
		t.Errorf("kinds %v %v", s.Kind, p.Kind)
	}
	if p.TraceID != s.TraceID || p.ParentSpanID != s.SpanID {
		t.Error("the process span is not the send span's child")
	}
	if !p.Start.Equal(received) || !p.Failed || !s.OK {
		t.Errorf("start %v failed %v ok %v", p.Start, p.Failed, s.OK)
	}
	if s.Attributes["messaging.destination.name"] != "order.placed" {
		t.Errorf("attributes %v", s.Attributes)
	}
}

func TestAnObserverOperationIsTraced(t *testing.T) {
	c, rec := traced(t)
	obs := telemetry.Signals{Tracer: c.Tracer(), Meter: c.Meter()}.Observer("streams")

	ctx, op := obs.Start(t.Context(), "nats", "send", telemetry.SpanKindProducer)
	_, encode := obs.Start(ctx, "nats", "encode", telemetry.SpanKindInternal)
	encode.Finish(telemetry.OutcomeOK)
	op.FinishErr(context.DeadlineExceeded)

	spans := rec.Spans()
	if len(spans) != 2 || spans[0].Name != "streams.encode" || spans[1].Name != "streams.send" {
		t.Fatalf("spans %+v", spans)
	}
	if spans[0].ParentSpanID != spans[1].SpanID {
		t.Error("encode is not send's child")
	}
	if !spans[1].Failed || spans[1].Attributes["outcome"] != "timeout" {
		t.Errorf("a timed-out send should be failed: %+v", spans[1])
	}
}

func TestStartLinksTheGivenContexts(t *testing.T) {
	c, rec := traced(t)
	tr := c.Tracer()

	a, sa := tr.Start(t.Context(), "producer.a")
	b, sb := tr.Start(t.Context(), "producer.b")
	sa.End()
	sb.End()
	_, batch := tr.Start(t.Context(), "batch", telemetry.WithLinks(a, b))
	batch.End()

	if links := rec.Spans()[2].Links; len(links) != 2 {
		t.Errorf("links %v", links)
	}
}
