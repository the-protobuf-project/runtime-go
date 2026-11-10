package nats_test

import (
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// Through a real JetStream server: a delivery's span is its publish's child,
// carried across in the message headers, and settles with the ack.
func TestATraceCrossesJetStream(t *testing.T) {
	rec := recorder.New()
	s := streams.Instrument(jetStream(t), rec.Signals(), "nats")
	_, m := declare(t, s, subject)

	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable through Instrument: %v", err)
	}
	deliveries, err := d.Consume(t.Context(), subject, "traced")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish(t.Context(), subject, map[string]string{"id": "u-1"}); err != nil {
		t.Fatal(err)
	}
	got := recvDelivery(t, deliveries, 5*time.Second)
	if err := got.Ack(got.Context()); err != nil {
		t.Fatal(err)
	}

	publish, deliver := rec.Span("streams.publish"), rec.Span("streams.deliver")
	if publish == nil || deliver == nil {
		t.Fatalf("spans %v", rec.Spans())
	}
	if deliver.Parent != publish.ID {
		t.Errorf("deliver's parent %q, want publish %q: the trace did not cross the broker", deliver.Parent, publish.ID)
	}
	if deliver.Kind != telemetry.SpanKindConsumer || !deliver.Ended() || deliver.Attr("streams.settled") != "ack" {
		t.Errorf("deliver %+v", deliver)
	}
	if got.Headers.Get(streams.SentAtHeader) == "" || len(rec.Measurements("streams.transit.duration")) != 1 {
		t.Error("the send time did not cross the broker")
	}
}
