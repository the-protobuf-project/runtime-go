package events

import (
	"net/http"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/telemetrytest"
)

// A round is a trace: the round, each feed, the send, each delivery, and the
// delivery's context reaching the webhook in its traceparent header.
func TestARoundIsTracedToTheWebhook(t *testing.T) {
	rec := telemetrytest.New()
	sig := rec.Signals()
	f := newFixture(t, Options{Meter: sig.Meter, Tracer: sig.Tracer, Logger: sig.Logger})
	f.hook(t, "webhooks/a", nil)
	f.round(t)

	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)

	deliver := rec.Span("events.deliver")
	if deliver == nil || deliver.Kind != telemetry.SpanKindClient {
		t.Fatalf("deliver span %+v", deliver)
	}
	if deliver.Attr("http.response.status_code") != http.StatusOK || deliver.Attr("events.state") != "SUCCEEDED" {
		t.Errorf("deliver attributes code %v state %v", deliver.Attr("http.response.status_code"), deliver.Attr("events.state"))
	}
	send := spanByID(rec, deliver.Parent)
	if send == nil || send.Name != "events.send" {
		t.Fatalf("deliver's parent %+v", send)
	}
	if round := spanByID(rec, send.Parent); round == nil || round.Name != "events.round" {
		t.Errorf("send's parent %+v", round)
	}
	if rec.Span("events.dispatch_feed") == nil {
		t.Error("no dispatch_feed span")
	}

	if got := f.recv.got[0].Header.Get("traceparent"); got != deliver.ID {
		t.Errorf("the webhook got traceparent %q, want the deliver span %q", got, deliver.ID)
	}
	if out := rec.Outcomes(); out["deliver"] != "ok" || out["round"] != "ok" {
		t.Errorf("outcomes %v", out)
	}
}

// A failed attempt is a failed deliver span, its status code kept.
func TestAFailedAttemptFailsItsSpan(t *testing.T) {
	rec := telemetrytest.New()
	f := newFixture(t, Options{Tracer: rec})
	f.recv.status = http.StatusServiceUnavailable
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)

	deliver := rec.Span("events.deliver")
	if deliver == nil || deliver.Attr("http.response.status_code") != http.StatusServiceUnavailable || deliver.Attr("events.state") != "PENDING" {
		t.Fatalf("deliver span %+v", deliver)
	}
}

func spanByID(rec *telemetrytest.Recorder, id string) *telemetrytest.Span {
	for _, s := range rec.Spans() {
		if s.ID == id {
			return s
		}
	}
	return nil
}
