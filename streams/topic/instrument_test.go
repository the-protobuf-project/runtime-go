package topic_test

// streams.Instrument, over the in-memory provider: the provider's spans, the
// capabilities it keeps, and how a topic sits on top of it.

import (
	"errors"
	"testing"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/topic"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

func instrumented(t *testing.T, durable, batch bool) (*memory, streams.Streams, *tracer, *meter) {
	t.Helper()
	tr, mt := &tracer{}, &meter{seen: map[string][]telemetry.Labels{}}
	m := newMemory(durable, batch)
	return m, streams.Instrument(m, telemetry.Signals{Tracer: tr, Meter: mt}, "memory"), tr, mt
}

func TestTheZeroSignalsLeaveTheProviderAlone(t *testing.T) {
	m := newMemory(false, false)
	if streams.Instrument(m, telemetry.Signals{}, "memory") != streams.Streams(m) {
		t.Fatal("Instrument without signals should return the provider itself")
	}
}

func TestAnInstrumentedPublishIsTheParentOfItsReceive(t *testing.T) {
	m, p, tr, mt := instrumented(t, false, false)
	mgr, err := topic.Bind(t.Context(), p, declaration())
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := mgr.Subscribe(t.Context(), "order.placed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Publish(t.Context(), "order.placed", order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	msg := receive(t, msgs)

	publish, rcv := tr.named("streams.publish"), tr.named("streams.receive")
	if publish == nil || rcv == nil || tr.named("streams.subscribe") == nil || tr.named("streams.bind") == nil {
		t.Fatalf("spans %v", tr.spans)
	}
	if publish.kind != telemetry.SpanKindProducer || rcv.kind != telemetry.SpanKindConsumer || rcv.parent != publish.id {
		t.Errorf("publish %v receive %v parent %q", publish.kind, rcv.kind, rcv.parent)
	}
	if h := m.calls()[0].opts.Headers; h.Get("traceparent") != publish.id || h.Get(streams.SentAtHeader) == "" {
		t.Errorf("published headers %v", h)
	}
	if !msg.Traced() || msg.Context().Value(spanKey{}) != rcv.id {
		t.Error("the message does not carry its receive span")
	}
	ops := mt.operations()
	if ops["publish"] != "ok" || ops["receive"] != "ok" || ops["subscribe"] != "ok" {
		t.Errorf("operations %v", ops)
	}
	if mt.count("streams.transit.duration") != 1 {
		t.Errorf("transit recorded %d times", mt.count("streams.transit.duration"))
	}
}

func TestAnInstrumentedProviderKeepsItsCapabilities(t *testing.T) {
	_, p, tr, _ := instrumented(t, true, true)
	mgr, err := topic.Bind(t.Context(), p, declaration(), streams.CapabilityDurable, streams.CapabilityBatch)
	if err != nil {
		t.Fatalf("bind requiring durable and batch: %v", err)
	}
	b, err := streams.AsBatch(mgr)
	if err != nil {
		t.Fatal(err)
	}
	if _, perr := b.PublishBatch(t.Context(), "order.placed", []any{order("a", "x"), order("b", "x")}); perr != nil {
		t.Fatal(perr)
	}
	if s := tr.named("streams.publish_batch"); s == nil || s.attrs["messaging.batch.message_count"] != 2 {
		t.Errorf("publish_batch span %+v", s)
	}

	_, plain, _, _ := instrumented(t, false, false)
	pm, err := topic.Bind(t.Context(), plain, declaration())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streams.AsDurable(pm); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("a plain provider gained durability through the wrapper: %v", err)
	}
}

func TestATopicOverAnInstrumentedProviderTracesEachStepOnce(t *testing.T) {
	_, p, tr, _ := instrumented(t, true, false)
	tel := topic.NewTelemetry(telemetry.Signals{Tracer: tr}, "memory")
	mgr, err := topic.Bind(t.Context(), p, declaration())
	if err != nil {
		t.Fatal(err)
	}
	orders := topic.New(mgr, placed()).WithTelemetry(tel)

	deliveries, err := orders.Consumer().Consume(t.Context(), "billing")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	d := receive(t, deliveries)

	send, encode, publish := tr.named("streams.send"), tr.named("streams.encode"), tr.named("streams.publish")
	deliver, decode := tr.named("streams.deliver"), tr.named("streams.decode")
	if send == nil || encode == nil || publish == nil || deliver == nil || decode == nil {
		t.Fatalf("spans %v", tr.spans)
	}
	if tr.named("streams.process") != nil {
		t.Error("the topic started a process span for a message the provider traced")
	}
	if encode.parent != send.id || publish.parent != send.id || deliver.parent != publish.id || decode.parent != deliver.id {
		t.Errorf("parents: encode %q publish %q deliver %q decode %q", encode.parent, publish.parent, deliver.parent, decode.parent)
	}

	tr.mu.Lock()
	early := deliver.ended
	tr.mu.Unlock()
	if early {
		t.Fatal("deliver ended before the delivery was settled")
	}
	if err := d.Ack(d.Context()); err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if !deliver.ended || deliver.attrs["streams.settled"] != "ack" {
		t.Errorf("deliver after ack: ended %v attrs %v", deliver.ended, deliver.attrs)
	}
}

func TestAMissingStreamIsNotAFailure(t *testing.T) {
	_, p, tr, mt := instrumented(t, false, false)
	if _, err := p.Get(t.Context(), "nope"); !errors.Is(err, streams.ErrNotFound) {
		t.Fatalf("Get: %v", err)
	}
	if s := tr.named("streams.get"); s == nil || s.failed {
		t.Errorf("get span %+v", s)
	}
	if mt.operations()["get"] != "not_found" {
		t.Errorf("operations %v", mt.operations())
	}
}
