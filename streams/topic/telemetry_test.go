package topic_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/the-protobuf-project/runtime-go/streams/topic"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// --- a tracer and meter that record --------------------------------------------

type spanKey struct{}

type span struct {
	name, id, parent string
	kind             telemetry.SpanKind
	attrs            map[string]any
	ended            bool
	failed           bool
	mu               *sync.Mutex
}

func (s *span) End()               { s.mu.Lock(); s.ended = true; s.mu.Unlock() }
func (s *span) SetError(err error) { s.mu.Lock(); s.failed = err != nil; s.mu.Unlock() }
func (s *span) SetOK()             {}
func (s *span) AddEvent(string)    {}
func (s *span) SetAttribute(k string, v any) {
	s.mu.Lock()
	s.attrs[k] = v
	s.mu.Unlock()
}
func (s *span) SetAttributes(a map[string]any) {
	for k, v := range a {
		s.SetAttribute(k, v)
	}
}

type tracer struct {
	mu    sync.Mutex
	spans []*span
}

func (tr *tracer) Start(ctx context.Context, name string, opts ...telemetry.SpanOption) (context.Context, telemetry.Span) {
	cfg := telemetry.NewSpanConfig(opts...)
	parent, _ := ctx.Value(spanKey{}).(string)
	tr.mu.Lock()
	s := &span{name: name, id: fmt.Sprintf("s%d", len(tr.spans)+1), parent: parent, kind: cfg.Kind, attrs: map[string]any{}, mu: &tr.mu}
	for k, v := range cfg.Attributes {
		s.attrs[k] = v
	}
	tr.spans = append(tr.spans, s)
	tr.mu.Unlock()
	return context.WithValue(ctx, spanKey{}, s.id), s
}

func (tr *tracer) Inject(ctx context.Context, h map[string]string) {
	if id, ok := ctx.Value(spanKey{}).(string); ok {
		h["traceparent"] = id
	}
}

func (tr *tracer) Extract(ctx context.Context, h map[string]string) context.Context {
	if id := h["traceparent"]; id != "" {
		return context.WithValue(ctx, spanKey{}, id)
	}
	return ctx
}

func (tr *tracer) named(name string) *span {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, s := range tr.spans {
		if s.name == name {
			return s
		}
	}
	return nil
}

type meter struct {
	mu   sync.Mutex
	seen map[string][]telemetry.Labels
}

type instrument struct {
	m    *meter
	name string
}

func (i instrument) Add(_ context.Context, _ float64, l telemetry.Labels)    { i.rec(l) }
func (i instrument) Set(_ context.Context, _ float64, l telemetry.Labels)    { i.rec(l) }
func (i instrument) Record(_ context.Context, _ float64, l telemetry.Labels) { i.rec(l) }
func (i instrument) rec(l telemetry.Labels) {
	i.m.mu.Lock()
	defer i.m.mu.Unlock()
	i.m.seen[i.name] = append(i.m.seen[i.name], l)
}

func (m *meter) Counter(n string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	return instrument{m, n}
}
func (m *meter) UpDownCounter(n string, _ ...telemetry.InstrumentOption) telemetry.UpDownCounter {
	return instrument{m, n}
}
func (m *meter) Gauge(n string, _ ...telemetry.InstrumentOption) telemetry.Gauge {
	return instrument{m, n}
}
func (m *meter) Histogram(n string, _ ...telemetry.InstrumentOption) telemetry.Histogram {
	return instrument{m, n}
}

func (m *meter) operations() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]string{}
	for _, l := range m.seen["runtime.operation.count"] {
		out[l["operation"]] = l["outcome"]
	}
	return out
}

func (m *meter) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.seen[name])
}

// --- tests ---------------------------------------------------------------------

func TestOneMessageIsOneTraceFromSendToAck(t *testing.T) {
	tr, mt := &tracer{}, &meter{seen: map[string][]telemetry.Labels{}}
	tel := topic.NewTelemetry(telemetry.Signals{Tracer: tr, Meter: mt}, "memory")
	m := newMemory(true, false)
	orders := topic.New(bind(t, m), placed()).WithTelemetry(tel)

	deliveries, err := orders.Consumer().Consume(t.Context(), "billing")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	d := receive(t, deliveries)

	send, encode := tr.named("streams.send"), tr.named("streams.encode")
	process, decode := tr.named("streams.process"), tr.named("streams.decode")
	if send == nil || encode == nil || process == nil || decode == nil {
		t.Fatalf("spans %v", tr.spans)
	}
	if encode.parent != send.id || process.parent != send.id || decode.parent != process.id {
		t.Errorf("parents: encode %q process %q decode %q, send %q", encode.parent, process.parent, decode.parent, send.id)
	}
	if send.kind != telemetry.SpanKindProducer || process.kind != telemetry.SpanKindConsumer {
		t.Errorf("kinds %v %v", send.kind, process.kind)
	}
	if send.attrs["messaging.destination.name"] != "order.placed" || send.attrs["messaging.message.id"] == nil {
		t.Errorf("send attributes %v", send.attrs)
	}
	headers := m.calls()[0].opts.Headers
	if headers.Get("traceparent") != send.id || headers.Get(topic.SentAtHeader) == "" {
		t.Errorf("published headers %v", headers)
	}

	tr.mu.Lock()
	open := process.ended
	tr.mu.Unlock()
	if open {
		t.Fatal("process ended before the delivery was settled")
	}
	if err := d.Ack(d.Context()); err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	settled := process.ended && process.attrs["streams.settled"] == "ack"
	tr.mu.Unlock()
	if !settled {
		t.Errorf("process after ack: ended %v attrs %v", process.ended, process.attrs)
	}

	ops := mt.operations()
	for _, op := range []string{"send", "encode", "decode", "process"} {
		if ops[op] != "ok" {
			t.Errorf("runtime.operation.count for %s: %q", op, ops[op])
		}
	}
	if mt.count("streams.transit.duration") != 1 || mt.count("streams.message.size") != 2 {
		t.Errorf("transit %d size %d", mt.count("streams.transit.duration"), mt.count("streams.message.size"))
	}
}

func TestAnUndecodableMessageIsAFailedDecode(t *testing.T) {
	tr, mt := &tracer{}, &meter{seen: map[string][]telemetry.Labels{}}
	tel := topic.NewTelemetry(telemetry.Signals{Tracer: tr, Meter: mt}, "memory")
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed()).WithTelemetry(tel)

	msgs, err := orders.Subscriber().Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	m.inject("order.placed", "json", []byte("not json"))
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	if r := receive(t, msgs); r.Message().OrderID != "o-1" || r.Context() == nil {
		t.Errorf("received %+v", r.Message())
	}

	tr.mu.Lock()
	defer tr.mu.Unlock()
	var failed int
	for _, s := range tr.spans {
		if s.name == "streams.decode" && s.failed {
			failed++
		}
	}
	if failed != 1 {
		t.Errorf("failed decodes %d, want 1", failed)
	}
}

func TestATopicWithoutTelemetryReportsNothing(t *testing.T) {
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed())
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	if h := m.calls()[0].opts.Headers; h.Get("traceparent") != "" {
		t.Errorf("a no-op tracer injected %v", h)
	}
}
