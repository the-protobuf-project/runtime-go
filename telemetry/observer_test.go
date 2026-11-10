package telemetry_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// --- a tracer that records -----------------------------------------------------

type spanKey struct{}

type recordedSpan struct {
	name    string
	config  telemetry.SpanConfig
	parent  *recordedSpan
	attrs   map[string]any
	events  []string
	err     error
	ok      bool
	ended   int
	tracer  *recorder
	traceID string
}

func (s *recordedSpan) End()               { s.tracer.mu.Lock(); s.ended++; s.tracer.mu.Unlock() }
func (s *recordedSpan) SetError(err error) { s.err = err }
func (s *recordedSpan) SetOK()             { s.ok = true }
func (s *recordedSpan) AddEvent(n string)  { s.events = append(s.events, n) }
func (s *recordedSpan) SetAttribute(k string, v any) {
	s.attrs[k] = v
}
func (s *recordedSpan) SetAttributes(a map[string]any) {
	for k, v := range a {
		s.attrs[k] = v
	}
}

type recorder struct {
	mu    sync.Mutex
	spans []*recordedSpan
	next  int
}

func (r *recorder) Start(ctx context.Context, name string, opts ...telemetry.SpanOption) (context.Context, telemetry.Span) {
	cfg := telemetry.NewSpanConfig(opts...)
	parent, _ := ctx.Value(spanKey{}).(*recordedSpan)
	r.mu.Lock()
	r.next++
	traceID := fmt.Sprintf("trace-%d", r.next)
	if parent != nil {
		traceID = parent.traceID
	}
	s := &recordedSpan{name: name, config: cfg, parent: parent, attrs: map[string]any{}, tracer: r, traceID: traceID}
	for k, v := range cfg.Attributes {
		s.attrs[k] = v
	}
	r.spans = append(r.spans, s)
	r.mu.Unlock()
	return context.WithValue(ctx, spanKey{}, s), s
}

func (r *recorder) Inject(ctx context.Context, h map[string]string) {
	if s, ok := ctx.Value(spanKey{}).(*recordedSpan); ok {
		h["traceparent"] = s.traceID
	}
}

func (r *recorder) Extract(ctx context.Context, h map[string]string) context.Context {
	if id := h["traceparent"]; id != "" {
		return context.WithValue(ctx, spanKey{}, &recordedSpan{traceID: id, attrs: map[string]any{}, tracer: r})
	}
	return ctx
}

// --- a meter that records ------------------------------------------------------

type measurement struct {
	name   string
	value  float64
	labels telemetry.Labels
}

type meter struct {
	mu   sync.Mutex
	seen []measurement
}

type instrument struct {
	m    *meter
	name string
}

func (i instrument) Add(_ context.Context, v float64, l telemetry.Labels)    { i.rec(v, l) }
func (i instrument) Set(_ context.Context, v float64, l telemetry.Labels)    { i.rec(v, l) }
func (i instrument) Record(_ context.Context, v float64, l telemetry.Labels) { i.rec(v, l) }
func (i instrument) rec(v float64, l telemetry.Labels) {
	i.m.mu.Lock()
	defer i.m.mu.Unlock()
	i.m.seen = append(i.m.seen, measurement{i.name, v, l})
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

func (m *meter) named(name string) []measurement {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []measurement
	for _, s := range m.seen {
		if s.name == name {
			out = append(out, s)
		}
	}
	return out
}

// --- tests ---------------------------------------------------------------------

func TestTheZeroSignalsEmitNothing(t *testing.T) {
	var s telemetry.Signals
	if s.Enabled() {
		t.Fatal("zero Signals reports enabled")
	}
	ctx, op := s.Observer("streams").Start(t.Context(), "nats", "send", telemetry.SpanKindProducer)
	if ctx != t.Context() {
		t.Error("a no-op tracer should hand back the caller's context")
	}
	op.Finish(telemetry.OutcomeOK)
}

func TestAnOperationIsASpanAndItsMetrics(t *testing.T) {
	tr, m := &recorder{}, &meter{}
	obs := telemetry.Signals{Tracer: tr, Meter: m}.Observer("streams")

	ctx, op := obs.Start(t.Context(), "nats", "send", telemetry.SpanKindProducer)
	_, child := obs.Start(ctx, "nats", "encode", telemetry.SpanKindInternal)
	child.Finish(telemetry.OutcomeOK)
	op.WrittenRecords(1)
	op.Finish(telemetry.OutcomeOK)
	op.Finish(telemetry.OutcomeError) // ignored

	if len(tr.spans) != 2 {
		t.Fatalf("spans %d", len(tr.spans))
	}
	send, encode := tr.spans[0], tr.spans[1]
	if send.name != "streams.send" || send.config.Kind != telemetry.SpanKindProducer || encode.parent != send {
		t.Errorf("send %q kind %v; encode parent %v", send.name, send.config.Kind, encode.parent)
	}
	if send.ended != 1 || send.attrs["outcome"] != "ok" || !send.ok || send.attrs["backend"] != "nats" {
		t.Errorf("send ended %d attrs %v ok %v", send.ended, send.attrs, send.ok)
	}

	counts := m.named("runtime.operation.count")
	if len(counts) != 2 || counts[1].labels["operation"] != "send" || counts[1].labels["outcome"] != "ok" {
		t.Errorf("counts %+v", counts)
	}
	if d := m.named("runtime.operation.duration"); len(d) != 2 {
		t.Errorf("durations %+v", d)
	}
	var active float64
	for _, a := range m.named("runtime.operation.active") {
		active += a.value
	}
	if active != 0 {
		t.Errorf("active does not balance: %v", active)
	}
	if w := m.named("runtime.records.written"); len(w) != 1 || w[0].value != 1 {
		t.Errorf("written %+v", w)
	}
}

func TestFinishErrClassifiesTheError(t *testing.T) {
	tr := &recorder{}
	obs := telemetry.Signals{Tracer: tr}.Observer("streams")

	cases := []struct {
		err  error
		want telemetry.Outcome
	}{
		{nil, telemetry.OutcomeOK},
		{context.Canceled, telemetry.OutcomeCancelled},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), telemetry.OutcomeTimeout},
		{classified{telemetry.OutcomeUnsupported}, telemetry.OutcomeUnsupported},
		{errors.New("boom"), telemetry.OutcomeError},
	}
	for _, c := range cases {
		_, op := obs.Start(t.Context(), "nats", "send", telemetry.SpanKindProducer)
		op.FinishErr(c.err)
		s := tr.spans[len(tr.spans)-1]
		if s.attrs["outcome"] != c.want.String() {
			t.Errorf("%v: outcome %v, want %v", c.err, s.attrs["outcome"], c.want)
		}
		if c.want.IsError() != (s.err != nil) {
			t.Errorf("%v: span error %v", c.err, s.err)
		}
	}
}

type classified struct{ o telemetry.Outcome }

func (c classified) Error() string              { return c.o.String() }
func (c classified) Outcome() telemetry.Outcome { return c.o }

func TestStartLinkedKeepsAtMost32Links(t *testing.T) {
	tr := &recorder{}
	obs := telemetry.Signals{Tracer: tr}.Observer("streams")
	links := make([]context.Context, 40)
	for i := range links {
		links[i] = t.Context()
	}
	_, op := obs.StartLinked(t.Context(), "nats", "send", telemetry.SpanKindProducer, links)
	op.Finish(telemetry.OutcomeOK)
	if n := len(tr.spans[0].config.Links); n != 32 {
		t.Errorf("links %d, want 32", n)
	}
}

func TestOutcomeLabelsMatchRuntimeRs(t *testing.T) {
	want := []string{"ok", "not_found", "conflict", "unsupported", "invalid", "timeout", "cancelled", "panic", "closed", "error"}
	for i, w := range want {
		if got := telemetry.Outcome(i).String(); got != w {
			t.Errorf("Outcome(%d) = %q, want %q", i, got, w)
		}
	}
}

type traceparent string

func (traceparent) Start(ctx context.Context, _ string, _ ...telemetry.SpanOption) (context.Context, telemetry.Span) {
	return ctx, nil
}
func (p traceparent) Inject(_ context.Context, h map[string]string)                  { h["traceparent"] = string(p) }
func (traceparent) Extract(ctx context.Context, _ map[string]string) context.Context { return ctx }

func TestTraceIDReadsTheTraceparent(t *testing.T) {
	for header, want := range map[string]string{
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01": "4bf92f3577b34da6a3ce929d0e0e4736",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01": "",
		"garbage": "",
		"":        "",
	} {
		if got := telemetry.TraceID(t.Context(), traceparent(header)); got != want {
			t.Errorf("%q -> %q, want %q", header, got, want)
		}
	}
	if telemetry.TraceID(t.Context(), nil) != "" || telemetry.TraceID(t.Context(), telemetry.NoopTracer) != "" {
		t.Error("a nil or no-op tracer has no trace id")
	}
}
