package telemetrytest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Recorder keeps everything reported through it. Safe for concurrent use.
type Recorder struct {
	mu           sync.Mutex
	spans        []*Span
	measurements []Measurement
	logs         []Log
	bound        telemetry.Fields
	root         *Recorder
}

// New returns an empty recorder.
func New() *Recorder { return &Recorder{} }

// Signals is the recorder as all three handles.
func (r *Recorder) Signals() telemetry.Signals {
	return telemetry.Signals{Meter: r, Tracer: r, Logger: r}
}

func (r *Recorder) owner() *Recorder {
	if r.root != nil {
		return r.root
	}
	return r
}

// --- tracing -------------------------------------------------------------------

type spanKey struct{}

// Span is one recorded span.
type Span struct {
	Name   string
	ID     string
	Parent string // the id of the span it was started under, or extracted from
	Kind   telemetry.SpanKind
	Start  time.Time
	Links  int

	mu     sync.Mutex
	attrs  map[string]any
	events []string
	err    error
	ok     bool
	ended  int
}

// Attr returns one attribute.
func (s *Span) Attr(key string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attrs[key]
}

// Ended reports whether End was called.
func (s *Span) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended > 0
}

// Failed reports whether SetError was called with an error.
func (s *Span) Failed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err != nil
}

// Err is the error SetError recorded.
func (s *Span) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Events are the events added, in order.
func (s *Span) Events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func (s *Span) End() {
	s.mu.Lock()
	s.ended++
	s.mu.Unlock()
}

func (s *Span) SetError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *Span) SetOK() {
	s.mu.Lock()
	s.ok = true
	s.mu.Unlock()
}

func (s *Span) AddEvent(name string) {
	s.mu.Lock()
	s.events = append(s.events, name)
	s.mu.Unlock()
}

func (s *Span) SetAttribute(key string, value any) {
	s.mu.Lock()
	s.attrs[key] = value
	s.mu.Unlock()
}

func (s *Span) SetAttributes(attrs map[string]any) {
	s.mu.Lock()
	for k, v := range attrs {
		s.attrs[k] = v
	}
	s.mu.Unlock()
}

// Start records a span under the one in ctx.
func (r *Recorder) Start(ctx context.Context, name string, opts ...telemetry.SpanOption) (context.Context, telemetry.Span) {
	o := r.owner()
	cfg := telemetry.NewSpanConfig(opts...)
	parent, _ := ctx.Value(spanKey{}).(string)
	o.mu.Lock()
	s := &Span{
		Name: name, ID: fmt.Sprintf("span-%d", len(o.spans)+1), Parent: parent,
		Kind: cfg.Kind, Start: cfg.Start, Links: len(cfg.Links), attrs: map[string]any{},
	}
	for k, v := range cfg.Attributes {
		s.attrs[k] = v
	}
	o.spans = append(o.spans, s)
	o.mu.Unlock()
	return context.WithValue(ctx, spanKey{}, s.ID), s
}

// Inject writes the id of the span in ctx as headers["traceparent"].
func (r *Recorder) Inject(ctx context.Context, headers map[string]string) {
	if id, ok := ctx.Value(spanKey{}).(string); ok && headers != nil {
		headers["traceparent"] = id
	}
}

// Extract makes headers["traceparent"] the parent of spans started from the
// returned context.
func (r *Recorder) Extract(ctx context.Context, headers map[string]string) context.Context {
	if id := headers["traceparent"]; id != "" {
		return context.WithValue(ctx, spanKey{}, id)
	}
	return ctx
}

// SpanID is the id of the span ctx carries, or "".
func SpanID(ctx context.Context) string {
	id, _ := ctx.Value(spanKey{}).(string)
	return id
}

// Spans are every span started, in order.
func (r *Recorder) Spans() []*Span {
	o := r.owner()
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*Span(nil), o.spans...)
}

// Span is the first span named name, or nil.
func (r *Recorder) Span(name string) *Span {
	for _, s := range r.Spans() {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// Named are every span named name.
func (r *Recorder) Named(name string) []*Span {
	var out []*Span
	for _, s := range r.Spans() {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// --- metrics -------------------------------------------------------------------

// Measurement is one recorded value.
type Measurement struct {
	Name   string
	Value  float64
	Labels telemetry.Labels
}

type instrument struct {
	r    *Recorder
	name string
}

func (i instrument) Add(_ context.Context, v float64, l telemetry.Labels)    { i.rec(v, l) }
func (i instrument) Set(_ context.Context, v float64, l telemetry.Labels)    { i.rec(v, l) }
func (i instrument) Record(_ context.Context, v float64, l telemetry.Labels) { i.rec(v, l) }

func (i instrument) rec(v float64, l telemetry.Labels) {
	o := i.r.owner()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.measurements = append(o.measurements, Measurement{Name: i.name, Value: v, Labels: l})
}

func (r *Recorder) Counter(name string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	return instrument{r, name}
}

func (r *Recorder) UpDownCounter(name string, _ ...telemetry.InstrumentOption) telemetry.UpDownCounter {
	return instrument{r, name}
}

func (r *Recorder) Gauge(name string, _ ...telemetry.InstrumentOption) telemetry.Gauge {
	return instrument{r, name}
}

func (r *Recorder) Histogram(name string, _ ...telemetry.InstrumentOption) telemetry.Histogram {
	return instrument{r, name}
}

// Measurements are every value recorded under name, in order.
func (r *Recorder) Measurements(name string) []Measurement {
	o := r.owner()
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []Measurement
	for _, m := range o.measurements {
		if m.Name == name {
			out = append(out, m)
		}
	}
	return out
}

// Sum adds every value recorded under name.
func (r *Recorder) Sum(name string) float64 {
	var total float64
	for _, m := range r.Measurements(name) {
		total += m.Value
	}
	return total
}

// Outcomes maps each operation counted in runtime.operation.count to the
// outcome of its last completion.
func (r *Recorder) Outcomes() map[string]string {
	out := map[string]string{}
	for _, m := range r.Measurements("runtime.operation.count") {
		out[m.Labels["operation"]] = m.Labels["outcome"]
	}
	return out
}

// --- logs ----------------------------------------------------------------------

// Log is one recorded log record.
type Log struct {
	Level  telemetry.Level
	Msg    string
	Err    error
	Fields telemetry.Fields
	SpanID string // the span the record's context carried
}

func (r *Recorder) log(ctx context.Context, level telemetry.Level, msg string, err error, fields telemetry.Fields) {
	all := telemetry.Fields{}
	for k, v := range r.bound {
		all[k] = v
	}
	for k, v := range fields {
		all[k] = v
	}
	o := r.owner()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.logs = append(o.logs, Log{Level: level, Msg: msg, Err: err, Fields: all, SpanID: SpanID(ctx)})
}

func (r *Recorder) Debug(ctx context.Context, msg string, f telemetry.Fields) {
	r.log(ctx, telemetry.LevelDebug, msg, nil, f)
}

func (r *Recorder) Info(ctx context.Context, msg string, f telemetry.Fields) {
	r.log(ctx, telemetry.LevelInfo, msg, nil, f)
}

func (r *Recorder) Warn(ctx context.Context, msg string, f telemetry.Fields) {
	r.log(ctx, telemetry.LevelWarn, msg, nil, f)
}

func (r *Recorder) Error(ctx context.Context, msg string, err error, f telemetry.Fields) {
	r.log(ctx, telemetry.LevelError, msg, err, f)
}

// Enabled is true at every level, so guarded log lines are recorded too.
func (r *Recorder) Enabled(context.Context, telemetry.Level) bool { return true }

// With returns a logger adding fields to every record, recording into r.
func (r *Recorder) With(fields telemetry.Fields) telemetry.Logger {
	bound := telemetry.Fields{}
	for k, v := range r.bound {
		bound[k] = v
	}
	for k, v := range fields {
		bound[k] = v
	}
	return &Recorder{bound: bound, root: r.owner()}
}

// Logs are every record logged, in order.
func (r *Recorder) Logs() []Log {
	o := r.owner()
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]Log(nil), o.logs...)
}

var (
	_ telemetry.Meter  = (*Recorder)(nil)
	_ telemetry.Tracer = (*Recorder)(nil)
	_ telemetry.Logger = (*Recorder)(nil)
	_ telemetry.Span   = (*Span)(nil)
)
