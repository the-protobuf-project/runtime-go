package telemetry

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Signals are the handles a runtime package reports through: metrics, traces
// and logs, the profiler that attributes samples to operations, and the
// baggage that travels with a trace. Every one is optional; a nil handle is
// the matching no-op, so the zero Signals emits nothing. The application owns the backend behind them: nothing here starts
// an exporter, installs a global or shuts a provider down.
//
// It is runtime-rs's telemetry::Telemetry under a name that does not stutter.
type Signals struct {
	Meter    Meter
	Tracer   Tracer
	Logger   Logger
	Profiler Profiler
	Baggage  Baggage
}

// Enabled reports whether any signal was supplied.
func (s Signals) Enabled() bool {
	return s.Meter != nil || s.Tracer != nil || s.Logger != nil || s.Profiler != nil || s.Baggage != nil
}

// Observer returns the observer for one runtime component ("streams",
// "database", "cache"), building its instruments once.
func (s Signals) Observer(component string) *Observer {
	o := &Observer{component: component, tracer: s.Tracer, log: s.Logger, profiler: s.Profiler, baggage: s.Baggage}
	if o.tracer == nil {
		o.tracer = NoopTracer
	}
	if o.log == nil {
		o.log = NoopLogger
	}
	if o.profiler == nil {
		o.profiler = NoopProfiler
	}
	if o.baggage == nil {
		o.baggage = NoopBaggage
	}
	if s.Meter != nil {
		m := s.Meter
		o.metrics = &operationMetrics{
			count:    m.Counter("runtime.operation.count", WithDescription("Completed operations by outcome"), WithUnit("{operation}")),
			duration: m.Histogram("runtime.operation.duration", WithDescription("Operation duration"), WithUnit("s")),
			active:   m.UpDownCounter("runtime.operation.active", WithDescription("Operations in flight"), WithUnit("{operation}")),
			read:     m.Counter("runtime.records.read", WithDescription("Records returned or delivered"), WithUnit("{record}")),
			written:  m.Counter("runtime.records.written", WithDescription("Records the backend acknowledged"), WithUnit("{record}")),
			queue:    m.UpDownCounter("runtime.ingestion.queue.depth", WithDescription("Admitted records waiting to be written"), WithUnit("{record}")),
		}
	}
	return o
}

// operationMetrics are an observer's instruments, made once.
type operationMetrics struct {
	count, read, written Counter
	duration             Histogram
	active, queue        UpDownCounter
}

// Observer starts the operations of one component. It is cheap to share and
// safe for concurrent use.
type Observer struct {
	component string
	tracer    Tracer
	log       Logger
	profiler  Profiler
	baggage   Baggage
	metrics   *operationMetrics
}

// Tracer is the tracer the observer starts spans with, for a component that
// carries trace context across a process boundary with Inject and Extract.
func (o *Observer) Tracer() Tracer { return o.tracer }

// MetricsEnabled reports whether the observer records metrics; one without a
// meter still traces and logs.
func (o *Observer) MetricsEnabled() bool { return o.metrics != nil }

// SetBaggage returns ctx carrying key=value as W3C baggage, which the tracer's
// Inject sends on to every service downstream. Without a [Signals.Baggage] it
// returns ctx unchanged.
func (o *Observer) SetBaggage(ctx context.Context, key, value string) (context.Context, error) {
	return o.baggage.Set(ctx, key, value)
}

// Baggage returns the W3C baggage ctx carries.
func (o *Observer) Baggage(ctx context.Context) map[string]string { return o.baggage.Members(ctx) }

// maxLinks bounds the producer contexts one operation links to.
const maxLinks = 32

// Start begins one operation of backend ("nats", "postgres") named operation
// ("publish", "get"), as a span named component.operation under the span in
// ctx. The returned context carries the span; pass it to the work the
// operation does. Finish the operation exactly once, on every path, on the
// goroutine that started it (its profile labels are that goroutine's); a
// defer is the usual way.
//
// While it runs, the goroutine carries profile labels component, backend and
// operation, and the baggage ctx carries is set on the span as baggage.<key>.
func (o *Observer) Start(ctx context.Context, backend, operation string, kind SpanKind, opts ...SpanOption) (context.Context, *Operation) {
	labels := Labels{"component": o.component, "backend": backend, "operation": operation}
	attrs := map[string]any{"component": o.component, "backend": backend, "operation": operation}
	baggage := o.baggage.Members(ctx)
	for k, v := range baggage {
		attrs["baggage."+k] = v
	}
	ctx, restore := o.profiler.Label(ctx, labels)
	all := make([]SpanOption, 0, len(opts)+2)
	all = append(all, WithSpanKind(kind), WithSpanAttributes(attrs))
	all = append(all, opts...)
	ctx, span := o.tracer.Start(ctx, o.component+"."+operation, all...)
	if o.metrics != nil {
		o.metrics.active.Add(ctx, 1, labels)
	}
	if o.log.Enabled(ctx, LevelDebug) {
		fields := Fields{"component": o.component, "backend": backend, "operation": operation}
		for k, v := range baggage {
			fields["baggage."+k] = v
		}
		o.log.Debug(ctx, "runtime.operation.started", fields)
	}
	return ctx, &Operation{observer: o, ctx: ctx, span: span, labels: labels, baggage: baggage, restore: restore, started: time.Now()}
}

// StartLinked is [Observer.Start] for work done on behalf of several callers,
// a batch: it links at most 32 of their contexts instead of choosing one as
// the parent.
func (o *Observer) StartLinked(ctx context.Context, backend, operation string, kind SpanKind, links []context.Context, opts ...SpanOption) (context.Context, *Operation) {
	if len(links) > maxLinks {
		links = links[:maxLinks]
	}
	return o.Start(ctx, backend, operation, kind, append(opts, WithLinks(links...))...)
}

// QueueDelta moves the depth of backend's ingest queue by delta. Every
// increment must be balanced, including for records dropped on failure.
func (o *Observer) QueueDelta(ctx context.Context, backend string, delta int64) {
	if o.metrics != nil {
		o.metrics.queue.Add(ctx, float64(delta), Labels{"component": o.component, "backend": backend})
	}
}

// Operation is one in-flight operation: its span and its share of the
// observer's metrics. Finish reports it once; later calls do nothing.
type Operation struct {
	observer      *Observer
	ctx           context.Context
	span          Span
	labels        Labels
	baggage       map[string]string
	restore       func()
	started       time.Time
	read, written int64
	once          sync.Once
}

// Context carries the operation's span.
func (op *Operation) Context() context.Context { return op.ctx }

// Span is the operation's span, for attributes and events of its own.
func (op *Operation) Span() Span { return op.span }

// ReadRecords counts n records returned or delivered.
func (op *Operation) ReadRecords(n int) { op.read += int64(n) }

// WrittenRecords counts n records the backend acknowledged; never count a
// record only admitted to a queue.
func (op *Operation) WrittenRecords(n int) { op.written += int64(n) }

// Finish ends the operation with outcome: it records the count, duration and
// record counts, ends the span (failed when outcome [Outcome.IsError]) and
// logs the completion.
func (op *Operation) Finish(outcome Outcome) {
	op.once.Do(func() { op.finish(outcome) })
}

// FinishErr ends the operation with [OutcomeOf] err.
func (op *Operation) FinishErr(err error) {
	op.once.Do(func() {
		if err != nil && OutcomeOf(err).IsError() {
			op.span.SetError(err)
		}
		op.finish(OutcomeOf(err))
	})
}

func (op *Operation) finish(outcome Outcome) {
	o := op.observer
	elapsed := time.Since(op.started).Seconds()
	if o.metrics != nil {
		o.metrics.active.Add(op.ctx, -1, op.labels)
		labels := make(Labels, len(op.labels)+1)
		for k, v := range op.labels {
			labels[k] = v
		}
		labels["outcome"] = outcome.String()
		o.metrics.count.Add(op.ctx, 1, labels)
		o.metrics.duration.Record(op.ctx, elapsed, labels)
		if op.read > 0 {
			o.metrics.read.Add(op.ctx, float64(op.read), labels)
		}
		if op.written > 0 {
			o.metrics.written.Add(op.ctx, float64(op.written), labels)
		}
	}
	op.span.SetAttribute("outcome", outcome.String())
	if outcome.IsError() {
		op.span.SetAttribute("error.type", outcome.String())
	} else {
		op.span.SetOK()
	}
	fields := Fields{
		"component": op.labels["component"], "backend": op.labels["backend"], "operation": op.labels["operation"],
		"outcome": outcome.String(), "duration_seconds": elapsed,
		"records_read": op.read, "records_written": op.written,
	}
	for k, v := range op.baggage {
		fields["baggage."+k] = v
	}
	// Errors are errors; everything else, a conflict and a miss included, is
	// a routine result and logs at debug.
	if outcome.IsError() {
		o.log.Error(op.ctx, "runtime.operation.completed", nil, fields)
	} else {
		o.log.Debug(op.ctx, "runtime.operation.completed", fields)
	}
	op.span.End()
	op.restore()
}

// OutcomeOf classifies err: nil is [OutcomeOK], a cancelled context
// [OutcomeCancelled], a passed deadline [OutcomeTimeout], an error that says
// which outcome it is (one with an Outcome() Outcome method, anywhere in its
// chain) that outcome, and anything else [OutcomeError].
func OutcomeOf(err error) Outcome {
	var classified interface{ Outcome() Outcome }
	switch {
	case err == nil:
		return OutcomeOK
	case errors.Is(err, context.Canceled):
		return OutcomeCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return OutcomeTimeout
	case errors.As(err, &classified):
		return classified.Outcome()
	default:
		return OutcomeError
	}
}
