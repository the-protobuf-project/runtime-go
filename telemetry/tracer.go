package telemetry

import (
	"context"
	"strings"
	"time"
)

// SpanKind says what side of an interaction a span records, as OpenTelemetry
// defines it.
type SpanKind int

const (
	// SpanKindInternal is work inside one process. It is the zero value.
	SpanKindInternal SpanKind = iota

	// SpanKindServer is a request this process serves.
	SpanKindServer

	// SpanKindClient is a request this process makes and waits on.
	SpanKindClient

	// SpanKindProducer hands a message to a broker for someone else to receive.
	SpanKindProducer

	// SpanKindConsumer receives a message a producer sent.
	SpanKindConsumer
)

// String names the kind as OpenTelemetry spells it.
func (k SpanKind) String() string {
	switch k {
	case SpanKindServer:
		return "server"
	case SpanKindClient:
		return "client"
	case SpanKindProducer:
		return "producer"
	case SpanKindConsumer:
		return "consumer"
	default:
		return "internal"
	}
}

// Tracer is the backend-agnostic tracing contract, beside [Meter] and
// [Logger].
//
// A span's context travels in ctx: Start returns a context carrying the new
// span, and a span started from that context is its child. Inject and Extract
// carry the same context across a process boundary in string headers, a
// message's or a request's, so a trace continues in another program, possibly
// another language.
type Tracer interface {
	// Start begins a span named name under the span in ctx, if any, and returns
	// a context carrying it. End the span when the work is done.
	Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)

	// Inject writes the trace context in ctx into headers (W3C traceparent and
	// tracestate, or whatever the backend propagates).
	Inject(ctx context.Context, headers map[string]string)

	// Extract returns ctx carrying the remote trace context headers hold, for
	// a span started from it to be the remote span's child. headers may be nil.
	Extract(ctx context.Context, headers map[string]string) context.Context
}

// Span is one timed piece of work in a trace.
//
// Its methods match the span of runtime-go's telemetry SDK, so the SDK's spans
// satisfy it as they are. A Span is safe to end twice; the second End does
// nothing.
type Span interface {
	// End finishes the span now.
	End()

	// SetError records err and marks the span failed. A nil err does nothing.
	SetError(err error)

	// SetOK marks the span succeeded.
	SetOK()

	// AddEvent records a moment within the span.
	AddEvent(name string)

	// SetAttribute sets one attribute. Values are strings, booleans, integers
	// or floats; anything else is recorded as its fmt %v form.
	SetAttribute(key string, value any)

	// SetAttributes sets several attributes; see [Span.SetAttribute].
	SetAttributes(attrs map[string]any)
}

// SpanConfig is what [SpanOption]s set; implementations read it through
// [NewSpanConfig].
type SpanConfig struct {
	Kind       SpanKind
	Start      time.Time
	Attributes map[string]any
	// Links are contexts whose spans this one follows from without being their
	// child: the producers of a batch, each carried in its own context.
	Links []context.Context
}

// SpanOption configures one span at Start.
type SpanOption func(*SpanConfig)

// WithSpanKind sets the span's kind; internal when not given.
func WithSpanKind(kind SpanKind) SpanOption {
	return func(c *SpanConfig) { c.Kind = kind }
}

// WithStartTime starts the span at t rather than now, for work that began
// before the span could be (a message's receipt, decoded after it arrived).
func WithStartTime(t time.Time) SpanOption {
	return func(c *SpanConfig) { c.Start = t }
}

// WithSpanAttributes sets attributes the span starts with.
func WithSpanAttributes(attrs map[string]any) SpanOption {
	return func(c *SpanConfig) {
		if c.Attributes == nil {
			c.Attributes = make(map[string]any, len(attrs))
		}
		for k, v := range attrs {
			c.Attributes[k] = v
		}
	}
}

// WithLinks links the span to the span each context carries.
func WithLinks(ctxs ...context.Context) SpanOption {
	return func(c *SpanConfig) { c.Links = append(c.Links, ctxs...) }
}

// NewSpanConfig folds opts into one SpanConfig.
func NewSpanConfig(opts ...SpanOption) SpanConfig {
	var c SpanConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// NoopTracer is a Tracer whose spans record nothing and whose Inject and
// Extract carry nothing. It is the safe default when no backend has been wired
// in, as [NoopMeter] and [NoopLogger] are.
var NoopTracer Tracer = noopTracer{}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...SpanOption) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) Inject(context.Context, map[string]string) {}
func (noopTracer) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

type noopSpan struct{}

func (noopSpan) End()                         {}
func (noopSpan) SetError(error)               {}
func (noopSpan) SetOK()                       {}
func (noopSpan) AddEvent(string)              {}
func (noopSpan) SetAttribute(string, any)     {}
func (noopSpan) SetAttributes(map[string]any) {}

// TraceID returns the W3C trace id of the span ctx carries, as t would send
// it in a traceparent header, or "" when there is none: the id to quote in an
// error message so an operator can find the request's trace.
func TraceID(ctx context.Context, t Tracer) string {
	if t == nil {
		return ""
	}
	h := map[string]string{}
	t.Inject(ctx, h)
	// version "-" trace-id "-" parent-id "-" flags, as W3C Trace Context
	// defines traceparent.
	parts := strings.Split(h["traceparent"], "-")
	if len(parts) != 4 || len(parts[1]) != 32 || parts[1] == strings.Repeat("0", 32) {
		return ""
	}
	return parts[1]
}
