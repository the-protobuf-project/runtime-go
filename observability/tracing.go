package observability

import "context"

// Tracer runs a function inside a span. The closure form is deliberate: it
// makes the span's lifetime the function's lifetime, so a caller cannot leak
// an unended span by forgetting to call End.
type Tracer interface {
	Trace(ctx context.Context, name string, fn func(context.Context, Span) error) error
}

// Span is one unit of work inside a trace. The span is ended, and its status
// set from the returned error, by the Tracer that created it.
type Span interface {
	// SetAttributes attaches structured detail to the span.
	SetAttributes(Fields)

	// AddEvent marks a point in time within the span.
	AddEvent(name string)

	// RecordError attaches an error to the span. A nil error is ignored.
	RecordError(err error)
}

// NoopTracer still runs fn — tracing being unconfigured must not skip the work
// being traced — but records nothing.
var NoopTracer Tracer = noopTracer{}

type noopTracer struct{}

func (noopTracer) Trace(ctx context.Context, _ string, fn func(context.Context, Span) error) error {
	return fn(ctx, noopSpan{})
}

type noopSpan struct{}

func (noopSpan) SetAttributes(Fields) {}
func (noopSpan) AddEvent(string)      {}
func (noopSpan) RecordError(error)    {}
