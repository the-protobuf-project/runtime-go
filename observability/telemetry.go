package observability

import (
	"context"

	"github.com/the-protobuf-project/telemetry/telemetry-go"
)

// sdkTracer adapts the SDK's tracing pipeline to [Tracer].
//
// The SDK's Trace takes an optional data struct it reflects over for
// `telemetry:"..."` tags. This passes nil: runtime-go modules attach detail
// through [Span.SetAttributes] instead, which is explicit and does not depend
// on a struct's tags matching what the SDK expects.
type sdkTracer struct{ otel *telemetry.Telemetry }

func (s sdkTracer) Trace(ctx context.Context, name string, fn func(context.Context, Span) error) error {
	return s.otel.Tracing.Trace(ctx, name, nil, func(ctx context.Context, sp *telemetry.Span) error {
		return fn(ctx, sdkSpan{sp: sp})
	})
}

// sdkSpan adapts the SDK's span to [Span]. End and status are handled by the
// SDK's Trace, so neither is exposed here.
type sdkSpan struct{ sp *telemetry.Span }

func (s sdkSpan) SetAttributes(f Fields) {
	if len(f) == 0 {
		return
	}
	s.sp.SetAttributes(map[string]interface{}(f))
}

func (s sdkSpan) AddEvent(name string) { s.sp.AddEvent(name) }

func (s sdkSpan) RecordError(err error) {
	if err != nil {
		s.sp.SetError(err)
	}
}
