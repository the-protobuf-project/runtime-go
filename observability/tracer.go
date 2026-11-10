package observability

import (
	"context"

	sdk "github.com/the-protobuf-project/telemetry/telemetry-go"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Tracer returns the [telemetry.Tracer] for this client: the SDK's tracing,
// which records spans when the client was built with tracing on and records
// nothing otherwise. It is safe on a zero Client, returning
// [telemetry.NoopTracer].
func (c *Client) Tracer() telemetry.Tracer {
	if c == nil || c.otel == nil || c.otel.Tracing == nil {
		return telemetry.NoopTracer
	}
	return tracer{c.otel}
}

// tracer adapts the SDK's Tracing to [telemetry.Tracer].
//
// The SDK's span already has the contract's methods, so spans pass through
// as they are; only the start options are translated, because the contract's
// cannot name the SDK's.
type tracer struct {
	otel *sdk.Telemetry
}

func (t tracer) Start(ctx context.Context, name string, opts ...telemetry.SpanOption) (context.Context, telemetry.Span) {
	cfg := telemetry.NewSpanConfig(opts...)
	start := []sdk.SpanOption{sdk.WithSpanKind(spanKind(cfg.Kind))}
	if !cfg.Start.IsZero() {
		start = append(start, sdk.WithStartTime(cfg.Start))
	}
	if len(cfg.Attributes) > 0 {
		start = append(start, sdk.WithSpanAttributes(cfg.Attributes))
	}
	if len(cfg.Links) > 0 {
		start = append(start, sdk.WithLinks(cfg.Links...))
	}
	return t.otel.Tracing.StartSpan(ctx, name, start...)
}

func (t tracer) Inject(ctx context.Context, headers map[string]string) {
	t.otel.Tracing.Inject(ctx, headers)
}

func (t tracer) Extract(ctx context.Context, headers map[string]string) context.Context {
	return t.otel.Tracing.Extract(ctx, headers)
}

func spanKind(k telemetry.SpanKind) sdk.SpanKind {
	switch k {
	case telemetry.SpanKindServer:
		return sdk.SpanKindServer
	case telemetry.SpanKindClient:
		return sdk.SpanKindClient
	case telemetry.SpanKindProducer:
		return sdk.SpanKindProducer
	case telemetry.SpanKindConsumer:
		return sdk.SpanKindConsumer
	default:
		return sdk.SpanKindInternal
	}
}
