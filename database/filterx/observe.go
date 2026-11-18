package filterx

import (
	"context"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// SignalsObserver adapts runtime-go telemetry signals to the [Observer] the
// list engines report through: one span per query, failed when the query
// fails, and a debug log per rejected filter or order input. Without a tracer
// or a logger that half is a no-op.
//
// The engines start with [SignalsObserver] over the process's default signals
// ([telemetry.Default], which runtime-go/observability's Adopt sets), so a
// program that adopted its SDK sees every list query without wiring anything;
// [GormEngine.Observe] and [HasuraEngine.Observe] replace it.
func SignalsObserver(s telemetry.Signals) Observer {
	if s.Tracer == nil && s.Logger == nil {
		return NopObserver{}
	}
	tracer, log := s.Tracer, s.Logger
	if tracer == nil {
		tracer = telemetry.NoopTracer
	}
	if log == nil {
		log = telemetry.NoopLogger
	}
	return signalsObserver{tracer: tracer, log: log}
}

type signalsObserver struct {
	tracer telemetry.Tracer
	log    telemetry.Logger
}

// Span runs fn under a client span named name.
func (o signalsObserver) Span(ctx context.Context, name string, fn func(context.Context) error) error {
	ctx, span := o.tracer.Start(ctx, name, telemetry.WithSpanKind(telemetry.SpanKindClient))
	defer span.End()
	err := fn(ctx)
	if err != nil {
		span.SetError(err)
	} else {
		span.SetOK()
	}
	return err
}

// Debug logs a non-fatal engine event.
func (o signalsObserver) Debug(msg string, kv map[string]any) {
	o.log.Debug(context.Background(), msg, telemetry.Fields(kv))
}

// defaultObserver is what an engine reports through until Observe replaces it.
func defaultObserver() Observer { return SignalsObserver(telemetry.Default()) }
