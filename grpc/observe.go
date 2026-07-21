package grpc

import (
	"context"

	"github.com/the-protobuf-project/opentelementry/opentelementry-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rpcMetrics is recorded once per traced call. The service and method names
// ride as metric attributes (see Observer.record) so the counters slice per
// endpoint; the opentelementry instance itself carries the application
// identity. Errors counts only server faults (see IsServerError), never
// expected client/business rejections like NotFound or FailedPrecondition.
type rpcMetrics struct {
	Requests int64 `opentelementry:"metric:counter:rpc.requests"`
	Errors   int64 `opentelementry:"metric:counter:rpc.errors"`
}

// Observer wraps RPC handler bodies with a trace span, request/error
// counters, and outcome logging. Construct one per application with the
// application's opentelementry client and share it across handlers — it is
// stateless beyond the client handle.
type Observer struct {
	o *opentelementry.Opentelementry
}

// NewObserver returns an Observer emitting through o.
func NewObserver(o *opentelementry.Opentelementry) *Observer {
	return &Observer{o: o}
}

// Traced runs fn inside a span named "<service>/<method>", records the
// request/error counters, and logs the outcome. The RPC result is returned to
// the caller through a closure variable; Traced only carries the error so the
// tracing layer can set the span status. Expected client/business rejections
// are logged at debug, not error, and excluded from the error counter.
func (ob *Observer) Traced(ctx context.Context, service, method string, fn func(context.Context) error) error {
	return ob.o.Tracing.Trace(ctx, service+"/"+method, nil, func(ctx context.Context, _ *opentelementry.Span) error {
		err := fn(ctx)
		ob.record(service, method, err)
		switch {
		case err == nil:
			ob.o.Logger.Debug(method + " ok")
		case IsServerError(err):
			_ = ob.o.Logger.Error(method+" failed", map[string]any{"error": err.Error()})
		default:
			// Expected client/business outcome (NotFound, FailedPrecondition,
			// etc.) — returned to the caller but not a service fault.
			ob.o.Logger.Debug(method+" rejected", map[string]any{"error": err.Error()})
		}
		return err
	})
}

// record emits the per-call counters tagged with the service and method names.
// Only server faults increment Errors.
func (ob *Observer) record(service, method string, err error) {
	m := rpcMetrics{Requests: 1}
	if IsServerError(err) {
		m.Errors = 1
	}
	_ = ob.o.Metrics.Record(m, opentelementry.WithAttributes(
		opentelementry.StringAttribute("service", service),
		opentelementry.StringAttribute("method", method),
	))
}

// IsServerError reports whether err is a server-side fault — the codes that
// mean the service itself misbehaved — as opposed to an expected
// client/business outcome (NotFound, InvalidArgument, FailedPrecondition,
// ResourceExhausted, Aborted, AlreadyExists, ...). A non-status error reads as
// Unknown, which counts.
func IsServerError(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
