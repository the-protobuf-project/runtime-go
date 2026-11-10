package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// rpcTelemetry instruments every call the server handles: a server span named
// grpc.<full method>, continuing the caller's trace from the call's metadata
// (W3C traceparent and tracestate), and a share of runtime.operation.count and
// runtime.operation.duration, labeled component "grpc", backend "grpc", the
// full method as the operation, and the outcome its status code maps to. A
// stream is one operation for its whole life.
//
// It reports through the module's signals ([shared.Signals]), which come from
// the application's telemetry when it adopted its SDK instance, so every span
// lands in the application's one pipeline under its service name.
type rpcTelemetry struct {
	obs *telemetry.Observer
}

func newRPCTelemetry(s telemetry.Signals) *rpcTelemetry {
	return &rpcTelemetry{obs: s.Observer("grpc")}
}

// start begins the operation of one call to fullMethod.
func (r *rpcTelemetry) start(ctx context.Context, fullMethod string, streaming bool) (context.Context, *telemetry.Operation) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		carrier := make(map[string]string, len(md))
		for k, v := range md {
			if len(v) > 0 {
				carrier[k] = v[0]
			}
		}
		ctx = r.obs.Tracer().Extract(ctx, carrier)
	}
	service, method := splitMethod(fullMethod)
	return r.obs.Start(ctx, "grpc", strings.TrimPrefix(fullMethod, "/"), telemetry.SpanKindServer,
		telemetry.WithSpanAttributes(map[string]any{
			"rpc.system": "grpc", "rpc.service": service, "rpc.method": method, "rpc.grpc.streaming": streaming,
		}))
}

// finishCall finishes op with the call's status.
func finishCall(op *telemetry.Operation, err error) {
	code := status.Code(err)
	op.Span().SetAttribute("rpc.grpc.status_code", int(code))
	outcome := codeOutcome(code)
	if outcome.IsError() {
		op.Span().SetError(err)
	}
	op.Finish(outcome)
}

func (r *rpcTelemetry) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, op := r.start(ctx, info.FullMethod, false)
	resp, err := handler(ctx, req)
	finishCall(op, err)
	return resp, err
}

func (r *rpcTelemetry) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, op := r.start(ss.Context(), info.FullMethod, true)
	err := handler(srv, &tracedStream{ServerStream: ss, ctx: ctx})
	finishCall(op, err)
	return err
}

// tracedStream hands a stream handler the context carrying its span.
type tracedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *tracedStream) Context() context.Context { return s.ctx }

// codeOutcome classifies a status code: what the caller did wrong is not a
// server failure, and an absent record or a conflict is an expected answer.
func codeOutcome(c codes.Code) telemetry.Outcome {
	switch c {
	case codes.OK:
		return telemetry.OutcomeOK
	case codes.NotFound:
		return telemetry.OutcomeNotFound
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition:
		return telemetry.OutcomeConflict
	case codes.InvalidArgument, codes.OutOfRange, codes.PermissionDenied, codes.Unauthenticated:
		return telemetry.OutcomeInvalid
	case codes.Unimplemented:
		return telemetry.OutcomeUnsupported
	case codes.DeadlineExceeded:
		return telemetry.OutcomeTimeout
	case codes.Canceled:
		return telemetry.OutcomeCancelled
	case codes.ResourceExhausted:
		return telemetry.OutcomeClosed
	default:
		return telemetry.OutcomeError
	}
}

// splitMethod splits "/pkg.Service/Method" into its service and method.
func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[:i], full[i+1:]
	}
	return "", full
}

// telemetryOptions are the server options installing rpcTelemetry first in
// both chains, so the span covers every interceptor after it.
func telemetryOptions() []grpc.ServerOption {
	r := newRPCTelemetry(shared.Signals())
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(r.unary),
		grpc.ChainStreamInterceptor(r.stream),
	}
}
