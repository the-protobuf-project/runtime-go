package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// A call is a server span continuing the caller's trace from its metadata.
func TestACallContinuesTheCallersTrace(t *testing.T) {
	rec := recorder.New()
	r := newRPCTelemetry(rec.Signals())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", "caller-span"))
	info := &grpc.UnaryServerInfo{FullMethod: "/shop.v1.ShopService/GetShelf"}

	var inner string
	_, err := r.unary(ctx, nil, info, func(ctx context.Context, _ any) (any, error) {
		inner = recorder.SpanID(ctx)
		return nil, status.Error(codes.NotFound, "no shelf")
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err %v", err)
	}

	s := rec.Span("grpc.shop.v1.ShopService/GetShelf")
	if s == nil || s.Kind != telemetry.SpanKindServer || s.Parent != "caller-span" {
		t.Fatalf("span %+v", s)
	}
	if inner != s.ID {
		t.Error("the handler does not run under the call's span")
	}
	if s.Attr("rpc.service") != "shop.v1.ShopService" || s.Attr("rpc.method") != "GetShelf" || s.Failed() {
		t.Errorf("attributes service %v method %v failed %v", s.Attr("rpc.service"), s.Attr("rpc.method"), s.Failed())
	}
	if got := rec.Outcomes()["shop.v1.ShopService/GetShelf"]; got != "not_found" {
		t.Errorf("outcome %q", got)
	}
}

func TestAServerFaultFailsTheSpan(t *testing.T) {
	rec := recorder.New()
	r := newRPCTelemetry(rec.Signals())
	info := &grpc.UnaryServerInfo{FullMethod: "/x.Y/Z"}
	_, _ = r.unary(context.Background(), nil, info, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.Internal, "boom")
	})
	if s := rec.Span("grpc.x.Y/Z"); s == nil || !s.Failed() {
		t.Fatalf("span %+v", s)
	}
}

func TestCodeOutcomes(t *testing.T) {
	for c, want := range map[codes.Code]telemetry.Outcome{
		codes.OK: telemetry.OutcomeOK, codes.NotFound: telemetry.OutcomeNotFound,
		codes.AlreadyExists: telemetry.OutcomeConflict, codes.InvalidArgument: telemetry.OutcomeInvalid,
		codes.Unimplemented: telemetry.OutcomeUnsupported, codes.DeadlineExceeded: telemetry.OutcomeTimeout,
		codes.Canceled: telemetry.OutcomeCancelled, codes.Internal: telemetry.OutcomeError,
	} {
		if got := codeOutcome(c); got != want {
			t.Errorf("%v -> %v, want %v", c, got, want)
		}
	}
}
