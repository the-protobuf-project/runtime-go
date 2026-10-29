package shared

import (
	"context"
	"net/http"
	"slices"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestIncomingHeaders_SetsMappedKeys(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer abc")
	h.Set("X-Other", "not mapped")

	ctx := IncomingHeaders(context.Background(), DefaultHeaderMappings(), h)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		t.Fatal("no incoming metadata")
	}
	if got := md.Get("authorization"); !slices.Equal(got, []string{"Bearer abc"}) {
		t.Errorf("authorization = %v", got)
	}
	if got := md.Get("x-other"); len(got) != 0 {
		t.Errorf("unmapped header lifted: %v", got)
	}
}

// A mapped key already on ctx is replaced by the request's own value, or
// dropped when the request has none: a stale credential must never pass as
// this caller's.
func TestIncomingHeaders_ReplacesStaleValues(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer stale", "x-request-id", "old", "keep", "me"))

	h := http.Header{}
	h.Set("X-Request-Id", "new")
	md, _ := metadata.FromIncomingContext(IncomingHeaders(ctx, DefaultHeaderMappings(), h))

	if got := md.Get("authorization"); len(got) != 0 {
		t.Errorf("stale authorization survived: %v", got)
	}
	if got := md.Get("x-request-id"); !slices.Equal(got, []string{"new"}) {
		t.Errorf("x-request-id = %v", got)
	}
	if got := md.Get("keep"); !slices.Equal(got, []string{"me"}) {
		t.Errorf("unmapped incoming key lost: %v", got)
	}
}

func TestIncomingHeaders_NoMappingsOrHeaders(t *testing.T) {
	ctx := context.Background()
	if got := IncomingHeaders(ctx, nil, http.Header{"Authorization": {"x"}}); got != ctx {
		t.Error("no mappings should return ctx unchanged")
	}
	if got := IncomingHeaders(ctx, DefaultHeaderMappings(), nil); got != ctx {
		t.Error("nothing to lift should return ctx unchanged")
	}
	reserved := []HeaderMapping{{HTTPHeader: "X-Timeout", GRPCKey: "grpc-timeout"}}
	if _, ok := metadata.FromIncomingContext(IncomingHeaders(ctx, reserved, http.Header{"X-Timeout": {"1S"}})); ok {
		t.Error("a grpc- key was written")
	}
}
