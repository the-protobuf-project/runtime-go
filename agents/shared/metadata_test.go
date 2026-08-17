package shared

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestForwardMetadata_IncomingGRPC(t *testing.T) {
	// Simulate incoming gRPC metadata.
	md := metadata.Pairs(
		"authorization", "Bearer token123",
		"x-request-id", "req-abc",
		"grpc-timeout", "5s", // should be filtered
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = ForwardMetadata(ctx)

	outMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := outMD.Get("authorization"); len(got) == 0 || got[0] != "Bearer token123" {
		t.Errorf("authorization: got %v", got)
	}
	if got := outMD.Get("x-request-id"); len(got) == 0 || got[0] != "req-abc" {
		t.Errorf("x-request-id: got %v", got)
	}
	if got := outMD.Get("grpc-timeout"); len(got) != 0 {
		t.Errorf("grpc-timeout should be filtered, got %v", got)
	}
}

func TestForwardMetadata_HTTPHeaders(t *testing.T) {
	mappings := []HeaderMapping{
		{HTTPHeader: "Authorization", GRPCKey: "authorization"},
		{HTTPHeader: "X-Tenant-Id", GRPCKey: "x-tenant-id"},
	}

	// Simulate HTTP request with headers flowing through middleware.
	var capturedCtx context.Context
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCtx = r.Context()
	})
	handler := HeadersMiddleware(mappings, inner)

	req := httptest.NewRequestWithContext(t.Context(), "POST", "/invoke", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("X-Tenant-Id", "tenant-42")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if capturedCtx == nil {
		t.Fatal("handler was not called")
	}

	// Now ForwardMetadata should pick up the HTTP headers.
	ctx := ForwardMetadata(capturedCtx)
	outMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := outMD.Get("authorization"); len(got) == 0 || got[0] != "Bearer secret" {
		t.Errorf("authorization: got %v", got)
	}
	if got := outMD.Get("x-tenant-id"); len(got) == 0 || got[0] != "tenant-42" {
		t.Errorf("x-tenant-id: got %v", got)
	}
}

func TestForwardMetadata_MergesBoth(t *testing.T) {
	// Incoming gRPC metadata, as a proxied call would carry.
	grpcMD := metadata.Pairs("x-request-id", "from-grpc")

	// The HTTP headers arrive the only way they can — through the middleware,
	// which is the sole writer of the context key ForwardMetadata reads. The
	// x-request-id mapping collides with the gRPC value on purpose.
	mappings := []HeaderMapping{
		{HTTPHeader: "Authorization", GRPCKey: "authorization"},
		{HTTPHeader: "X-Request-Id", GRPCKey: "x-request-id"},
	}

	var forwarded context.Context
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = ForwardMetadata(metadata.NewIncomingContext(r.Context(), grpcMD))
	})

	req := httptest.NewRequestWithContext(t.Context(), "POST", "/invoke", nil)
	req.Header.Set("Authorization", "Bearer http-token")
	req.Header.Set("X-Request-Id", "from-http")
	HeadersMiddleware(mappings, inner).ServeHTTP(httptest.NewRecorder(), req)

	outMD, ok := metadata.FromOutgoingContext(forwarded)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := outMD.Get("authorization"); len(got) == 0 || got[0] != "Bearer http-token" {
		t.Errorf("authorization: got %v", got)
	}
	// The header the client actually sent wins over the hop's leftover.
	if got := outMD.Get("x-request-id"); len(got) != 1 || got[0] != "from-http" {
		t.Errorf("x-request-id: got %v, want [from-http]", got)
	}
}

func TestHeadersMiddleware_NoMappingsForwardsNothing(t *testing.T) {
	var forwarded context.Context
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = ForwardMetadata(r.Context())
	})

	req := httptest.NewRequestWithContext(t.Context(), "POST", "/invoke", nil)
	req.Header.Set("Authorization", "Bearer secret")
	HeadersMiddleware(nil, inner).ServeHTTP(httptest.NewRecorder(), req)

	// Unmapped is unforwarded: a header nobody asked for must not reach the
	// backend just because it was on the request.
	if md, ok := metadata.FromOutgoingContext(forwarded); ok {
		t.Errorf("expected no outgoing metadata, got %v", md)
	}
}

func TestForwardMetadata_EmptyContextUnchanged(t *testing.T) {
	ctx := context.Background()
	if got := ForwardMetadata(ctx); got != ctx {
		t.Error("a context with nothing to forward should be returned as-is")
	}
}
