package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/metadata"
)

// The header-forwarding behavior itself is tested in agents/shared, which owns
// it. What this covers is the seam: mcp's re-exports have to reach that same
// implementation, or generated ForwardTo code silently forwards nothing.
func TestForwardMetadata_ReExportsShared(t *testing.T) {
	mappings := []HeaderMapping{
		{HTTPHeader: "Authorization", GRPCKey: "authorization"},
		{HTTPHeader: "X-Tenant-Id", GRPCKey: "x-tenant-id"},
	}

	var forwarded context.Context
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = ForwardMetadata(r.Context())
	})

	req := httptest.NewRequestWithContext(t.Context(), "POST", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("X-Tenant-Id", "tenant-42")
	HeadersMiddleware(mappings, inner).ServeHTTP(httptest.NewRecorder(), req)

	outMD, ok := metadata.FromOutgoingContext(forwarded)
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

func TestDefaultHeaderMappings_ReExportsShared(t *testing.T) {
	got := DefaultHeaderMappings()
	if len(got) != 3 {
		t.Fatalf("got %d mappings, want 3", len(got))
	}
	if got[0].HTTPHeader != "Authorization" || got[0].GRPCKey != "authorization" {
		t.Errorf("first mapping: got %+v", got[0])
	}
}

func TestWithProgressToken(t *testing.T) {
	// A progress token can arrive as a string or a number; both have to survive
	// as metadata, which is string-valued.
	for _, tc := range []struct {
		name  string
		token any
		want  string
	}{
		{"string", "tok-1", "tok-1"},
		{"int", 42, "42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithProgressToken(context.Background(), tc.token)
			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Fatal("expected outgoing metadata")
			}
			if got := md.Get(GRPCProgressTokenKey); len(got) != 1 || got[0] != tc.want {
				t.Errorf("got %v, want [%s]", got, tc.want)
			}
		})
	}
}

func TestWithProgressToken_NilIsUntouched(t *testing.T) {
	// No token means the client did not ask for progress, and the backend
	// decides on the key's presence — so it must not appear at all.
	ctx := context.Background()
	if got := WithProgressToken(ctx, nil); got != ctx {
		t.Error("a nil token should leave the context alone")
	}
	if got := WithIncomingProgressToken(ctx, nil); got != ctx {
		t.Error("a nil token should leave the context alone")
	}
}

func TestWithProgressToken_PreservesExistingOutgoing(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer secret"))

	md, ok := metadata.FromOutgoingContext(WithProgressToken(ctx, "tok-1"))
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer secret" {
		t.Errorf("authorization was lost: got %v", got)
	}
	if got := md.Get(GRPCProgressTokenKey); len(got) != 1 || got[0] != "tok-1" {
		t.Errorf("progress token: got %v", got)
	}
}

func TestWithIncomingProgressToken(t *testing.T) {
	// In-process handlers read the token off incoming metadata, not outgoing.
	ctx := WithIncomingProgressToken(context.Background(), "tok-9")
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		t.Fatal("expected incoming metadata")
	}
	if got := md.Get(GRPCProgressTokenKey); len(got) != 1 || got[0] != "tok-9" {
		t.Errorf("got %v, want [tok-9]", got)
	}
}
