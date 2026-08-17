package mcp

import (
	"context"
	"fmt"
	"net/http"

	"github.com/the-protobuf-project/runtime-go/agents/shared"
	"google.golang.org/grpc/metadata"
)

// GRPCProgressTokenKey is the gRPC metadata key for the MCP progress token.
// When an MCP client sends progressToken in params._meta, the gateway forwards
// it via this metadata key. gRPC servers can check for its presence to decide
// whether to send MCPProgress chunks (skip overhead when client doesn't want progress).
const GRPCProgressTokenKey = "mcp-progress-token"

// HeaderMapping maps an HTTP header name to a gRPC metadata key.
// Used with MCPServerConfig.HeaderMappings to forward headers from MCP HTTP
// requests into gRPC outgoing metadata. Use DefaultHeaderMappings() for common ones.
//
// It is [shared.HeaderMapping] — the A2A runtime crosses the same HTTP-to-gRPC
// boundary and forwards headers the same way, so the mapping is defined once
// for the module.
type HeaderMapping = shared.HeaderMapping

// ForwardMetadata prepares gRPC outgoing metadata on the context, combining
// incoming gRPC metadata with any headers [HeadersMiddleware] stashed there.
// Generated ForwardTo code calls it before every gRPC client call.
//
// See [shared.ForwardMetadata] for the precedence rules.
func ForwardMetadata(ctx context.Context) context.Context {
	return shared.ForwardMetadata(ctx)
}

// HeadersMiddleware returns HTTP middleware that extracts the configured
// headers from the incoming request and stores them on its context, where
// [ForwardMetadata] picks them up.
func HeadersMiddleware(mappings []HeaderMapping, next http.Handler) http.Handler {
	return shared.HeadersMiddleware(mappings, next)
}

// DefaultHeaderMappings returns commonly forwarded header mappings:
// Authorization, X-Request-ID, and X-Trace-ID.
func DefaultHeaderMappings() []HeaderMapping {
	return shared.DefaultHeaderMappings()
}

// WithProgressToken adds the MCP progress token to outgoing gRPC metadata.
// Call this before forwarding to a gRPC backend when the MCP client sent
// progressToken in params._meta. The backend can read it via metadata.FromIncomingContext
// and key GRPCProgressTokenKey to decide whether to send progress chunks.
func WithProgressToken(ctx context.Context, token any) context.Context {
	if token == nil {
		return ctx
	}
	// MCP progressToken can be string or int; serialize for metadata.
	str := fmt.Sprint(token)
	md, _ := metadata.FromOutgoingContext(ctx)
	if md == nil {
		md = metadata.MD{}
	}
	md = md.Copy()
	md.Set(GRPCProgressTokenKey, str)
	return metadata.NewOutgoingContext(ctx, md)
}

// WithIncomingProgressToken adds the MCP progress token as incoming gRPC
// metadata on the context. Use this for in-process (Register) streaming
// handlers so that gRPC server methods that check
// metadata.FromIncomingContext see the token and emit progress chunks.
func WithIncomingProgressToken(ctx context.Context, token any) context.Context {
	if token == nil {
		return ctx
	}
	str := fmt.Sprint(token)
	md := metadata.New(map[string]string{GRPCProgressTokenKey: str})
	return metadata.NewIncomingContext(ctx, md)
}
