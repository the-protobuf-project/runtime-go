package shared

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

// IncomingHeaders returns ctx with the mapped headers of h as incoming gRPC
// metadata, the place a gRPC server's unary interceptor reads a caller's
// credentials from. It is the in-process counterpart of [HeadersMiddleware]:
// that one prepares headers for a gRPC call made onward over the network, this
// one hands them to an interceptor chain run in the same process, such as the
// one an MCP tool call dispatches through.
//
// Every mapped key is first removed from whatever incoming metadata ctx
// already carries and then set from h alone, so a value an earlier request on
// the same session left behind never reaches the interceptor as this caller's.
// Mappings onto the grpc- prefix, which the transport owns, are ignored. With
// no mappings ctx is returned unchanged.
func IncomingHeaders(ctx context.Context, mappings []HeaderMapping, h http.Header) context.Context {
	if len(mappings) == 0 {
		return ctx
	}
	existing, had := metadata.FromIncomingContext(ctx)
	md := existing.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	for _, m := range mappings {
		key := strings.ToLower(m.GRPCKey)
		if key == "" || strings.HasPrefix(key, "grpc-") {
			continue
		}
		md.Delete(key)
		if h == nil {
			continue
		}
		if vals := h.Values(m.HTTPHeader); len(vals) > 0 {
			md.Set(key, vals...)
		}
	}
	if !had && len(md) == 0 {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, md)
}
