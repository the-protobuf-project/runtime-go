package shared

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

// HeaderMapping names one HTTP header to read and the gRPC metadata key to
// write it as. Mappings are explicit rather than a blanket copy because the two
// namespaces are not the same shape: gRPC reserves the grpc- prefix, keys are
// lowercase there and case-insensitive here, and a proxy that forwarded
// everything would leak hop-by-hop headers a backend has no business seeing.
type HeaderMapping struct {
	HTTPHeader string // HTTP header name to read (case-insensitive)
	GRPCKey    string // gRPC metadata key to write (use lowercase)
}

// httpHeadersKeyType is unexported so nothing outside this package can plant
// values under it — the only writer is [HeadersMiddleware].
type httpHeadersKeyType struct{}

var httpHeadersKey = httpHeadersKeyType{}

// HeadersMiddleware returns HTTP middleware that lifts the mapped headers off
// the request and stashes them on its context, where [ForwardMetadata] finds
// them later.
//
// It is two steps rather than one because the read and the write happen in
// different places: the headers exist while the HTTP request is being served,
// and the gRPC call that needs them is made further in, by a handler that never
// sees an *http.Request. With no mappings it returns next unwrapped, so the
// unconfigured case costs nothing.
func HeadersMiddleware(mappings []HeaderMapping, next http.Handler) http.Handler {
	if len(mappings) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pairs := make(map[string]string, len(mappings))
		for _, m := range mappings {
			if v := r.Header.Get(m.HTTPHeader); v != "" {
				pairs[m.GRPCKey] = v
			}
		}
		if len(pairs) > 0 {
			ctx := context.WithValue(r.Context(), httpHeadersKey, pairs)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// ForwardMetadata prepares gRPC outgoing metadata on ctx from two sources:
//
//  1. Incoming gRPC metadata, for the gRPC-to-gRPC case — every key except the
//     grpc- prefixed ones, which the transport owns.
//  2. Headers stashed by [HeadersMiddleware], for the HTTP-to-gRPC case.
//
// Where both carry the same key the HTTP one wins: it came from the client that
// actually made this call, while the incoming metadata may be a hop's leftovers.
//
// A context with neither is returned unchanged rather than carrying an empty
// metadata.MD, so a caller can tell "nothing to forward" from "forwarded
// nothing".
func ForwardMetadata(ctx context.Context) context.Context {
	md := metadata.MD{}

	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		for k, vals := range incoming {
			key := strings.ToLower(k)
			if strings.HasPrefix(key, "grpc-") {
				continue // reserved by gRPC
			}
			md[key] = append(md[key], vals...)
		}
	}

	if pairs, ok := ctx.Value(httpHeadersKey).(map[string]string); ok {
		for k, v := range pairs {
			md.Set(strings.ToLower(k), v) // overwrites duplicates
		}
	}

	if len(md) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// DefaultHeaderMappings returns the three headers worth forwarding by default:
// the credential the backend authenticates on, and the two ids that make a
// request traceable across the hop.
func DefaultHeaderMappings() []HeaderMapping {
	return []HeaderMapping{
		{HTTPHeader: "Authorization", GRPCKey: "authorization"},
		{HTTPHeader: "X-Request-Id", GRPCKey: "x-request-id"},
		{HTTPHeader: "X-Trace-Id", GRPCKey: "x-trace-id"},
	}
}
