package mcp

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/the-protobuf-project/runtime-go/agents/shared"
)

// IncomingHeadersMiddleware returns MCP receiving middleware that lifts the
// mapped HTTP headers of each request into the incoming gRPC metadata of the
// context the request is handled in.
//
// The go-sdk keeps each HTTP request's headers on the request itself
// (RequestExtra.Header) and hands handlers a context that carries none of
// them, so without this a unary interceptor run through [InvokeUnary] — auth
// above all — never sees the Authorization header the client sent. Reading the
// headers per request rather than per HTTP handler is what makes it right for
// stateful sessions too, where the context belongs to the session and outlives
// any one POST.
//
// [NewMCPServer] installs it whenever cfg.HeaderMappings is set, which covers a
// service at its own path and the combined endpoint alike. Requests that did
// not arrive over HTTP (stdio) carry no headers, and every mapped key is
// cleared for them, so nothing stale is ever passed off as the caller's.
func IncomingHeadersMiddleware(mappings []HeaderMapping) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		if len(mappings) == 0 {
			return next
		}
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			var header http.Header
			if req != nil {
				if extra := req.GetExtra(); extra != nil {
					header = extra.Header
				}
			}
			return next(shared.IncomingHeaders(ctx, mappings, header), method, req)
		}
	}
}
