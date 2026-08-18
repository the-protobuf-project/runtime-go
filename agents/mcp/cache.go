package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// CacheHint is how long a client may treat a response as fresh, and who may
// share it. Protocol revision 2026-07-28 made list and read results cacheable.
//
// TTLMs of 0 means immediately stale, which is also what a client assumes when
// no hint is sent — so a zero hint is indistinguishable from none.
type CacheHint struct {
	TTLMs int
	// Scope is "public" or "private". Empty means unstated, which clients
	// default to public; say "private" explicitly for anything user-specific,
	// because a shared cache holding a per-user response leaks it.
	Scope string
}

// Cache scopes, matching HTTP Cache-Control.
const (
	CacheScopePublic  = "public"
	CacheScopePrivate = "private"
)

// CacheHints carries the hint for a service's list results plus any per-resource
// overrides for resources/read, keyed by URI.
type CacheHints struct {
	List      *CacheHint
	Resources map[string]CacheHint
}

// cacheMiddleware stamps cache hints onto the results the SDK builds.
//
// The SDK assembles list results internally from the registered tools, prompts
// and resources, so a server has no field to set — middleware is the supported
// seam.
//
// A declared hint is applied unconditionally rather than only filling blanks.
// The SDK defaults CacheScope to "public" before this runs, so there is no
// "unset" state left to detect: a private resource would silently stay public,
// which publishes a per-user response to any shared cache.
func cacheMiddleware(hints CacheHints) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			res, err := next(ctx, method, req)
			if err != nil || res == nil {
				return res, err
			}
			switch r := res.(type) {
			case *mcp.ListToolsResult:
				applyCacheHint(&r.Cacheable, hints.List)
			case *mcp.ListPromptsResult:
				applyCacheHint(&r.Cacheable, hints.List)
			case *mcp.ListResourcesResult:
				applyCacheHint(&r.Cacheable, hints.List)
			case *mcp.ListResourceTemplatesResult:
				applyCacheHint(&r.Cacheable, hints.List)
			case *mcp.ReadResourceResult:
				// A read is hinted per resource; fall back to the service
				// default so a declared TTL still covers resources that do not
				// override it.
				hint := hints.List
				if rr, ok := req.(*mcp.ReadResourceRequest); ok && rr.Params != nil {
					if override, found := hints.Resources[rr.Params.URI]; found {
						hint = &override
					}
				}
				applyCacheHint(&r.Cacheable, hint)
			}
			return res, nil
		}
	}
}

// applyCacheHint writes a result's cache fields from the declared hint.
func applyCacheHint(c *mcp.Cacheable, hint *CacheHint) {
	if hint == nil {
		return
	}
	c.TTLMs = hint.TTLMs
	if hint.Scope != "" {
		c.CacheScope = hint.Scope
	}
}

// ApplyCacheHints installs the cache-hint middleware on a server. Generated
// registration code calls it when the proto declares any hint.
func ApplyCacheHints(s *mcp.Server, hints CacheHints) {
	if hints.List == nil && len(hints.Resources) == 0 {
		return
	}
	s.AddReceivingMiddleware(cacheMiddleware(hints))
}
