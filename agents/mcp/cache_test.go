package mcp

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func run(t *testing.T, hints CacheHints, method string, req mcp.Request, res mcp.Result) mcp.Result {
	t.Helper()
	handler := cacheMiddleware(hints)(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return res, nil
	})
	got, err := handler(context.Background(), method, req)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return got
}

// Every list result the 2026-07-28 revision made cacheable must carry the
// service's hint; missing one silently opts that method out of caching.
func TestCacheHintReachesEveryListResult(t *testing.T) {
	hints := CacheHints{List: &CacheHint{TTLMs: 5000, Scope: CacheScopePublic}}

	for _, tc := range []struct {
		method string
		res    mcp.Result
		get    func(mcp.Result) mcp.Cacheable
	}{
		{"tools/list", &mcp.ListToolsResult{}, func(r mcp.Result) mcp.Cacheable { return r.(*mcp.ListToolsResult).Cacheable }},
		{"prompts/list", &mcp.ListPromptsResult{}, func(r mcp.Result) mcp.Cacheable { return r.(*mcp.ListPromptsResult).Cacheable }},
		{"resources/list", &mcp.ListResourcesResult{}, func(r mcp.Result) mcp.Cacheable { return r.(*mcp.ListResourcesResult).Cacheable }},
		{"resources/templates/list", &mcp.ListResourceTemplatesResult{}, func(r mcp.Result) mcp.Cacheable {
			return r.(*mcp.ListResourceTemplatesResult).Cacheable
		}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			c := tc.get(run(t, hints, tc.method, nil, tc.res))
			if c.TTLMs != 5000 {
				t.Errorf("TTLMs = %d, want 5000", c.TTLMs)
			}
			if c.CacheScope != CacheScopePublic {
				t.Errorf("CacheScope = %q, want %q", c.CacheScope, CacheScopePublic)
			}
		})
	}
}

// A resource may be more volatile than its siblings, so its own hint wins over
// the service default.
func TestPerResourceHintOverridesTheServiceDefault(t *testing.T) {
	hints := CacheHints{
		List:      &CacheHint{TTLMs: 5000, Scope: CacheScopePublic},
		Resources: map[string]CacheHint{"gallery://live": {TTLMs: 100, Scope: CacheScopePrivate}},
	}

	req := &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "gallery://live"}}
	c := run(t, hints, "resources/read", req, &mcp.ReadResourceResult{}).(*mcp.ReadResourceResult).Cacheable
	if c.TTLMs != 100 || c.CacheScope != CacheScopePrivate {
		t.Errorf("override not applied: ttl=%d scope=%q", c.TTLMs, c.CacheScope)
	}

	// A resource with no override still gets the service default.
	req = &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "gallery://stable"}}
	c = run(t, hints, "resources/read", req, &mcp.ReadResourceResult{}).(*mcp.ReadResourceResult).Cacheable
	if c.TTLMs != 5000 || c.CacheScope != CacheScopePublic {
		t.Errorf("service default not applied: ttl=%d scope=%q", c.TTLMs, c.CacheScope)
	}
}

// The SDK stamps CacheScope="public" onto every result before middleware runs,
// so a declared hint has to overwrite rather than fill a blank — otherwise a
// resource declared private stays public and leaks through a shared cache.
func TestDeclaredHintOverwritesTheSDKDefault(t *testing.T) {
	hints := CacheHints{List: &CacheHint{TTLMs: 0, Scope: CacheScopePrivate}}
	res := &mcp.ListToolsResult{Cacheable: mcp.Cacheable{TTLMs: 5000, CacheScope: CacheScopePublic}}

	c := run(t, hints, "tools/list", nil, res).(*mcp.ListToolsResult).Cacheable
	if c.CacheScope != CacheScopePrivate {
		t.Errorf("CacheScope = %q, want private — the SDK default must not win", c.CacheScope)
	}
	if c.TTLMs != 0 {
		t.Errorf("TTLMs = %d, want 0 — an opt-out must clear an inherited TTL", c.TTLMs)
	}
}

// An unstated scope leaves whatever the SDK defaulted, since the proto said
// nothing about sharing.
func TestUnstatedScopeLeavesTheDefault(t *testing.T) {
	hints := CacheHints{List: &CacheHint{TTLMs: 100}}
	res := &mcp.ListToolsResult{Cacheable: mcp.Cacheable{CacheScope: CacheScopePublic}}

	c := run(t, hints, "tools/list", nil, res).(*mcp.ListToolsResult).Cacheable
	if c.CacheScope != CacheScopePublic {
		t.Errorf("CacheScope = %q, want the untouched default", c.CacheScope)
	}
}

// Installing with nothing declared must not add a middleware pass at all.
func TestApplyCacheHintsIsANoOpWhenNothingIsDeclared(t *testing.T) {
	s := NewMCPServer(&MCPServerConfig{Name: "cache-test", Version: "1.0.0"})
	ApplyCacheHints(s, CacheHints{})
}
