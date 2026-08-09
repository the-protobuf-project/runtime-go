package resp

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/core"
)

// New returns a cache backed by a client you own.
//
// The backend it reports is the client's, so a Dragonfly client yields a cache
// that says dragonfly in its errors without this package knowing the difference.
//
// Nothing is reachable yet: no database has been chosen, so there is nothing to
// read or write. That is [Provider.SetDatabase].
func New(client *Client, cfg cache.Config) cache.Provider {
	return &provider{client: client, cfg: cfg}
}

// provider is the RESP implementation of cache.Provider. It holds no strategy
// code — see [core.Build].
type provider struct {
	client *Client
	cfg    cache.Config
}

var _ cache.Provider = (*provider)(nil)

func (p *provider) Backend() string {
	if p.client == nil {
		return "resp"
	}
	return p.client.backend
}

// SetDatabase selects a named database and returns the strategies over it.
//
// The name qualifies the keys and nothing else: the connection stays on
// whichever index it was built for, so no client is derived, no SELECT is
// issued, and [cache.DB.Close] only drains background work. That is what lets
// the same call work on a cluster client, where database 0 is the only one
// there is.
func (p *provider) SetDatabase(ctx context.Context, name string) (*cache.DB, error) {
	if p.client == nil || p.client.inner == nil {
		return nil, fmt.Errorf("resp: no client; build one with NewClient")
	}
	backend := p.client.backend
	if err := core.CheckNamespace(name); err != nil {
		return nil, fmt.Errorf("%s: %w", backend, err)
	}

	// Reach the server before handing back strategies over it, for the same
	// reason SelectIndex does: a refused AUTH should surface on this line rather
	// than on some Get three layers into a request handler.
	if perr := p.client.inner.Ping(ctx).Err(); perr != nil {
		return nil, fmt.Errorf("%s: database %q is not reachable: %w", backend, name, perr)
	}

	return core.Build(primitives{client: p.client.inner, backend: backend}, core.Spec{
		Prefix:       p.cfg.Prefix,
		Namespace:    name,
		Database:     p.client.currentIndex(),
		EmbedDB:      false, // the name already separates these keys
		DefaultTTL:   p.cfg.DefaultTTL,
		DefaultStale: p.cfg.DefaultStale,
		Concurrency:  p.cfg.Concurrency,
		RequireTTL:   p.cfg.RequireTTL,
		// Nothing was derived to reach this database — the namespace is a
		// string — so there is nothing here to release.
		Release: nil,
	}), nil
}

// SelectIndex selects a database by index and returns the strategies over it.
//
// When the index asked for is the one the client already has — the common case,
// and the reason to configure a client at all — nothing is derived and
// [cache.DB.Close] only drains background work. Otherwise a client is derived
// for that index and the DB owns it.
func (p *provider) SelectIndex(ctx context.Context, index int) (*cache.DB, error) {
	if p.client == nil || p.client.inner == nil {
		return nil, fmt.Errorf("resp: no client; build one with NewClient")
	}
	backend := p.client.backend
	if index < 0 {
		return nil, fmt.Errorf("%s: database index cannot be negative, got %d", backend, index)
	}

	inner, release, err := p.client.selectDB(index)
	if err != nil {
		return nil, err
	}

	// Reach the database before handing back strategies over it, so a refused
	// AUTH or a database the server was not configured with surfaces on this
	// line instead of on some Get three layers into a request handler.
	if perr := inner.Ping(ctx).Err(); perr != nil {
		if release != nil {
			_ = release()
		}
		return nil, fmt.Errorf("%s: database %d is not reachable: %w", backend, index, perr)
	}

	return core.Build(primitives{client: inner, backend: backend}, core.Spec{
		Prefix:       p.cfg.Prefix,
		Database:     index,
		EmbedDB:      false, // the index is the connection's; keys need not repeat it
		DefaultTTL:   p.cfg.DefaultTTL,
		DefaultStale: p.cfg.DefaultStale,
		Concurrency:  p.cfg.Concurrency,
		RequireTTL:   p.cfg.RequireTTL,
		Release:      release,
	}), nil
}
