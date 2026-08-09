package memcached

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/core"
)

// New returns a cache backed by a client you own.
func New(client *Client, cfg cache.Config) cache.Provider {
	return &provider{client: client, cfg: cfg}
}

type provider struct {
	client *Client
	cfg    cache.Config
}

var _ cache.Provider = (*provider)(nil)

func (p *provider) Backend() string { return "memcached" }

// SetDatabase selects a named database.
//
// This is the call that costs memcached nothing to support. A name is a key
// segment on every backend, so orders here means exactly what orders means on
// Redis — the one selection form whose guarantee does not change underneath a
// program that switches backends.
//
// That guarantee is worth stating plainly rather than implying: names are kept
// apart by everyone agreeing to use them, not by the server. A flush_all still
// takes out every one of them, and a key built by hand elsewhere can still
// collide.
func (p *provider) SetDatabase(_ context.Context, name string) (*cache.DB, error) {
	if p.client == nil || p.client.inner == nil {
		return nil, fmt.Errorf("memcached: no client; build one with memcached.NewClient")
	}
	if err := core.CheckNamespace(name); err != nil {
		return nil, fmt.Errorf("memcached: %w", err)
	}
	if err := core.CheckKnown(name, p.cfg.Databases); err != nil {
		return nil, fmt.Errorf("memcached: %w", err)
	}

	return core.Build(primitives{client: p.client.inner}, core.Spec{
		Prefix:       p.cfg.Prefix,
		Namespace:    name,
		EmbedDB:      false, // the name already separates these keys
		DefaultTTL:   p.cfg.DefaultTTL,
		DefaultStale: p.cfg.DefaultStale,
		Concurrency:  p.cfg.Concurrency,
		RequireTTL:   p.cfg.RequireTTL,
		// Nothing was derived to reach this database — the namespace is a
		// string — so there is nothing here to release. Close still drains the
		// background refreshes core started, which is why it is not a no-op.
		Release: nil,
	}), nil
}

// SelectIndex selects a database by index.
//
// memcached has no numbered databases, so the index cannot be a property of the
// connection the way it is on a RESP server — it becomes a segment of the key
// instead: orders:db3:cache:vol:session-abc. Unlike [provider.SetDatabase],
// which promises a namespace and delivers one, this promises what the caller
// asked SELECT for and cannot deliver it. Two Redis databases are isolated by
// the server; two of these are not.
//
// The alternative was to reject any index but 0 here. That would make the
// contract honest at the cost of making it useless: a program that reads its
// database from configuration would simply fail to start against memcached,
// which is not a better outcome than a namespace that holds as long as nobody
// reaches around it.
func (p *provider) SelectIndex(_ context.Context, index int) (*cache.DB, error) {
	if p.client == nil || p.client.inner == nil {
		return nil, fmt.Errorf("memcached: no client; build one with memcached.NewClient")
	}
	if index < 0 {
		return nil, fmt.Errorf("memcached: database index cannot be negative, got %d", index)
	}

	return core.Build(primitives{client: p.client.inner}, core.Spec{
		Prefix:       p.cfg.Prefix,
		Database:     index,
		EmbedDB:      true, // no databases of its own; the index lives in the key
		DefaultTTL:   p.cfg.DefaultTTL,
		DefaultStale: p.cfg.DefaultStale,
		Concurrency:  p.cfg.Concurrency,
		RequireTTL:   p.cfg.RequireTTL,
		Release:      nil,
	}), nil
}

// DropDatabase reports [cache.ErrUnsupported].
//
// memcached has no cursor, so there is no way to find the keys belonging to one
// namespace short of knowing every key already. flush_all would empty the
// server — every database, every prefix, and anything else sharing it — which is
// not what was asked for and is the kind of help nobody wants.
func (p *provider) DropDatabase(_ context.Context, name string) (int, error) {
	if err := core.CheckNamespace(name); err != nil {
		return 0, fmt.Errorf("memcached: %w", err)
	}
	return 0, fmt.Errorf("%w: memcached cannot drop a database (no cursor to find its keys, and flush_all would empty the server)", cache.ErrUnsupported)
}
