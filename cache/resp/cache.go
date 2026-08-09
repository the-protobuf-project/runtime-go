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

// SetDatabase selects a database and returns the strategies over it.
//
// When the index asked for is the one the client already has — the common case,
// and the reason to configure a client at all — nothing is derived and
// [cache.DB.Close] only drains background work. Otherwise a client is derived
// for that index and the DB owns it.
func (p *provider) SetDatabase(ctx context.Context, index int) (*cache.DB, error) {
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
		Release:      release,
	}), nil
}
