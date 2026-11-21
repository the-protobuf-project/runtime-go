package workerskv

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

func (p *provider) Backend() string { return backend }

func (p *provider) primitives() (primitives, error) {
	if p.client == nil || p.client.inner == nil {
		return primitives{}, fmt.Errorf("workerskv: no client; build one with workerskv.NewClient")
	}
	return primitives{client: p.client}, nil
}

// SetDatabase selects a named database: a key segment, as on every backend.
// [NewClient] already reached the namespace, so nothing is asked of the server
// here.
func (p *provider) SetDatabase(_ context.Context, name string) (*cache.DB, error) {
	prims, err := p.primitives()
	if err != nil {
		return nil, err
	}
	if err := core.CheckNamespace(name); err != nil {
		return nil, fmt.Errorf("workerskv: %w", err)
	}
	if err := core.CheckKnown(name, p.cfg.Databases); err != nil {
		return nil, fmt.Errorf("workerskv: %w", err)
	}
	return core.Build(prims, core.Spec{
		Prefix:       p.cfg.Prefix,
		Namespace:    name,
		EmbedDB:      false, // the name already separates these keys
		DefaultTTL:   p.cfg.DefaultTTL,
		DefaultStale: p.cfg.DefaultStale,
		Concurrency:  p.cfg.Concurrency,
		RequireTTL:   p.cfg.RequireTTL,
		Release:      nil,
	}), nil
}

// SelectIndex selects a database by index. KV has no numbered databases, so the
// index becomes a key segment and the server enforces nothing between two of
// them; a separate namespace is the boundary it does enforce.
func (p *provider) SelectIndex(_ context.Context, index int) (*cache.DB, error) {
	prims, err := p.primitives()
	if err != nil {
		return nil, err
	}
	if index < 0 {
		return nil, fmt.Errorf("workerskv: database index cannot be negative, got %d", index)
	}
	return core.Build(prims, core.Spec{
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

// DropDatabase lists a named database's keys by prefix and deletes them in
// bulk. A listing lags writes like everything else in KV, so a key written in
// the last minute may survive the drop.
func (p *provider) DropDatabase(ctx context.Context, name string) (int, error) {
	prims, err := p.primitives()
	if err != nil {
		return 0, err
	}
	if cerr := core.CheckNamespace(name); cerr != nil {
		return 0, fmt.Errorf("workerskv: %w", cerr)
	}
	head := core.NewKeyspace(p.cfg.Prefix, name, 0, false).Head()
	n, err := core.Drop(ctx, prims, head)
	if err != nil {
		return n, fmt.Errorf("workerskv: cannot drop %q: %w", name, err)
	}
	return n, nil
}
