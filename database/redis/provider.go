package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Provider is a Redis backend bound to a client you own.
//
// The client is yours: this package does not dial it and does not close it.
// Hand the same one to the cache and streams layers and all three share a pool.
type Provider struct {
	rdb goredis.UniversalClient
	cfg config
}

var _ database.Provider = (*Provider)(nil)

// NewProvider returns a provider over rdb.
func NewProvider(rdb goredis.UniversalClient, opts ...Option) *Provider {
	return &Provider{rdb: rdb, cfg: newConfig(opts...)}
}

// Backend names this implementation.
func (p *Provider) Backend() string { return "redis" }

// SetDatabase selects a database and returns the driver over it.
//
// The name becomes a key segment rather than a Redis numbered database. Two
// reasons, and both matter for what this is for: a numbered database is a
// property of the connection, so selecting one means a second client and a
// second pool, and Redis ships with sixteen of them — which would cap a
// multi-tenant program at sixteen tenants. A segment has no ceiling, needs no
// connection of its own, and works on a cluster, where database 0 is the only
// one there is.
//
// What it does not give you is server-side isolation: two names are kept apart
// by everyone agreeing to use them, and a FLUSHDB reaches both. That is the
// trade, and it is the same one the cache module makes.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*database.DB, error) {
	if p.rdb == nil {
		return nil, fmt.Errorf("%w: redis provider has no client", database.ErrBadConfig)
	}
	if err := database.CheckDatabaseName(name); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	// Reach the server before handing back a driver over it, so an unreachable
	// address or a refused AUTH surfaces on this line rather than on some Get
	// three layers into a request handler.
	if err := p.rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: database %q is not reachable: %w", name, err)
	}

	d := &Driver{rdb: p.rdb, keys: newKeys(p.cfg.prefix, name)}
	return database.Build(d, "redis", name, nil), nil
}
