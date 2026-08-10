package orm

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Provider is a relational backend bound to a *gorm.DB you own.
//
// The client is yours: this package does not open it and does not close it.
// Hand the same one to several providers and they share a pool.
type Provider struct {
	db *gorm.DB
}

var _ store.Provider = (*Provider)(nil)

// NewProvider returns a provider over db.
//
// Open db with gorm.Config{TranslateError: true} so duplicate-key and not-found
// errors arrive as the GORM sentinels this driver maps to
// [store.ErrAlreadyExists] and [store.ErrNotFound]. Without it those come back
// as raw driver strings and the adapter cannot tell them apart.
func NewProvider(db *gorm.DB) *Provider { return &Provider{db: db} }

// Backend names this implementation.
func (p *Provider) Backend() string { return "gorm" }

// SetDatabase selects a schema and returns the driver over it.
//
// On PostgreSQL the name is a schema and every resource driven through the
// result is qualified with it, overriding [store.Resource.Schema] — which is
// what lets one set of generated descriptors serve many tenants. On SQLite and
// MySQL, where a schema is a database, the same qualification applies and it is
// the caller's job to have created it.
//
// An empty name keeps each resource on the schema its descriptor was generated
// with. Nothing is derived either way, so [store.DB.Close] is a no-op: the
// connection pool is the one you handed in.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*store.DB, error) {
	if p.db == nil {
		return nil, fmt.Errorf("%w: orm provider has no *gorm.DB", store.ErrBadConfig)
	}
	if err := store.CheckDatabaseName(name); err != nil {
		return nil, fmt.Errorf("gorm: %w", err)
	}

	// Reach the server before handing back a driver over it, so a database that
	// is unreachable or a schema that is not there surfaces on this line rather
	// than on some Get three layers into a request handler.
	sql, err := p.db.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm: cannot reach the connection pool: %w", err)
	}
	if err := sql.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("gorm: database %q is not reachable: %w", name, err)
	}

	return store.Build(&Driver{db: p.db, schema: name}, "gorm", name, nil), nil
}
