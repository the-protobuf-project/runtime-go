package arangodb

import (
	"context"
	"fmt"

	"github.com/arangodb/go-driver/v2/arangodb"
	arangoshared "github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Option configures a provider.
type Option func(*Provider)

// WithRegistry supplies the registry the graph half needs to turn a resource
// name into a collection.
//
// A program that only stores records never needs it: every CRUD call is handed
// the descriptor it operates on. A program that walks edges does, because a
// [database.Ref] carries a name rather than a collection — which is what makes
// it portable to Neo4j, where collections do not exist.
func WithRegistry(reg *database.Registry) Option {
	return func(p *Provider) { p.registry = reg }
}

// Provider is an ArangoDB backend bound to a client you own.
type Provider struct {
	client   *Client
	registry *database.Registry
}

var _ database.Provider = (*Provider)(nil)

// NewProvider returns a provider over client.
func NewProvider(client *Client, opts ...Option) *Provider {
	p := &Provider{client: client}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// Backend names this implementation.
func (p *Provider) Backend() string { return "arangodb" }

// SetDatabase selects a database and returns the driver over it.
//
// The name is a real ArangoDB database, so the isolation is the server's: two
// tenants cannot read each other's collections by constructing a key, and
// dropping one leaves its neighbors alone.
//
// It must already exist. Creating one is [Provider.EnsureDatabase], kept
// separate because a typo in a database name should not silently produce an
// empty database that every read then reports as missing records.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*database.DB, error) {
	if p.client == nil || p.client.inner == nil {
		return nil, fmt.Errorf("%w: arangodb provider has no client", database.ErrBadConfig)
	}
	if err := database.CheckDatabaseName(name); err != nil {
		return nil, fmt.Errorf("arangodb: %w", err)
	}
	if name == "" {
		return nil, fmt.Errorf("arangodb: SetDatabase needs a name; ArangoDB has no implicit default")
	}

	// Reach the database before handing back a driver over it, so one that is
	// not there surfaces on this line rather than on some Get three layers into
	// a request handler.
	if _, err := p.client.inner.GetDatabase(ctx, name, &arangodb.GetDatabaseOptions{SkipExistCheck: false}); err != nil {
		if arangoshared.IsNotFound(err) {
			return nil, fmt.Errorf("%w: database %q", database.ErrNotFound, name)
		}
		return nil, fmt.Errorf("arangodb: database %q is not reachable: %w", name, err)
	}

	d := &Driver{client: p.client.inner, dbName: name, registry: p.registry}
	return database.Build(d, "arangodb", name, nil), nil
}

// EnsureDatabase creates a database if it is not already there, and is safe to
// call repeatedly.
func (p *Provider) EnsureDatabase(ctx context.Context, name string) error {
	if p.client == nil || p.client.inner == nil {
		return fmt.Errorf("%w: arangodb provider has no client", database.ErrBadConfig)
	}
	if err := database.CheckDatabaseName(name); err != nil {
		return fmt.Errorf("arangodb: %w", err)
	}
	if _, err := p.client.inner.CreateDatabase(ctx, name, nil); err != nil && !arangoshared.IsConflict(err) {
		return fmt.Errorf("arangodb: cannot create database %q: %w", name, err)
	}
	return nil
}

// DropDatabase removes a database and everything in it.
func (p *Provider) DropDatabase(ctx context.Context, name string) error {
	if p.client == nil || p.client.inner == nil {
		return fmt.Errorf("%w: arangodb provider has no client", database.ErrBadConfig)
	}
	if err := database.CheckDatabaseName(name); err != nil {
		return fmt.Errorf("arangodb: %w", err)
	}
	db, err := p.client.inner.GetDatabase(ctx, name, &arangodb.GetDatabaseOptions{SkipExistCheck: true})
	if err != nil {
		return fmt.Errorf("arangodb: cannot open %q: %w", name, err)
	}
	if rerr := db.Remove(ctx); rerr != nil {
		if arangoshared.IsNotFound(rerr) {
			return nil
		}
		return fmt.Errorf("arangodb: cannot drop %q: %w", name, rerr)
	}
	return nil
}
