package mongodb

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Provider is a MongoDB backend bound to a client you own.
//
// The client is yours: this package does not dial it and does not close it.
type Provider struct {
	client *Client
}

var _ database.Provider = (*Provider)(nil)

// NewProvider returns a provider over client.
func NewProvider(client *Client) *Provider { return &Provider{client: client} }

// Backend names this implementation.
func (p *Provider) Backend() string { return "mongodb" }

// SetDatabase selects a database and returns the driver over it.
//
// The name is a real MongoDB database, so the isolation is the server's rather
// than a naming convention: dropping one leaves its neighbors alone, and two
// tenants cannot read each other's collections by constructing a key. That is
// what makes this the natural backend for a multi-tenant program, and why the
// name overrides [database.Resource.Schema] for every resource driven through the
// result.
//
// An empty name keeps each resource in the database its descriptor names.
// Nothing is derived either way — one client serves every database — so
// [database.DB.Close] is a no-op and closing the client stays your job.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*database.DB, error) {
	if p.client == nil || p.client.inner == nil {
		return nil, fmt.Errorf("%w: mongodb provider has no client", database.ErrBadConfig)
	}
	if err := database.CheckDatabaseName(name); err != nil {
		return nil, fmt.Errorf("mongodb: %w", err)
	}

	// Reach the server before handing back a driver over it, so an unreachable
	// address or a refused credential surfaces on this line rather than on some
	// Get three layers into a request handler.
	if err := p.client.inner.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongodb: database %q is not reachable: %w", name, err)
	}

	d := &Driver{client: p.client.inner, db: name}
	return database.Build(d, "mongodb", name, nil), nil
}

// DropDatabase removes a database and everything in it.
func (p *Provider) DropDatabase(ctx context.Context, name string) error {
	if p.client == nil || p.client.inner == nil {
		return fmt.Errorf("%w: mongodb provider has no client", database.ErrBadConfig)
	}
	if err := database.CheckDatabaseName(name); err != nil {
		return fmt.Errorf("mongodb: %w", err)
	}
	if name == "" {
		return fmt.Errorf("mongodb: DropDatabase needs a name")
	}
	if err := p.client.inner.Database(name).Drop(ctx); err != nil {
		return fmt.Errorf("mongodb: cannot drop %q: %w", name, err)
	}
	return nil
}
