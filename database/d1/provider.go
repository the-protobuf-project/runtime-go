package d1

import (
	"context"
	"fmt"

	"github.com/cloudflare/cloudflare-go/v7"
	cfd1 "github.com/cloudflare/cloudflare-go/v7/d1"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Config says which account and database a driver talks to, and through which
// client.
type Config struct {
	// Client is the SDK client, carrying the API token. It is yours: this
	// package neither builds nor closes it.
	Client *cloudflare.Client

	// AccountID owns the databases.
	AccountID string

	// DatabaseID is the database [Provider.SetDatabase] selects for an empty
	// name, and the one [New] drives. It may be empty on a provider that only
	// ever selects by name.
	DatabaseID string
}

func (c Config) check() error {
	if c.Client == nil {
		return fmt.Errorf("%w: d1 needs a cloudflare client", store.ErrBadConfig)
	}
	if c.AccountID == "" {
		return fmt.Errorf("%w: d1 needs an account id", store.ErrBadConfig)
	}
	return nil
}

// Provider is a D1 backend bound to an account.
type Provider struct {
	cfg Config
}

var _ store.Provider = (*Provider)(nil)

// NewProvider returns a provider over the account in cfg.
func NewProvider(cfg Config) *Provider { return &Provider{cfg: cfg} }

// Backend names this implementation.
func (p *Provider) Backend() string { return "d1" }

// SetDatabase selects a D1 database and returns the driver over it.
//
// An empty name is [Config.DatabaseID]. Any other name is looked up in the
// account, and a name that matches no database is an error rather than a
// database created on the spot: creating one is an account change, and a typo in
// a tenant name should not be how a new one appears.
//
// Either way the database is read once here, so a token without access or a
// database that is gone surfaces on this line rather than on some Get three
// layers into a request handler.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*store.DB, error) {
	if err := p.cfg.check(); err != nil {
		return nil, err
	}
	if err := store.CheckDatabaseName(name); err != nil {
		return nil, fmt.Errorf("d1: %w", err)
	}

	id := p.cfg.DatabaseID
	if name != "" {
		found, err := p.lookup(ctx, name)
		if err != nil {
			return nil, err
		}
		id = found
	} else {
		if id == "" {
			return nil, fmt.Errorf("%w: d1 was asked for its configured database, and Config.DatabaseID is empty", store.ErrBadConfig)
		}
		_, err := p.cfg.Client.D1.Database.Get(ctx, id, cfd1.DatabaseGetParams{AccountID: cloudflare.F(p.cfg.AccountID)})
		if err != nil {
			return nil, fmt.Errorf("d1: database %s is not reachable: %w", id, err)
		}
	}

	cfg := p.cfg
	cfg.DatabaseID = id
	return store.InstrumentDefault(store.Build(New(cfg), "d1", name, nil)), nil
}

// lookup resolves a database name to its id.
//
// The list endpoint filters by name but matches loosely, so the result is
// checked for an exact match rather than trusted to be one: tenant_a must not
// select tenant_ab.
func (p *Provider) lookup(ctx context.Context, name string) (string, error) {
	pages := p.cfg.Client.D1.Database.ListAutoPaging(ctx, cfd1.DatabaseListParams{
		AccountID: cloudflare.F(p.cfg.AccountID),
		Name:      cloudflare.F(name),
	})
	for pages.Next() {
		if db := pages.Current(); db.Name == name {
			return db.UUID, nil
		}
	}
	if err := pages.Err(); err != nil {
		return "", fmt.Errorf("d1: cannot list databases: %w", err)
	}
	return "", fmt.Errorf("d1: no database named %q in the account; create it with `wrangler d1 create %s`", name, name)
}
