package d1

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Provider is a D1 backend bound to a client you own.
type Provider struct {
	client *Client
}

var _ store.Provider = (*Provider)(nil)

// NewProvider returns a provider over client's account.
func NewProvider(client *Client) *Provider { return &Provider{client: client} }

// Backend names this implementation.
func (p *Provider) Backend() string { return "d1" }

// SetDatabase selects a D1 database and returns the driver over it.
//
// An empty name is [Config.DatabaseID]; any other is looked up in the account
// with [Client.Lookup]. Either way the database is read once here, so a token
// without access or a database that is gone surfaces on this line rather than
// on some Get three layers into a request handler.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*store.DB, error) {
	if p.client == nil {
		return nil, fmt.Errorf("%w: d1 provider has no client; build one with d1.NewClient", store.ErrBadConfig)
	}
	if err := store.CheckDatabaseName(name); err != nil {
		return nil, fmt.Errorf("d1: %w", err)
	}

	id := p.client.cfg.DatabaseID
	if name != "" {
		found, err := p.client.Lookup(ctx, name)
		if err != nil {
			return nil, err
		}
		id = found
	} else {
		if id == "" {
			return nil, fmt.Errorf("%w: d1 was asked for its configured database, and Config.DatabaseID is empty", store.ErrBadConfig)
		}
		if err := p.client.Ping(ctx, id); err != nil {
			return nil, err
		}
	}
	return store.InstrumentDefault(store.Build(New(p.client, id), "d1", name, nil)), nil
}
