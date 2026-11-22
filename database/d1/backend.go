package d1

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Registers the D1 driver with the database registry, so
// store.NewDriver(store.D1, client) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	store.Register(store.D1, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be the *Client the driver runs on, and the database it drives is
// the client's [Config.DatabaseID].
func open(cfg any) (store.Driver, error) {
	c, ok := cfg.(*Client)
	if !ok {
		return nil, fmt.Errorf("%w: d1 driver needs a *d1.Client, got %T", store.ErrBadConfig, cfg)
	}
	if c == nil {
		return nil, fmt.Errorf("%w: d1 driver was given a nil client", store.ErrBadConfig)
	}
	if c.cfg.DatabaseID == "" {
		return nil, fmt.Errorf("%w: d1 driver needs a client with a database id", store.ErrBadConfig)
	}
	return New(c, c.cfg.DatabaseID), nil
}
