package d1

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Registers the D1 driver with the database registry, so
// store.NewDriver(store.D1, d1.Config{...}) works once this package is linked
// in — whether imported directly or for its side effect alone.
func init() {
	store.Register(store.D1, open)
}

// open adapts the registry's untyped config to this driver's constructor.
func open(cfg any) (store.Driver, error) {
	c, ok := cfg.(Config)
	if !ok {
		return nil, fmt.Errorf("%w: d1 driver needs a d1.Config, got %T", store.ErrBadConfig, cfg)
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	if c.DatabaseID == "" {
		return nil, fmt.Errorf("%w: d1 driver needs a database id", store.ErrBadConfig)
	}
	return New(c), nil
}
