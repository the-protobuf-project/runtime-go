package evm

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Registers the EVM driver with the database registry, so
// database.NewDriver(database.EVM, cfg) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	database.Register(database.EVM, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be this package's [Config]; a pointer to one is accepted too,
// since that is an easy thing to pass by mistake.
func open(cfg any) (database.Driver, error) {
	switch c := cfg.(type) {
	case Config:
		return newChecked(c)
	case *Config:
		if c == nil {
			return nil, fmt.Errorf("%w: evm driver was given a nil *Config", database.ErrBadConfig)
		}
		return newChecked(*c)
	default:
		return nil, fmt.Errorf("%w: evm driver needs an evm.Config, got %T", database.ErrBadConfig, cfg)
	}
}

// newChecked rejects a Config that cannot serve a single call before handing
// back a driver, so the failure names the missing wiring instead of surfacing
// as a nil dereference on the first request.
func newChecked(cfg Config) (database.Driver, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("%w: evm driver needs a chain Backend", database.ErrBadConfig)
	}
	if len(cfg.Resources) == 0 {
		return nil, fmt.Errorf("%w: evm driver needs at least one resource contract", database.ErrBadConfig)
	}
	return New(cfg), nil
}
