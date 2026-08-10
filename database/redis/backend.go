package redis

import (
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Registers the key-value driver with the database registry, so
// store.NewDriver(store.Redis, rdb) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	store.Register(store.Redis, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be the client the driver runs on, which this package neither
// dials nor closes.
func open(cfg any) (store.Driver, error) {
	rdb, ok := cfg.(goredis.UniversalClient)
	if !ok {
		return nil, fmt.Errorf("%w: redis driver needs a go-redis UniversalClient, got %T", store.ErrBadConfig, cfg)
	}
	if rdb == nil {
		return nil, fmt.Errorf("%w: redis driver was given a nil client", store.ErrBadConfig)
	}
	return New(rdb), nil
}
