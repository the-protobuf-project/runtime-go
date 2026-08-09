package redis

import (
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Registers the key-value driver with the database registry, so
// database.NewDriver(database.Redis, rdb) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	database.Register(database.Redis, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be the client the driver runs on, which this package neither
// dials nor closes.
func open(cfg any) (database.Driver, error) {
	rdb, ok := cfg.(goredis.UniversalClient)
	if !ok {
		return nil, fmt.Errorf("%w: redis driver needs a go-redis UniversalClient, got %T", database.ErrBadConfig, cfg)
	}
	if rdb == nil {
		return nil, fmt.Errorf("%w: redis driver was given a nil client", database.ErrBadConfig)
	}
	return New(rdb), nil
}
