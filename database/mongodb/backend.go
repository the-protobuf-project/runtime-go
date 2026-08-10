package mongodb

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Registers the document driver with the database registry, so
// store.NewDriver(store.MongoDB, client) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	store.Register(store.MongoDB, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be the client the driver runs on, which this package neither
// dials nor closes.
func open(cfg any) (store.Driver, error) {
	client, ok := cfg.(*mongo.Client)
	if !ok {
		return nil, fmt.Errorf("%w: mongodb driver needs a *mongo.Client, got %T", store.ErrBadConfig, cfg)
	}
	if client == nil {
		return nil, fmt.Errorf("%w: mongodb driver was given a nil client", store.ErrBadConfig)
	}
	return New(client), nil
}
