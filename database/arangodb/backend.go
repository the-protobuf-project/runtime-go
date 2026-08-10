package arangodb

import (
	"fmt"

	"github.com/arangodb/go-driver/v2/arangodb"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Registers the graph driver with the database registry, so
// store.NewDriver(store.ArangoDB, client) works once this package is
// linked in — whether imported directly or for its side effect alone.
func init() {
	store.Register(store.ArangoDB, open)
}

// open adapts the registry's untyped config to this driver's constructor.
//
// A driver built this way has no resource registry, so it stores and reads
// records but refuses to walk edges — [WithRegistry] is how a program that
// needs the graph half supplies one, and it goes through [NewProvider].
func open(cfg any) (store.Driver, error) {
	client, ok := cfg.(arangodb.Client)
	if !ok {
		return nil, fmt.Errorf("%w: arangodb driver needs an arangodb.Client, got %T", store.ErrBadConfig, cfg)
	}
	return &Driver{client: client}, nil
}
