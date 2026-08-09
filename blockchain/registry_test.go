package blockchain_test

import (
	"slices"
	"testing"

	_ "github.com/the-protobuf-project/runtime-go/blockchain/evm"
	_ "github.com/the-protobuf-project/runtime-go/blockchain/fabric"
	_ "github.com/the-protobuf-project/runtime-go/interfaces/orm"
	"github.com/the-protobuf-project/runtime-go/interfaces/store"
)

// All three drivers register into the one store registry, across both modules,
// without colliding — which is the whole point of the seam.
func TestAllDriversRegisterTogether(t *testing.T) {
	got := store.Backends()
	for _, want := range []store.Backend{store.ORM, store.EVM, store.Fabric} {
		if !slices.Contains(got, want) {
			t.Errorf("store.Backends() = %v, missing %q", got, want)
		}
	}
}
