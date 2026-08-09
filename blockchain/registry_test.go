package blockchain_test

import (
	"slices"
	"testing"

	_ "github.com/the-protobuf-project/runtime-go/blockchain/evm"
	_ "github.com/the-protobuf-project/runtime-go/blockchain/fabric"
	"github.com/the-protobuf-project/runtime-go/database"
	_ "github.com/the-protobuf-project/runtime-go/database/orm"
)

// All three drivers register into the one store registry, across both modules,
// without colliding — which is the whole point of the seam.
func TestAllDriversRegisterTogether(t *testing.T) {
	got := database.Backends()
	for _, want := range []database.Backend{database.ORM, database.EVM, database.Fabric} {
		if !slices.Contains(got, want) {
			t.Errorf("database.Backends() = %v, missing %q", got, want)
		}
	}
}
