package fabric_test

import (
	"errors"
	"slices"
	"testing"

	_ "github.com/the-protobuf-project/runtime-go/blockchain/fabric"
	"github.com/the-protobuf-project/runtime-go/interfaces/store"
)

// Importing this package must register the driver, since that is what makes
// store.NewDriver(store.Fabric, ...) work for anyone who imports it for effect
// alone.
func TestInitRegistersFabricBackend(t *testing.T) {
	if !slices.Contains(store.Backends(), store.Fabric) {
		t.Fatalf("store.Backends() = %v, want it to contain %q", store.Backends(), store.Fabric)
	}
}

func TestNewDriverBuildsWithoutConfig(t *testing.T) {
	d, err := store.NewDriver(store.Fabric, nil)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	if d == nil {
		t.Error("NewDriver returned a nil Driver with no error")
	}
}

func TestNewDriverRejectsUnexpectedConfig(t *testing.T) {
	if _, err := store.NewDriver(store.Fabric, "unexpected"); !errors.Is(err, store.ErrBadConfig) {
		t.Errorf("NewDriver error = %v, want it to wrap store.ErrBadConfig", err)
	}
}
