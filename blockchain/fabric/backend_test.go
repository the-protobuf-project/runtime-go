package fabric_test

import (
	"errors"
	"slices"
	"testing"

	_ "github.com/the-protobuf-project/runtime-go/blockchain/fabric"
	"github.com/the-protobuf-project/runtime-go/database"
)

// Importing this package must register the driver, since that is what makes
// database.NewDriver(database.Fabric, ...) work for anyone who imports it for effect
// alone.
func TestInitRegistersFabricBackend(t *testing.T) {
	if !slices.Contains(database.Backends(), database.Fabric) {
		t.Fatalf("database.Backends() = %v, want it to contain %q", database.Backends(), database.Fabric)
	}
}

func TestNewDriverBuildsWithoutConfig(t *testing.T) {
	d, err := database.NewDriver(database.Fabric, nil)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	if d == nil {
		t.Error("NewDriver returned a nil Driver with no error")
	}
}

func TestNewDriverRejectsUnexpectedConfig(t *testing.T) {
	if _, err := database.NewDriver(database.Fabric, "unexpected"); !errors.Is(err, database.ErrBadConfig) {
		t.Errorf("NewDriver error = %v, want it to wrap database.ErrBadConfig", err)
	}
}
