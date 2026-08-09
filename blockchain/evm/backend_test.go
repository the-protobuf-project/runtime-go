package evm_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/the-protobuf-project/runtime-go/blockchain/evm"
	"github.com/the-protobuf-project/runtime-go/database"
)

// Importing this package must register the driver, since that is what makes
// database.NewDriver(database.EVM, ...) work for anyone who imports it for effect
// alone.
func TestInitRegistersEVMBackend(t *testing.T) {
	if !slices.Contains(database.Backends(), database.EVM) {
		t.Fatalf("database.Backends() = %v, want it to contain %q", database.Backends(), database.EVM)
	}
}

// A Config wired enough to construct a driver. The backend is never called —
// these tests only cover the registry boundary, not chain traffic.
func validConfig() evm.Config {
	return evm.Config{
		Backend: stubBackend{},
		Resources: map[string]evm.Contract{
			"Book": {ABI: abi.ABI{}, Address: common.HexToAddress("0x1")},
		},
	}
}

func TestNewDriverAcceptsConfigByValueAndPointer(t *testing.T) {
	cfg := validConfig()

	t.Run("value", func(t *testing.T) {
		if _, err := database.NewDriver(database.EVM, cfg); err != nil {
			t.Errorf("NewDriver with evm.Config: %v", err)
		}
	})
	t.Run("pointer", func(t *testing.T) {
		if _, err := database.NewDriver(database.EVM, &cfg); err != nil {
			t.Errorf("NewDriver with *evm.Config: %v", err)
		}
	})
}

// Config that cannot serve a single call must be rejected at construction, so
// the failure names the missing wiring instead of surfacing as a nil
// dereference on the first request.
func TestNewDriverRejectsUnusableConfig(t *testing.T) {
	noBackend := validConfig()
	noBackend.Backend = nil

	noResources := validConfig()
	noResources.Resources = nil

	for _, tc := range []struct {
		name string
		cfg  any
	}{
		{"wrong type", "not a config"},
		{"nil pointer", (*evm.Config)(nil)},
		{"no config", nil},
		{"no backend", noBackend},
		{"no resources", noResources},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := database.NewDriver(database.EVM, tc.cfg); !errors.Is(err, database.ErrBadConfig) {
				t.Errorf("NewDriver error = %v, want it to wrap database.ErrBadConfig", err)
			}
		})
	}
}
