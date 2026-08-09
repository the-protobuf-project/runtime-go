package orm_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/glebarez/sqlite"
	_ "github.com/the-protobuf-project/runtime-go/interfaces/orm"
	"github.com/the-protobuf-project/runtime-go/interfaces/store"
	"gorm.io/gorm"
)

// Importing this package must register the driver, since that is what makes
// store.NewDriver(store.ORM, ...) work for anyone who imports it for effect
// alone.
func TestInitRegistersORMBackend(t *testing.T) {
	if !slices.Contains(store.Backends(), store.ORM) {
		t.Fatalf("store.Backends() = %v, want it to contain %q", store.Backends(), store.ORM)
	}
}

func TestNewDriverBuildsFromGormDB(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	d, err := store.NewDriver(store.ORM, db)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	if d == nil {
		t.Error("NewDriver returned a nil Driver with no error")
	}
}

// The config is opaque at the registry boundary, so passing the wrong thing is
// an easy mistake and has to fail loudly rather than nil-dereference later.
func TestNewDriverRejectsWrongConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  any
	}{
		{"wrong type", "a dsn string"},
		{"nil handle", (*gorm.DB)(nil)},
		{"no config", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.NewDriver(store.ORM, tc.cfg); !errors.Is(err, store.ErrBadConfig) {
				t.Errorf("NewDriver(%v) error = %v, want it to wrap store.ErrBadConfig", tc.cfg, err)
			}
		})
	}
}
