package orm_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/the-protobuf-project/runtime-go/database"
	_ "github.com/the-protobuf-project/runtime-go/database/orm"
	"gorm.io/gorm"
)

// Importing this package must register the driver, since that is what makes
// database.NewDriver(database.ORM, ...) work for anyone who imports it for effect
// alone.
func TestInitRegistersORMBackend(t *testing.T) {
	if !slices.Contains(database.Backends(), database.ORM) {
		t.Fatalf("database.Backends() = %v, want it to contain %q", database.Backends(), database.ORM)
	}
}

func TestNewDriverBuildsFromGormDB(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	d, err := database.NewDriver(database.ORM, db)
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
			if _, err := database.NewDriver(database.ORM, tc.cfg); !errors.Is(err, database.ErrBadConfig) {
				t.Errorf("NewDriver(%v) error = %v, want it to wrap database.ErrBadConfig", tc.cfg, err)
			}
		})
	}
}
