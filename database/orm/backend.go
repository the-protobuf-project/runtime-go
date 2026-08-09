package orm

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database"
	"gorm.io/gorm"
)

// Registers the relational driver with the database registry, so
// database.NewDriver(database.ORM, db) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	database.Register(database.ORM, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be the *gorm.DB the driver runs on; see [New] for how it should
// be opened.
func open(cfg any) (database.Driver, error) {
	db, ok := cfg.(*gorm.DB)
	if !ok {
		return nil, fmt.Errorf("%w: orm driver needs a *gorm.DB, got %T", database.ErrBadConfig, cfg)
	}
	if db == nil {
		return nil, fmt.Errorf("%w: orm driver was given a nil *gorm.DB", database.ErrBadConfig)
	}
	return New(db), nil
}
