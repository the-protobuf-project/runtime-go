package orm

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/interfaces/store"
	"gorm.io/gorm"
)

// Registers the relational driver with the store registry, so
// store.NewDriver(store.ORM, db) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	store.Register(store.ORM, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// config must be the *gorm.DB the driver runs on; see [New] for how it should
// be opened.
func open(cfg any) (store.Driver, error) {
	db, ok := cfg.(*gorm.DB)
	if !ok {
		return nil, fmt.Errorf("%w: orm driver needs a *gorm.DB, got %T", store.ErrBadConfig, cfg)
	}
	if db == nil {
		return nil, fmt.Errorf("%w: orm driver was given a nil *gorm.DB", store.ErrBadConfig)
	}
	return New(db), nil
}
