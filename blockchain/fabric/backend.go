package fabric

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Registers the Fabric driver with the database registry, so
// store.NewDriver(store.Fabric, nil) works once this package is linked in —
// whether imported directly or for its side effect alone.
func init() {
	store.Register(store.Fabric, open)
}

// open adapts the registry's untyped config to this driver's constructor. The
// stub takes no configuration, so anything but nil is a wiring mistake worth
// reporting rather than ignoring.
func open(cfg any) (store.Driver, error) {
	if cfg != nil {
		return nil, fmt.Errorf("%w: fabric driver takes no configuration, got %T", store.ErrBadConfig, cfg)
	}
	return New(), nil
}
