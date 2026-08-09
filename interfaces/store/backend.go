package store

import (
	"fmt"
	"sort"
	"sync"
)

// Backend names a [Driver] implementation.
type Backend string

const (
	// ORM is the relational driver, in the interfaces/orm package.
	ORM Backend = "orm"
	// EVM is the Ethereum driver, in the blockchain/evm package.
	EVM Backend = "evm"
	// Fabric is the Hyperledger Fabric driver, in the blockchain/fabric package.
	Fabric Backend = "fabric"
)

// Opener constructs a driver from its backend-specific configuration. Drivers
// register one with [Register] from an init function.
//
// cfg is untyped on purpose. Unlike a cache or a stream, the drivers do not
// share a configuration shape — the relational driver needs a live *gorm.DB,
// the EVM driver needs contract ABIs and an RPC client, the Fabric stub needs
// nothing. A concrete Options struct covering all of them would force this
// package to import GORM and go-ethereum, and store deliberately depends on
// nothing but protobuf so that every driver can depend on it. Each opener
// asserts the config type it needs and reports [ErrBadConfig] when it does not
// match.
//
// Prefer the driver's own typed constructor — orm.New(db), evm.New(cfg) — when
// the backend is known at compile time. This registry is for selecting a
// backend by name at runtime, from configuration.
type Opener func(cfg any) (Driver, error)

var (
	registryMu sync.RWMutex
	drivers    = map[Backend]Opener{}
)

// Register makes a driver available to [NewDriver] under the given name. It is
// meant to be called from a driver package's init function, and panics if
// opener is nil or the backend is registered twice — both are programming
// errors worth surfacing at startup.
func Register(name Backend, opener Opener) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if opener == nil {
		panic("store: Register opener is nil for backend " + string(name))
	}
	if _, dup := drivers[name]; dup {
		panic("store: Register called twice for backend " + string(name))
	}
	drivers[name] = opener
}

// Backends returns the registered backend names, sorted.
func Backends() []Backend {
	registryMu.RLock()
	defer registryMu.RUnlock()

	names := make([]Backend, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names
}

// NewDriver opens the named backend with its own configuration. The driver's
// package must be linked in for it to be registered — import it directly, or
// for the side effect alone:
//
//	import _ "github.com/the-protobuf-project/runtime-go/interfaces/orm"
//
//	drv, err := store.NewDriver(store.ORM, gormDB)
func NewDriver(name Backend, cfg any) (Driver, error) {
	registryMu.RLock()
	opener, ok := drivers[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf(
			"store: backend %q is not registered (registered: %v); "+
				"import the driver package to register it",
			name, Backends())
	}
	return opener(cfg)
}
