package timescale

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database/orm"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Driver is a store.Driver backed by TimescaleDB.
//
// It embeds the relational driver rather than reimplementing it. TimescaleDB is
// PostgreSQL with an extension: a hypertable is a table, a row is a row, and
// every method on [store.Driver] already works against one. Writing a second
// CRUD implementation would produce two ways for the same INSERT to behave and
// no benefit at all.
//
// What this package adds is the part that is not SQL — partitioning a table by
// time, reading a window of it, and reducing a window into buckets. That is
// [store.TimeSeries], and it is the whole of what is here.
type Driver struct {
	*orm.Driver

	db     *gorm.DB
	schema string
}

var (
	_ store.Driver     = (*Driver)(nil)
	_ store.TimeSeries = (*Driver)(nil)
)

// New returns a driver over a *gorm.DB you own, connected to TimescaleDB.
//
// Open it with gorm.Config{TranslateError: true} so duplicate-key and not-found
// errors arrive as the sentinels the relational driver maps.
func New(db *gorm.DB) *Driver {
	return &Driver{Driver: orm.New(db), db: db}
}

// Provider is a TimescaleDB backend bound to a *gorm.DB you own.
type Provider struct {
	db  *gorm.DB
	orm *orm.Provider
}

var _ store.Provider = (*Provider)(nil)

// NewProvider returns a provider over db.
func NewProvider(db *gorm.DB) *Provider {
	return &Provider{db: db, orm: orm.NewProvider(db)}
}

// Backend names this implementation.
func (p *Provider) Backend() string { return "timescaledb" }

// SetDatabase selects a schema and returns the driver over it.
//
// The relational provider does the selecting, because the answer is the same
// one: on PostgreSQL a name is a schema and every resource driven through the
// result is qualified with it. This wraps what it returns so the time-series
// half is reachable too.
func (p *Provider) SetDatabase(ctx context.Context, name string) (*store.DB, error) {
	if p.db == nil {
		return nil, fmt.Errorf("%w: timescale provider has no *gorm.DB", store.ErrBadConfig)
	}
	inner, err := p.orm.SetDatabase(ctx, name)
	if err != nil {
		return nil, err
	}
	relational, ok := inner.Driver.(*orm.Driver)
	if !ok {
		return nil, fmt.Errorf("timescale: the relational provider returned a %T", inner.Driver)
	}

	// The extension is checked here rather than on the first hypertable, so a
	// Postgres that is not TimescaleDB is a startup error naming what is missing
	// rather than a confusing failure inside EnsureHypertable.
	if verr := verifyExtension(ctx, p.db); verr != nil {
		return nil, verr
	}

	d := &Driver{Driver: relational, db: p.db, schema: name}
	return store.Build(d, "timescaledb", name, inner.Release), nil
}

// verifyExtension reports whether the TimescaleDB extension is installed.
func verifyExtension(ctx context.Context, db *gorm.DB) error {
	var version string
	err := db.WithContext(ctx).
		Raw("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").
		Scan(&version).Error
	if err != nil {
		return fmt.Errorf("timescale: cannot check for the extension: %w", err)
	}
	if version == "" {
		return fmt.Errorf(
			"%w: this PostgreSQL has no timescaledb extension; CREATE EXTENSION timescaledb, or use the orm package for plain SQL",
			store.ErrBadConfig)
	}
	return nil
}
