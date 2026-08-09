package database

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Provider is a storage backend bound to a client you own.
//
// It deliberately has no CRUD methods. Until a database is chosen there is
// nothing to read or write, and a provider that also answered Get would need a
// default database nobody chose — the source of a whole class of "wrote to the
// wrong one" bugs.
//
// There is no Close either: the provider did not open the client and will not
// close it. Only a [DB] can own something, and only when it had to derive it.
type Provider interface {
	// SetDatabase selects a database and returns the driver over it. It reaches
	// the server before returning, so a bad address or a database that is not
	// there surfaces here rather than at the first Get.
	//
	// What a name selects is the backend's own idea of a database: a schema on
	// PostgreSQL, a database on MongoDB or ArangoDB, a key namespace on Redis.
	// It overrides [Resource.Schema] for every resource driven through the
	// result, which is what makes one set of generated descriptors serve many
	// tenants — the descriptor says what a Book is, the selection says whose.
	//
	// An empty name means the client's own database, and every resource keeps
	// the schema its descriptor was generated with.
	SetDatabase(ctx context.Context, name string) (*DB, error)

	// Backend names the implementation — "gorm", "mongodb" — for messages.
	Backend() string
}

// DB is one database: the CRUD driver over it, plus the capabilities the
// backend has beyond CRUD.
//
// [Driver] is embedded, so a DB is usable anywhere a Driver is and the adapter
// needs no change. The capability fields are never nil — where a backend does
// not have one, its methods report [ErrUnimplemented] naming the backend and
// why. A nil field would panic far from the wiring mistake that caused it, on a
// line that looks innocent.
type DB struct {
	// Driver is the CRUD contract, embedded so db.Get(...) reads as a call on
	// the database rather than on a field of it.
	Driver

	// Tx runs several operations atomically. Never nil.
	Tx Transactional

	// Schema creates and inspects the backend structures a [Resource]
	// describes. Never nil.
	Schema Migrator

	// Graph walks the relationships between records. Never nil; on a backend
	// with no graph its methods report [ErrUnimplemented].
	Graph Graph

	// Series partitions a resource by time and reduces over it. Never nil; on a
	// backend that does not partition, its methods report [ErrUnimplemented].
	Series TimeSeries

	// Backend names the implementation behind this database.
	Backend string

	// Name is what was selected, and is empty when the client's own database
	// was taken.
	Name string

	// Release frees whatever selecting this database allocated, and is nil when
	// selecting it allocated nothing. Call [DB.Close] rather than this.
	Release func() error
}

// Close releases what selecting this database allocated, and nothing else.
//
// The client you built is untouched — closing it stays your job, and doing it
// here would break every other database opened from it.
func (db *DB) Close() error {
	if db == nil || db.Release == nil {
		return nil
	}
	return db.Release()
}

// Build assembles a DB from a driver and whatever capabilities it turned out to
// have, filling the rest with refusals.
//
// Capabilities are resolved once, here, rather than per call: a driver cannot
// grow transactions halfway through a request, and a type assertion on every
// operation would be a cost paid forever for a fact known at construction. It
// is the one place a provider needs, so no backend writes a refusal of its own
// or forgets to.
func Build(d Driver, backend, name string, release func() error) *DB {
	tx, ok := d.(Transactional)
	if !ok {
		tx = refuses{backend}
	}
	schema, ok := d.(Migrator)
	if !ok {
		schema = refuses{backend}
	}
	graph, ok := d.(Graph)
	if !ok {
		graph = refuses{backend}
	}
	series, ok := d.(TimeSeries)
	if !ok {
		series = refuses{backend}
	}
	return &DB{
		Driver:  d,
		Tx:      tx,
		Schema:  schema,
		Graph:   graph,
		Series:  series,
		Backend: backend,
		Name:    name,
		Release: release,
	}
}

// refuses stands in for every capability a backend does not have, so the field
// is never nil and the message says which backend and which operation.
type refuses struct{ backend string }

var (
	_ Transactional = refuses{}
	_ Migrator      = refuses{}
	_ Graph         = refuses{}
	_ TimeSeries    = refuses{}
)

func (r refuses) Run(context.Context, func(*DB) error) error {
	return fmt.Errorf("%w: %s cannot run a transaction", ErrUnimplemented, r.backend)
}

func (r refuses) EnsureSchema(context.Context, *Resource) error {
	return fmt.Errorf("%w: %s cannot create a schema", ErrUnimplemented, r.backend)
}

func (r refuses) DropSchema(context.Context, *Resource) error {
	return fmt.Errorf("%w: %s cannot drop a schema", ErrUnimplemented, r.backend)
}

func (r refuses) HasSchema(context.Context, *Resource) (bool, error) {
	return false, fmt.Errorf("%w: %s cannot inspect a schema", ErrUnimplemented, r.backend)
}

// CheckDatabaseName reports whether name can be used to select a database.
//
// Empty is allowed and means the client's own. What is refused is a name that
// could change the meaning of a generated statement rather than the database it
// runs against: every backend here interpolates the name into an identifier
// position, so anything outside letters, digits and underscore is rejected
// rather than quoted and hoped for.
//
// This is a containment rule, not a style one. A database name usually comes
// from configuration, but in a multi-tenant program it comes from a request,
// and that is exactly the path an injected identifier travels.
func CheckDatabaseName(name string) error {
	if name == "" {
		return nil
	}
	if len(name) > 63 {
		return fmt.Errorf("database name %q is longer than 63 characters", name)
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return fmt.Errorf(
				"database name %q cannot contain %q: it goes into an identifier position, so only letters, digits and underscore are accepted",
				name, r)
		}
	}
	return nil
}

func (r refuses) Connect(context.Context, *Resource, Ref, Ref, proto.Message) (Edge, error) {
	return Edge{}, fmt.Errorf("%w: %s is not a graph", ErrUnimplemented, r.backend)
}

func (r refuses) Disconnect(context.Context, *Resource, string) error {
	return fmt.Errorf("%w: %s is not a graph", ErrUnimplemented, r.backend)
}

func (r refuses) Neighbors(context.Context, Ref, TraverseOptions) ([]Edge, error) {
	return nil, fmt.Errorf("%w: %s is not a graph", ErrUnimplemented, r.backend)
}

func (r refuses) Traverse(context.Context, Ref, TraverseOptions) ([]Path, error) {
	return nil, fmt.Errorf("%w: %s is not a graph", ErrUnimplemented, r.backend)
}

func (r refuses) EnsureHypertable(context.Context, *Resource, HypertableOptions) error {
	return fmt.Errorf("%w: %s does not partition a resource by time", ErrUnimplemented, r.backend)
}

func (r refuses) Range(context.Context, *Resource, RangeOptions) (ListResult, error) {
	return ListResult{}, fmt.Errorf("%w: %s does not partition a resource by time", ErrUnimplemented, r.backend)
}

func (r refuses) Aggregate(context.Context, *Resource, AggregateOptions) ([]Bucket, error) {
	return nil, fmt.Errorf("%w: %s cannot reduce over time", ErrUnimplemented, r.backend)
}
