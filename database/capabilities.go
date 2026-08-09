package database

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Backends are not equally capable, and pretending otherwise is how a contract
// starts lying. Each interface here is something a driver implements only if it
// can; [Build] asserts for them once at construction and fills the gaps with a
// refusal that names the backend.
//
// This is deliberately different from returning [ErrUnimplemented] by hand. A
// method that exists on every driver and fails at runtime on some of them tells
// a caller nothing until it runs; an interface a driver either satisfies or does
// not is a fact settled at construction, testable with one assertion, and
// impossible for a backend to forget.

// Transactional is implemented by a driver that can run several operations
// atomically.
//
// The unit is a function rather than a Begin/Commit pair because the pair is
// almost always written wrong: a return between them leaks the transaction, and
// a panic leaves it open until the connection dies. A closure has one exit.
//
// Redis has MULTI but no rollback on a failed command, and a chain has no notion
// of a transaction at all beyond a single call, so neither implements this.
type Transactional interface {
	// Run calls fn with a database bound to a transaction, committing when fn
	// returns nil and rolling back on any error or panic.
	//
	// It hands over a [DB] rather than a [Driver] so a transaction reaches
	// everything the database can do. On a backend that is both a record store
	// and a graph, creating a record and the edges that join it is one
	// operation or it is nothing — and a body given only the CRUD half would
	// have to reach around the transaction to finish the job, which is the bug
	// this exists to prevent.
	//
	// The DB handed to fn is only valid for the life of the call. Storing it and
	// using it afterwards is a bug the implementation is not required to detect.
	// Its capability fields follow the same rule as any other: never nil, and
	// refusing by name where the backend has nothing behind them.
	Run(ctx context.Context, fn func(tx *DB) error) error
}

// Migrator is implemented by a driver that can create the backend structure a
// [Resource] describes.
//
// The descriptor already carries everything needed — column names, kinds, SQL
// types, the primary key, uniqueness, foreign keys — so the table, collection or
// index set is derivable rather than something a caller writes twice and keeps
// in sync by hand. That is the whole argument for generating the descriptor: the
// proto is the schema, and this is where that stops being a slogan.
//
// A chain driver does not implement it — a deployed contract is its schema, and
// creating one is a deployment, not a migration.
type Migrator interface {
	// EnsureSchema creates what res describes if it is not already there, and is
	// safe to call repeatedly. It does not alter a structure that exists with a
	// different shape: silently rewriting a live table is not a thing a library
	// should do on startup.
	EnsureSchema(ctx context.Context, res *Resource) error

	// DropSchema removes what res describes, and everything in it.
	DropSchema(ctx context.Context, res *Resource) error

	// HasSchema reports whether what res describes is already there.
	HasSchema(ctx context.Context, res *Resource) (bool, error)
}

// Batcher is implemented by a driver that can write or read many records in one
// exchange.
//
// It changes nothing about what a backend can express and a great deal about
// what a bulk operation costs: ten thousand records inserted one round trip
// each, against one statement per few hundred. Core code falls back to looping
// where it is absent, so this is an optimization and never a requirement.
type Batcher interface {
	// CreateMany stores every message, reporting one result per message in the
	// order given.
	CreateMany(ctx context.Context, res *Resource, msgs []proto.Message) ([]WriteResult, error)

	// GetMany returns the records for the given keys, in the order asked. A nil
	// entry is a record that was not there — not an error, because a bulk read
	// racing a delete is ordinary and the caller decides what a gap means.
	GetMany(ctx context.Context, res *Resource, keys []string) ([]proto.Message, error)
}

// Watcher is implemented by a driver whose backend can report changes as they
// happen — a MongoDB change stream, an ArangoDB write-ahead-log tail, a chain's
// event log.
//
// Polling is what a caller writes without this, and polling a store is the
// pattern that turns one slow query into a permanent load floor.
type Watcher interface {
	// Watch delivers a change per event until ctx is done, at which point the
	// channel is closed. A backend that has to start from a point in time takes
	// it from opts.
	Watch(ctx context.Context, res *Resource, opts WatchOptions) (<-chan Change, error)
}

// WatchOptions controls where a [Watcher] starts.
type WatchOptions struct {
	// Resume is an opaque token from a previous [Change]. Empty starts from now,
	// which is the safe default: replaying from the beginning of a busy
	// collection is rarely what someone meant and always expensive.
	Resume string
}

// ChangeKind says what happened to a record.
type ChangeKind int

const (
	// ChangeUnknown is the zero value, for a backend that cannot say.
	ChangeUnknown ChangeKind = iota
	ChangeCreated
	ChangeUpdated
	ChangeDeleted
)

// Change is one observed modification.
type Change struct {
	// Kind is what happened.
	Kind ChangeKind

	// Key is the primary key of the record that changed.
	Key string

	// Message is the record after the change, and is nil for a deletion and for
	// a backend that reports only that something changed.
	Message proto.Message

	// Resume is a token that restarts a watch just after this change. Empty
	// where the backend cannot produce one.
	Resume string
}
