package database

import "github.com/the-protobuf-project/runtime-go/database/store"

// The sentinels a caller tests for, re-exported so ordinary use never has to
// import [store] just to check whether a record was there.
var (
	// ErrNotFound is returned when no record carries the requested key.
	ErrNotFound = store.ErrNotFound

	// ErrAlreadyExists is returned when a write would duplicate a key or violate
	// a unique column.
	ErrAlreadyExists = store.ErrAlreadyExists

	// ErrUnimplemented is returned by a capability the backend does not have.
	ErrUnimplemented = store.ErrUnimplemented
)

// DB is one selected database, re-exported so a caller naming the type does not
// have to import [store] for it.
type DB = store.DB

// Provider is a backend bound to a client you own.
type Provider = store.Provider
