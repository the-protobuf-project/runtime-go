package store

import "errors"

// The driver error contract. Drivers return these sentinels (directly or wrapped
// with fmt.Errorf("%w", …)) so the adapter can translate storage outcomes into
// gRPC status codes without knowing which backend produced them. Callers test
// with errors.Is.
var (
	// ErrNotFound is returned by Get/Update/Delete/Exists when no record carries
	// the requested key. Maps to gRPC NotFound.
	ErrNotFound = errors.New("store: not found")

	// ErrAlreadyExists is returned by Create when a record with the same key (or a
	// value violating a unique constraint) already exists. Maps to gRPC AlreadyExists.
	ErrAlreadyExists = errors.New("store: already exists")

	// ErrUnimplemented is returned by a driver (or one of its methods) that does
	// not yet support an operation — e.g. the fabric write path before its bridge
	// lands. Maps to gRPC Unimplemented.
	ErrUnimplemented = errors.New("store: unimplemented")

	// ErrPermissionDenied is returned when the backend rejects a write for
	// authorization reasons — e.g. an on-chain access-control revert (onlyOwner /
	// per-record owner). Maps to gRPC PermissionDenied.
	ErrPermissionDenied = errors.New("store: permission denied")
)
