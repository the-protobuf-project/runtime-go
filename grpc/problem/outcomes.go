package problem

import "errors"

// The sentinels of the standard outcomes. A repository returns one, or an
// error wrapping one, and a server's Translator renders it with its code:
// NOT_FOUND, ALREADY_EXISTS, ABORTED and INVALID_ARGUMENT.
//
// They live here, rather than with either side, so that the repositories
// (database/repox) and the servers (grpc/servicex) agree on one value per
// outcome: a repository's not found is a server's not found under errors.Is.
var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrConflict        = errors.New("conflict")
	ErrInvalidArgument = errors.New("invalid argument")
)
