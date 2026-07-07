package graphql

import "errors"

// ErrConflict is returned by an optimistic-concurrency update helper (the generated
// UpdateIfMatch) when no row matched the precondition — i.e. the mutation reported zero
// affected rows because another writer changed the row first. Callers re-read the row and
// retry. Test for it with errors.Is(err, graphql.ErrConflict).
var ErrConflict = errors.New("graphql: update precondition failed (no rows matched; concurrent modification)")
