package core

import (
	"context"
	"fmt"
)

// dropBatch is how many keys go into one delete while dropping a database.
const dropBatch = 256

// Drop deletes every key under one keyspace head and reports how many went.
//
// It walks the keyspace with a cursor and deletes in batches, so dropping a
// database neither builds a list of every key first nor issues one round trip
// per key. Neither of those makes it cheap: the walk is proportional to the
// whole keyspace rather than to the database being dropped, because a namespace
// is a naming convention and the server has no index of it.
//
// Not atomic, and it does not pretend to be. A cursor is a snapshot of nothing
// in particular, so a write arriving mid-walk may survive the drop. For a
// teardown that is fine; for anything where it is not, use a real database and
// FLUSHDB.
func Drop(ctx context.Context, driver Driver, head string) (int, error) {
	scanner, ok := driver.(Scanner)
	if !ok {
		return 0, unsupported(driver.Name(), "drop a database", "no cursor to walk the keyspace with")
	}

	keys, err := scanner.Scan(ctx, head+"*")
	if err != nil {
		return 0, fmt.Errorf("cache: cannot walk the keyspace: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}

	for start := 0; start < len(keys); start += dropBatch {
		end := min(start+dropBatch, len(keys))
		if derr := driver.Delete(ctx, keys[start:end]...); derr != nil {
			return start, fmt.Errorf("cache: cannot delete: %w", derr)
		}
	}
	return len(keys), nil
}
