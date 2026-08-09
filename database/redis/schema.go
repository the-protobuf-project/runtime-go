package redis

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database"
)

// EnsureSchema does nothing, successfully.
//
// Redis has no schema: a key holds whatever it is given, and a resource is ready
// to store the moment something stores it. Reporting that as success rather than
// as [database.ErrUnimplemented] is the honest answer to what the method actually
// asks — "is what this descriptor describes ready to use" — and it means a
// program that calls EnsureSchema on startup runs unchanged against Redis and
// against SQL, which is the point of having one contract.
//
// What it does not do is create the id set. That appears on the first write and
// an empty set is indistinguishable from no set in Redis anyway.
func (d *Driver) EnsureSchema(_ context.Context, res *database.Resource) error {
	if res == nil {
		return fmt.Errorf("redis: EnsureSchema needs a resource")
	}
	return nil
}

// HasSchema reports true for any resource.
//
// For the same reason EnsureSchema succeeds: there is nothing that could be
// absent. A caller using this to decide whether to run a migration will
// correctly decide not to.
func (d *Driver) HasSchema(_ context.Context, res *database.Resource) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("redis: HasSchema needs a resource")
	}
	return true, nil
}

// DropSchema removes every key belonging to a resource: the records, the id set,
// and every unique reservation.
//
// This is the one Migrator method that does real work here, and it is a cursor
// walk rather than a lookup — a namespace is a naming convention and Redis has
// no index of it, so finding one resource's keys means looking at all of them.
// Proportional to the whole keyspace, not to the resource, and not atomic: a
// write arriving mid-walk may survive. Fine for a teardown; think twice
// elsewhere.
func (d *Driver) DropSchema(ctx context.Context, res *database.Resource) error {
	if res == nil {
		return fmt.Errorf("redis: DropSchema needs a resource")
	}
	pattern := d.keys.pattern(res)

	var cursor uint64
	for {
		batch, next, err := d.rdb.Scan(ctx, cursor, pattern, scanBatch).Result()
		if err != nil {
			return fmt.Errorf("redis: cannot walk the keyspace of %s: %w", res.Name, err)
		}
		if len(batch) > 0 {
			if derr := d.rdb.Del(ctx, batch...).Err(); derr != nil {
				return fmt.Errorf("redis: cannot drop %s: %w", res.Name, derr)
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
