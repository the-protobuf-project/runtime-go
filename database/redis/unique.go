package redis

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database"
)

// The unique-column bookkeeping, kept apart from the CRUD it serves.
//
// A relational backend gets uniqueness from the engine. Redis has no
// constraints, so a column the descriptor marks Unique is only unique if this
// package makes it so — and a column that says it is unique while not being
// unique is exactly the kind of quiet lie the rest of this contract exists to
// avoid. One key per value, claimed with SET NX, is what makes two writers
// racing on the same e-mail address resolvable at all.

// claim is one reservation this call took, remembered so it can be released.
type claim struct{ key string }

// uniqueValue is one unique column and the value a message carries for it.
type uniqueValue struct {
	column string
	value  string
}

// uniqueValues returns the unique, non-primary columns of msg that carry a
// value.
//
// The primary key is excluded because the record key already enforces it: a
// second reservation would cost a round trip to prove something SET NX on the
// record has already proved. An empty value is skipped rather than reserved —
// otherwise the first record with an unset optional column would lock every
// other record out of leaving it unset.
func uniqueValues(res *database.Resource, msg proto.Message) []uniqueValue {
	var out []uniqueValue
	if msg == nil {
		return nil
	}
	cols, err := database.MessageToColumns(res, msg)
	if err != nil {
		return nil
	}
	for _, c := range res.Columns {
		if !c.Unique || c.PrimaryKey {
			continue
		}
		v, ok := cols[c.Name]
		if !ok || v == nil {
			continue
		}
		s := fmt.Sprint(v)
		if s == "" {
			continue
		}
		out = append(out, uniqueValue{column: c.Name, value: s})
	}
	return out
}

// claimUnique reserves every unique value msg carries for key.
//
// It reports the reservations it took so a caller can release them, and the
// first value that was already held — a conflict rather than an error, because
// it is an outcome the caller turns into [database.ErrAlreadyExists] with context
// only it has.
func (d *Driver) claimUnique(ctx context.Context, res *database.Resource, msg proto.Message, key string) (claimed []claim, conflict string, err error) {
	for _, u := range uniqueValues(res, msg) {
		k := d.keys.unique(res, u.column, escape(u.value))
		won, serr := d.rdb.SetNX(ctx, k, key, 0).Result()
		if serr != nil {
			return claimed, "", fmt.Errorf("redis: cannot reserve %s: %w", u.column, serr)
		}
		if !won {
			// Already held — but possibly by this very record, which happens
			// when Update rewrites a value it already owns.
			owner, gerr := d.rdb.Get(ctx, k).Result()
			if gerr == nil && owner == key {
				continue
			}
			return claimed, u.column, nil
		}
		claimed = append(claimed, claim{key: k})
	}
	return claimed, "", nil
}

// moveUnique claims what the new message needs and releases what the old one
// held and the new one does not.
//
// The claim comes before the release, so a failure partway leaves the old
// reservations intact rather than freeing a value the record still carries.
func (d *Driver) moveUnique(ctx context.Context, res *database.Resource, old, next proto.Message, key string) (claimed []claim, conflict string, err error) {
	claimed, conflict, err = d.claimUnique(ctx, res, next, key)
	if err != nil || conflict != "" {
		return claimed, conflict, err
	}

	keep := make(map[string]bool)
	for _, u := range uniqueValues(res, next) {
		keep[d.keys.unique(res, u.column, escape(u.value))] = true
	}
	var stale []string
	for _, u := range uniqueValues(res, old) {
		k := d.keys.unique(res, u.column, escape(u.value))
		if !keep[k] {
			stale = append(stale, k)
		}
	}
	if len(stale) > 0 {
		// A reservation that fails to clear is a value nobody can reuse, which
		// is bad but not wrong: the record is still correct, so this does not
		// fail the write.
		_ = d.rdb.Del(ctx, stale...).Err()
	}
	return claimed, "", nil
}

// release drops reservations this call took, used to undo a partial write.
func (d *Driver) release(ctx context.Context, claimed []claim) {
	if len(claimed) == 0 {
		return
	}
	keys := make([]string, len(claimed))
	for i, c := range claimed {
		keys[i] = c.key
	}
	_ = d.rdb.Del(ctx, keys...).Err()
}
