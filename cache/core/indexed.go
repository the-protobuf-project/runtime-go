package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// indexed is a document that also files each entry under the fields it was
// given, so it can be found by something other than its id.
//
// It embeds the document rather than reimplementing it: the id-keyed half is
// identical, and the only reason this type exists is the filing.
type indexed struct {
	*document
}

var _ cache.Indexed = (*indexed)(nil)

// pair is how a filed field is remembered under fields:{id}. One string rather
// than a map entry, because a set is the only structure a driver has to provide.
func pair(field, value string) string { return field + "=" + value }

// Create stores a value and files it under each index it was given.
//
// The index members go first, for the reason in the package documentation: a
// member naming an entry that is not there is swept on the next lookup, while an
// entry no index names is invisible to every lookup and every group delete.
func (s *indexed) Create(ctx context.Context, value any, opts ...cache.Option) (string, error) {
	o := cache.NewOptions(s.def, opts...)
	if len(o.Indexes) > 0 && s.sets == nil {
		return "", unsupported(s.driver.Name(), "index an entry", "no sets to index with")
	}
	id := o.ID
	if id == "" {
		id = s.newID()
	}
	if err := s.file(ctx, id, o.Indexes); err != nil {
		return "", err
	}

	// The id is resolved here because the index members are written before the
	// value, so it has to be settled before the entry is. Pass it down rather
	// than letting the document generate a second one — a fresh slice, so
	// appending cannot write into a caller's own options array.
	settled := make([]cache.Option, 0, len(opts)+1)
	settled = append(settled, opts...)
	settled = append(settled, cache.ID(id))
	return s.document.Create(ctx, value, settled...)
}

// Update replaces an entry and refiles it.
//
// Refiling is the part hand-rolled secondary indexes get wrong: change a user's
// e-mail and, without the removal below, the old address keeps finding them
// until the entry expires.
func (s *indexed) Update(ctx context.Context, id string, value any, opts ...cache.Option) error {
	o := cache.NewOptions(s.def, opts...)
	if err := s.document.Update(ctx, id, value, opts...); err != nil {
		return err
	}
	if s.sets == nil || len(o.Indexes) == 0 {
		return nil
	}
	return s.refile(ctx, id, o.Indexes)
}

// Delete removes an entry and every index member naming it.
func (s *indexed) Delete(ctx context.Context, id string) error {
	if s.sets != nil {
		if err := s.unfile(ctx, id); err != nil {
			return err
		}
	}
	return s.document.Delete(ctx, id)
}

// ByIndex decodes every live entry filed under field=value into dest.
func (s *indexed) ByIndex(ctx context.Context, field, value string, dest any) error {
	ids, err := s.IDsByIndex(ctx, field, value)
	if err != nil {
		return err
	}
	return s.fill(ctx, ids, dest)
}

// IDsByIndex returns the ids filed under field=value, sweeping the members whose
// entries have expired — batched exactly as [document.Keys] is, and for the same
// reason: an index nobody prunes is a slow leak that surfaces as lookups
// returning ids which decode to nothing.
func (s *indexed) IDsByIndex(ctx context.Context, field, value string) ([]string, error) {
	if s.sets == nil {
		return nil, unsupported(s.driver.Name(), "look up by "+field, "no sets to index with")
	}
	key := s.keys.by(field, value)
	ids, err := s.sets.SetMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("cache: cannot read the %s index: %w", field, err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	found, err := existsAll(ctx, s.driver, s.bulk, s.limit, s.entryKeys(ids))
	if err != nil {
		return nil, fmt.Errorf("cache: cannot check the %s index: %w", field, err)
	}

	live := make([]string, 0, len(ids))
	var stale []string
	for i, id := range ids {
		if found[i] {
			live = append(live, id)
			continue
		}
		stale = append(stale, id)
	}
	if len(stale) > 0 {
		_ = s.sets.SetRemove(ctx, key, stale...)
	}
	return live, nil
}

// DeleteByIndex removes every entry filed under field=value.
//
// This is group invalidation — every entry for one tenant, one user, one version
// of a computation — and it is the operation that makes an index worth
// maintaining even for a cache nobody queries by field.
//
// Done one id at a time it was the most expensive call in the package: a read
// and several removals each, serially. Here the field records are read in
// parallel, the removals are grouped so each index key is written once no matter
// how many ids it holds, and every value goes in a single delete.
func (s *indexed) DeleteByIndex(ctx context.Context, field, value string) (int, error) {
	ids, err := s.IDsByIndex(ctx, field, value)
	if err != nil || len(ids) == 0 {
		return 0, err
	}

	records, err := gather(ctx, s.limit, ids, func(ctx context.Context, id string) ([]string, error) {
		return s.sets.SetMembers(ctx, s.keys.fields(id))
	})
	if err != nil {
		return 0, fmt.Errorf("cache: cannot read the indexes of %d entr(ies): %w", len(ids), err)
	}

	members := make(map[string][]string) // index key -> ids to drop from it
	doomed := make([]string, 0, len(ids)*2)
	for i, id := range ids {
		for _, p := range records[i] {
			f, v, _ := strings.Cut(p, "=")
			key := s.keys.by(f, v)
			members[key] = append(members[key], id)
		}
		doomed = append(doomed, s.keys.entry(id), s.keys.fields(id))
	}

	for key, ids := range members {
		if rerr := s.sets.SetRemove(ctx, key, ids...); rerr != nil {
			return 0, fmt.Errorf("cache: cannot unfile from %s: %w", key, rerr)
		}
	}
	if rerr := s.sets.SetRemove(ctx, s.keys.index(), ids...); rerr != nil {
		return 0, fmt.Errorf("cache: cannot unindex: %w", rerr)
	}
	if derr := s.driver.Delete(ctx, doomed...); derr != nil {
		return 0, fmt.Errorf("cache: cannot delete: %w", derr)
	}
	return len(ids), nil
}
