package core

import (
	"context"
	"fmt"
	"strings"
)

// The index bookkeeping, kept apart from the lookups it serves.
//
// Every function here maintains two things at once: the set of ids under an
// index value, and the record under fields:{id} of which values an id was filed
// under. The second exists only so the first can be undone — without it,
// removing one entry would mean reading every index in the database.

// file adds an id to each index named, and records the pairs under fields:{id}
// so they can be found again without reading every index in the database.
func (s *indexed) file(ctx context.Context, id string, indexes map[string]string) error {
	if len(indexes) == 0 {
		return nil
	}
	pairs := make([]string, 0, len(indexes))
	for field, value := range indexes {
		if err := s.sets.SetAdd(ctx, s.keys.by(field, value), id); err != nil {
			return fmt.Errorf("cache: cannot file %s under %s: %w", id, field, err)
		}
		pairs = append(pairs, pair(field, value))
	}
	if err := s.sets.SetAdd(ctx, s.keys.fields(id), pairs...); err != nil {
		return fmt.Errorf("cache: cannot record the indexes of %s: %w", id, err)
	}
	return nil
}

// refile drops the id from the indexes it no longer belongs to, then files it
// under the ones it does.
func (s *indexed) refile(ctx context.Context, id string, want map[string]string) error {
	keep := make(map[string]bool, len(want))
	for field, value := range want {
		keep[pair(field, value)] = true
	}
	had, err := s.sets.SetMembers(ctx, s.keys.fields(id))
	if err != nil {
		return fmt.Errorf("cache: cannot read the indexes of %s: %w", id, err)
	}
	var dropped []string
	for _, p := range had {
		if keep[p] {
			continue
		}
		field, value, _ := strings.Cut(p, "=")
		if rerr := s.sets.SetRemove(ctx, s.keys.by(field, value), id); rerr != nil {
			return fmt.Errorf("cache: cannot unfile %s from %s: %w", id, field, rerr)
		}
		dropped = append(dropped, p)
	}
	if len(dropped) > 0 {
		_ = s.sets.SetRemove(ctx, s.keys.fields(id), dropped...)
	}
	return s.file(ctx, id, want)
}

// unfile removes an id from every index naming it.
func (s *indexed) unfile(ctx context.Context, id string) error {
	had, err := s.sets.SetMembers(ctx, s.keys.fields(id))
	if err != nil {
		return fmt.Errorf("cache: cannot read the indexes of %s: %w", id, err)
	}
	for _, p := range had {
		field, value, _ := strings.Cut(p, "=")
		if rerr := s.sets.SetRemove(ctx, s.keys.by(field, value), id); rerr != nil {
			return fmt.Errorf("cache: cannot unfile %s from %s: %w", id, field, rerr)
		}
	}
	return s.driver.Delete(ctx, s.keys.fields(id))
}
