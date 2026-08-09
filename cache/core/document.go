package core

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// document stores whole encoded values and maintains an index so they can be
// enumerated. It is the same code on every backend.
type document struct {
	driver Driver
	sets   Sets       // nil when the backend has none
	scan   SetScanner // nil when a set cannot be walked with a cursor
	leases Leases     // nil when the protocol reports no remaining TTL
	bulk   Bulk       // nil when many keys cannot share a round trip
	keys   Keyspace
	def    cache.Options
	limit  int
	newID  func() string

	// require refuses a write that resolved to no expiry.
	require bool
}

var _ cache.Document = (*document)(nil)

// Create stores a value, generating an id when none was given.
//
// The index member goes first. A failure between the two writes then leaves the
// index naming an entry that is not there, which every read already sweeps —
// where the other order would leave an entry no listing can see and no group
// delete can reach.
func (s *document) Create(ctx context.Context, value any, opts ...cache.Option) (string, error) {
	o := cache.NewOptions(s.def, opts...)
	id := o.ID
	if id == "" {
		id = s.newID()
	}
	if err := checkTTL(s.require, o, "Document.Create", id); err != nil {
		return "", err
	}
	body, err := encode(value)
	if err != nil {
		return "", err
	}
	if s.sets != nil {
		if serr := s.sets.SetAdd(ctx, s.keys.index(), id); serr != nil {
			return "", fmt.Errorf("cache: cannot index %s: %w", id, serr)
		}
	}
	if serr := s.driver.Set(ctx, s.keys.entry(id), body, o.TTL); serr != nil {
		return "", fmt.Errorf("cache: cannot store %s: %w", id, serr)
	}
	return id, nil
}

// Get decodes an entry into dest.
func (s *document) Get(ctx context.Context, id string, dest any) error {
	body, err := s.driver.Get(ctx, s.keys.entry(id))
	if err != nil {
		return notFound(id, err)
	}
	return decode(body, dest)
}

// Update replaces an entry, reporting a missing one rather than creating it.
func (s *document) Update(ctx context.Context, id string, value any, opts ...cache.Option) error {
	o := cache.NewOptions(s.def, opts...)
	if err := checkTTL(s.require, o, "Document.Update", id); err != nil {
		return err
	}
	body, err := encode(value)
	if err != nil {
		return err
	}
	ok, err := s.driver.Replace(ctx, s.keys.entry(id), body, o.TTL)
	if err != nil {
		return fmt.Errorf("cache: cannot update %s: %w", id, err)
	}
	if !ok {
		return fmt.Errorf("%w: %s", cache.ErrNotFound, id)
	}
	return nil
}

// Delete removes an entry and its index member.
func (s *document) Delete(ctx context.Context, id string) error {
	if err := s.driver.Delete(ctx, s.keys.entry(id)); err != nil {
		return fmt.Errorf("cache: cannot delete %s: %w", id, err)
	}
	if s.sets != nil {
		if err := s.sets.SetRemove(ctx, s.keys.index(), id); err != nil {
			return fmt.Errorf("cache: cannot unindex %s: %w", id, err)
		}
	}
	return nil
}

// Keys returns live ids, sweeping the members whose entries have expired.
//
// This is the most expensive read in the cache and the one that scales worst:
// it is O(entries) however it is written. See [liveMembers] for what is bounded
// and what is not, and prefer [cache.Volatile] outright when enumeration is
// never needed.
func (s *document) Keys(ctx context.Context) ([]string, error) {
	if s.sets == nil {
		return nil, unsupported(s.driver.Name(), "enumerate keys", "no sets to index with")
	}
	live, err := liveMembers(ctx, s.driver, s.sets, s.scan, s.bulk, s.limit, s.keys.index(), s.keys.entry)
	if err != nil {
		return nil, fmt.Errorf("cache: cannot read the index: %w", err)
	}
	return live, nil
}

// List decodes every live entry into dest, which must point to a slice.
func (s *document) List(ctx context.Context, dest any) error {
	ids, err := s.Keys(ctx)
	if err != nil {
		return err
	}
	return s.fill(ctx, ids, dest)
}

// fill decodes ids into a slice destination, skipping what expired since the ids
// were read — a listing is a snapshot, not a transaction. Shared with the
// lookups in [indexed], which need exactly the same thing.
func (s *document) fill(ctx context.Context, ids []string, dest any) error {
	slice, elem, err := sliceTarget(dest)
	if err != nil {
		return err
	}
	bodies, err := getAll(ctx, s.driver, s.bulk, s.limit, s.entryKeys(ids))
	if err != nil {
		return err
	}
	out := reflect.MakeSlice(slice.Type(), 0, len(ids))
	for _, body := range bodies {
		if body == nil {
			continue // expired between the index read and now
		}
		item := reflect.New(elem)
		if derr := decode(body, item.Interface()); derr != nil {
			return derr
		}
		out = reflect.Append(out, item.Elem())
	}
	slice.Set(out)
	return nil
}

// entryKeys maps ids to the keys their values live under.
func (s *document) entryKeys(ids []string) []string {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.keys.entry(id)
	}
	return keys
}

// TTL reports how much longer an entry lives.
func (s *document) TTL(ctx context.Context, id string) (time.Duration, error) {
	if s.leases == nil {
		return 0, unsupported(s.driver.Name(), "report a remaining TTL", "the protocol never returns one")
	}
	ttl, err := s.leases.TTL(ctx, s.keys.entry(id))
	if err != nil {
		return 0, notFound(id, err)
	}
	return ttl, nil
}
