package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/memcached"
)

// withMemcached runs the same three steps against a backend that can do far
// less. The call sites do not change — only what some of them answer.
func withMemcached(ctx context.Context) error {
	client, err := memcached.NewClient(memcached.Config{
		Servers: []string{"localhost:11211"},
		Timeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	c := memcached.New(client, cache.Config{Prefix: "example", DefaultTTL: time.Minute})

	// A name costs memcached nothing to honor: it is a key segment here exactly
	// as it is on Redis — example:orders:cache:vol:session-abc — which makes
	// this the one selection form that means the same thing on both. Isolation
	// is still by agreement rather than by the server.
	db, err := c.SetDatabase(ctx, "orders")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s database %q", db.Backend, db.Name)

	// --- VOLATILE: what this backend is for, and fully supported ---
	if serr := db.Volatile.Set(ctx, "session:abc", user{Name: "Alice"}, cache.TTL(time.Hour)); serr != nil {
		return serr
	}
	var session user
	if gerr := db.Volatile.Get(ctx, "session:abc", &session); gerr != nil {
		return gerr
	}
	if terr := db.Volatile.Touch(ctx, "session:abc", 2*time.Hour); terr != nil {
		return terr
	}
	log.Printf("volatile: read back %+v", session)

	// --- DOCUMENT: stores and fetches by id, and stops there ---
	id, err := db.Document.Create(ctx, user{Name: "Bob", Age: 25}, cache.TTL(time.Minute))
	if err != nil {
		return err
	}
	var bob user
	if err := db.Document.Get(ctx, id, &bob); err != nil {
		return err
	}
	log.Printf("document: stored and read %+v", bob)

	// What the backend cannot do, it says. Every one of these refusals comes
	// from core noticing a capability this driver does not implement — there is
	// no code in the memcached package that mentions enumeration at all.
	if _, err := db.Document.Keys(ctx); errors.Is(err, cache.ErrUnsupported) {
		log.Printf("memcached: %v", err)
	}
	if _, err := db.Document.TTL(ctx, id); errors.Is(err, cache.ErrUnsupported) {
		log.Printf("memcached: %v", err)
	}
	var found []user
	if err := db.Indexed.ByIndex(ctx, "email", "alice@example.com", &found); errors.Is(err, cache.ErrUnsupported) {
		log.Printf("memcached: %v", err)
	}
	if _, err := db.Volatile.Scan(ctx, "session:*"); errors.Is(err, cache.ErrUnsupported) {
		log.Printf("memcached: %v", err)
	}

	// --- ASIDE: fully supported, on a backend missing four capabilities ---
	// Read-through needs only a get, a lease, and an atomic add. Without a
	// conditional delete there is no cross-process lock, so loads collapse
	// within this process rather than across the fleet — correct either way.
	loads := 0
	people := db.Aside(func(ctx context.Context, id string) (any, error) {
		loads++
		return user{Name: "Carol", Email: id + "@example.com"}, nil
	})

	var carol user
	for range 3 {
		if err := people.GetOrLoad(ctx, "carol", &carol, cache.TTL(time.Minute)); err != nil {
			return err
		}
	}
	log.Printf("aside: 3 reads, %d load(s)", loads)

	// A loader reporting ErrNotFound has the absence remembered, so a stream of
	// requests for something that does not exist stops reaching it.
	misses := 0
	ghosts := db.Aside(func(ctx context.Context, id string) (any, error) {
		misses++
		return nil, cache.ErrNotFound
	})
	var ghost user
	for range 3 {
		if err := ghosts.GetOrLoad(ctx, "nobody", &ghost); !errors.Is(err, cache.ErrNotFound) {
			return err
		}
	}
	log.Printf("aside: 3 reads of something absent, %d load(s)", misses)

	_ = ghosts.Invalidate(ctx, "nobody")
	_ = people.Invalidate(ctx, "carol")
	_ = db.Volatile.Delete(ctx, "session:abc")
	return db.Document.Delete(ctx, id)
}
