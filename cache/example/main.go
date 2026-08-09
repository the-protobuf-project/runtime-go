// Command example runs the same three steps against every backend: Redis,
// Dragonfly and memcached.
//
// Note what is not in the imports below. No driver, and no alias — a program
// that caches names the cache and the backend it wants, and nothing else.
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/redis"
)

// user is this program's model. Nothing in the cache knows about it — that is
// the point of there being no document type: adding a field here changes nothing
// downstream.
type user struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func main() {
	ctx := context.Background()

	if err := withRedis(ctx); err != nil {
		log.Printf("redis: %v", err)
	}
	if err := withDragonfly(ctx); err != nil {
		log.Printf("dragonfly: %v", err)
	}
	if err := withMemcached(ctx); err != nil {
		log.Printf("memcached: %v", err)
	}
}

func withRedis(ctx context.Context) error {
	// Step one: the client, from the provider package. It is yours — hand the
	// same one to the database and streams layers and all three share a pool.
	client, err := redis.NewClient(ctx, redis.Config{
		Address:  "localhost:6379",
		Database: 1,
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	// Step two: declare the cache. Nothing is reachable yet — no database has
	// been chosen, so there is nothing to read or write.
	c := redis.New(client, cache.Config{Prefix: "example", DefaultTTL: time.Minute})

	// Step three: choose the database. This one is the client's own, so nothing
	// is derived and db.Close is a no-op.
	db, err := c.SetDatabase(ctx, 1)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s database %d", db.Backend, db.Index)

	// Asking for another index means a second client, derived from yours and
	// owned by this DB. Yours is untouched; this one must be closed.
	other, err := c.SetDatabase(ctx, 4)
	if err != nil {
		return err
	}
	defer func() { _ = other.Close() }()
	log.Printf("also on database %d, over a derived client", other.Index)

	// --- DOCUMENT: whole values you will enumerate ---
	alice := user{Name: "Alice", Email: "alice@example.com", Age: 30}

	id, err := db.Document.Create(ctx, "", alice, cache.TTL(30*time.Second))
	if err != nil {
		return err
	}
	var got user
	if gerr := db.Document.Get(ctx, id, &got); gerr != nil {
		return gerr
	}
	ttl, err := db.Document.TTL(ctx, id)
	if err != nil {
		return err
	}
	log.Printf("document: %+v, %v left", got, ttl.Round(time.Second))

	var everyone []user
	if lerr := db.Document.List(ctx, &everyone); lerr != nil { // sweeps as it reads
		return lerr
	}
	log.Printf("document: holds %d entr(ies)", len(everyone))

	// --- VOLATILE: keys you already have, and a lease ---
	// No index, so a write is one round trip and expiry costs nothing.
	if serr := db.Volatile.Set(ctx, "session:abc", alice, cache.TTL(15*time.Minute)); serr != nil {
		return serr
	}
	// A sliding window extends the lease without resending the value.
	if terr := db.Volatile.Touch(ctx, "session:abc", 20*time.Minute); terr != nil {
		return terr
	}
	if left, terr := db.Volatile.TTL(ctx, "session:abc"); terr == nil {
		log.Printf("volatile: session has %v left", left.Round(time.Second))
	}

	// --- INDEXED: found by something other than its id ---
	if _, cerr := db.Indexed.Create(ctx, "", alice,
		cache.TTL(time.Hour),
		cache.Index("email", alice.Email),
		cache.Index("tenant", "acme"),
	); cerr != nil {
		return cerr
	}

	// The lookup a cache usually cannot answer: the caller has the address, not
	// the id.
	var byEmail []user
	if ierr := db.Indexed.ByIndex(ctx, "email", alice.Email, &byEmail); ierr != nil {
		return ierr
	}
	log.Printf("indexed: %d match(es) for %s", len(byEmail), alice.Email)

	// And invalidation by group, which is what the index buys even when nothing
	// queries by field.
	dropped, err := db.Indexed.DeleteByIndex(ctx, "tenant", "acme")
	if err != nil {
		return err
	}
	log.Printf("indexed: dropped %d entr(ies) for one tenant", dropped)

	// --- ASIDE: read-through over a loader you supply ---
	// The loader is yours, so one database can have several — one per kind of
	// thing being cached.
	loads := 0
	people := db.Aside(func(ctx context.Context, id string) (any, error) {
		loads++ // the real read would be a database, an HTTP call, a computation
		return user{Name: "Bob", Email: id + "@example.com"}, nil
	})

	var bob user
	for range 3 {
		if err := people.GetOrLoad(ctx, "bob", &bob, cache.TTL(time.Minute)); err != nil {
			return err
		}
	}
	log.Printf("aside: 3 reads, %d load(s)", loads)

	// Stale serving: fresh for 100ms, servable for a minute after that. The read
	// below happens once the entry is stale, and returns the old value without
	// waiting — the refresh runs behind it.
	if err := people.GetOrLoad(ctx, "carol", &bob,
		cache.TTL(100*time.Millisecond), cache.Stale(time.Minute)); err != nil {
		return err
	}
	time.Sleep(150 * time.Millisecond)

	start := time.Now()
	if err := people.GetOrLoad(ctx, "carol", &bob,
		cache.TTL(100*time.Millisecond), cache.Stale(time.Minute)); err != nil {
		return err
	}
	log.Printf("aside: stale read served in %v, refreshing behind it", time.Since(start).Round(time.Microsecond))

	// For a value you know just changed, load over it rather than dropping it —
	// deleting leaves a window where every reader misses at once.
	if err := people.Refresh(ctx, "bob"); err != nil {
		return err
	}

	// --- MISSES ---
	// A miss is a normal cache outcome, not a failure.
	if err := db.Document.Get(ctx, "never-written", &got); errors.Is(err, cache.ErrNotFound) {
		log.Println("document: miss, as expected")
	}
	// Deleting what is already gone is not an error — the intent is met, and an
	// entry may legitimately have expired a moment earlier.
	if err := db.Document.Delete(ctx, id); err != nil {
		return err
	}
	if err := db.Document.Delete(ctx, id); err != nil {
		return err
	}
	return people.Invalidate(ctx, "bob")
}
