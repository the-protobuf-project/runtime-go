package main

import (
	"context"
	"log"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/dragonfly"
)

// withDragonfly is the Redis flow with one word changed.
//
// That is the whole demonstration. Dragonfly speaks the same protocol, so it
// runs on the same driver and gets the same five capabilities — every strategy
// works, including the secondary index and the fenced read-through lock, with no
// Dragonfly-specific code anywhere in this module beyond a name and a default
// port.
func withDragonfly(ctx context.Context) error {
	client, err := dragonfly.NewClient(ctx, dragonfly.Config{
		Address:  "localhost:6380",
		Database: 1,
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	c := dragonfly.New(client, cache.Config{
		Prefix:       "example",
		DefaultTTL:   time.Minute,
		DefaultStale: 30 * time.Second,
	})

	db, err := c.SetDatabase(ctx, 2)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s database %d", db.Backend, db.Index)

	alice := user{Name: "Alice", Email: "alice@example.com", Age: 30}

	// Indexed: the capability memcached cannot offer, working here because
	// Dragonfly has sets like any RESP server.
	id, err := db.Indexed.Create(ctx, "", alice,
		cache.TTL(time.Hour),
		cache.Index("email", alice.Email),
		cache.Index("tenant", "acme"),
	)
	if err != nil {
		return err
	}

	var byEmail []user
	if ierr := db.Indexed.ByIndex(ctx, "email", alice.Email, &byEmail); ierr != nil {
		return ierr
	}
	log.Printf("indexed: %d match(es) for %s", len(byEmail), alice.Email)

	ttl, err := db.Indexed.TTL(ctx, id)
	if err != nil {
		return err
	}
	log.Printf("document: %v left on %s", ttl.Round(time.Second), id)

	// Read-through, with the cross-process lock Dragonfly's scripting supports.
	loads := 0
	people := db.Aside(func(ctx context.Context, id string) (any, error) {
		loads++
		return user{Name: "Bob"}, nil
	})
	var bob user
	for range 3 {
		if gerr := people.GetOrLoad(ctx, "bob", &bob); gerr != nil {
			return gerr
		}
	}
	log.Printf("aside: 3 reads, %d load(s)", loads)

	dropped, err := db.Indexed.DeleteByIndex(ctx, "tenant", "acme")
	if err != nil {
		return err
	}
	log.Printf("indexed: dropped %d entr(ies) for one tenant", dropped)
	return people.Invalidate(ctx, "bob")
}
