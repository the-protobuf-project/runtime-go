// Command store demonstrates the runtime-go database module against a live
// Redis, including its content-addressed deduplication.
//
//	docker compose -f ../../../cache/docker/compose.yaml up -d
//	go run ./example/store
package main

import (
	"context"
	"errors"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/database"
	dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

func main() {
	ctx := context.Background()

	// The caller owns the connection.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
		DB:   6,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis is not reachable: %v", err)
	}

	s, err := dbredis.New(dbredis.Config{Client: rdb, Prefix: "example"})
	if err != nil {
		log.Fatalf("Failed to build store: %v", err)
	}

	db := database.Chain(s,
		database.WithRetryMiddleware(3, 100*time.Millisecond),
		database.WithTelemetryMiddleware(telemetry.NoopMeter),
	)

	// --- CREATE ---
	book := map[string]any{"title": "Dune", "author": "Herbert", "year": 1965}

	created, err := db.Create(ctx, database.Document{Data: book})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	log.Printf("created %s", created.ID())

	// --- DEDUPLICATION ---
	// The store is content-addressed, so writing identical content resolves to
	// the document that already holds it rather than storing a second copy.
	// Key order does not matter: the payload is canonicalized before hashing.
	same, err := db.Create(ctx, database.Document{
		Data: map[string]any{"year": 1965, "title": "Dune", "author": "Herbert"},
	})
	if err != nil {
		log.Fatalf("Create (duplicate): %v", err)
	}
	if same.ID() == created.ID() {
		log.Printf("identical content deduplicated to %s", same.ID())
	} else {
		log.Fatalf("expected deduplication, got a second document %s", same.ID())
	}

	// --- GET ---
	got, err := db.Get(ctx, created.ID())
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	log.Printf("got %s -> %v", got.ID(), got.Data)

	// --- UPDATE ---
	// Updating moves the content index: the old content becomes storable again
	// and the new content is now claimed by this document.
	if uerr := db.Update(ctx, created.ID(), database.Document{
		Data: map[string]any{"title": "Dune", "author": "Herbert", "year": 1966},
	}); uerr != nil {
		log.Fatalf("Update: %v", uerr)
	}
	log.Println("updated")

	// --- LIST ---
	// Results come back sorted by ID, so Limit and Offset page predictably.
	docs, err := db.List(ctx, database.Query{Limit: 10})
	if err != nil {
		log.Fatalf("List: %v", err)
	}
	log.Printf("store holds %d document(s)", len(docs))

	// --- DELETE ---
	if derr := db.Delete(ctx, created.ID()); derr != nil {
		log.Fatalf("Delete: %v", derr)
	}

	// Unlike a cache, a missing record is reported rather than shrugged off:
	// documents never expire on their own.
	if _, gerr := db.Get(ctx, created.ID()); errors.Is(gerr, database.ErrNotFound) {
		log.Printf("%s is gone, as expected", created.ID())
	} else {
		log.Fatalf("expected ErrNotFound after delete, got %v", gerr)
	}
}
