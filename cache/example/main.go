package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache"
	cacheredis "github.com/the-protobuf-project/runtime-go/cache/redis"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// user is this program's model. Nothing in the cache knows about it — that is
// the point of there being no document type: adding a field here changes
// nothing downstream.
type user struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func main() {
	ctx := context.Background()

	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(telemetry.LevelDebug)})))

	// You own the connection: the address, the database index, the pool, and
	// when it closes. The cache never dials and never closes it.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis is not reachable: %v", err)
	}

	// The one provider-specific line. Swap it for cachememcached.Connect(mc)
	// and nothing below changes.
	c := cacheredis.Connect(rdb,
		cacheredis.WithPrefix("example"),
		cacheredis.WithLogger(logger),
	)

	// Cross-cutting behavior wraps any provider.
	c = cache.Chain(c,
		cache.WithRetryMiddleware(3, 100*time.Millisecond),
		cache.WithLoggingMiddleware(logger),
		cache.WithTelemetryMiddleware(telemetry.NoopMeter),
	)

	// --- CREATE ---
	// An empty id has the provider generate one and hand it back.
	alice := user{Name: "Alice", Email: "alice@example.com", Age: 30}

	id, err := c.Create(ctx, "", alice, cache.TTL(30*time.Second))
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	log.Printf("created %s", id)

	// --- GET ---
	// The value decodes into a destination you own.
	var got user
	if gerr := c.Get(ctx, id, &got); gerr != nil {
		log.Fatalf("Get: %v", gerr)
	}
	ttl, _ := c.TTL(ctx, id)
	log.Printf("got %+v (%v remaining)", got, ttl.Round(time.Second))

	// --- UPDATE ---
	// Update refuses to resurrect an entry that is gone, so this reports a miss
	// rather than quietly creating one.
	if uerr := c.Update(ctx, id, user{Name: "Alice", Age: 31}, cache.TTL(time.Minute)); uerr != nil {
		log.Fatalf("Update: %v", uerr)
	}
	log.Println("updated")

	// --- TYPED VIEW ---
	// A wrapper over the same handler, not a second client — one provider
	// serves every model in the program.
	users := cache.For[user](c)

	bobID, err := users.Create(ctx, user{Name: "Bob", Age: 25}, cache.TTL(time.Minute))
	if err != nil {
		log.Fatalf("typed Create: %v", err)
	}
	bob, err := users.Get(ctx, bobID) // returns a user; no decoding here
	if err != nil {
		log.Fatalf("typed Get: %v", err)
	}
	log.Printf("typed view read %+v", bob)

	// --- LIST ---
	// Reads sweep entries that expired since the index was written, so the
	// index cannot grow without bound in a cache with short TTLs.
	all, err := users.List(ctx)
	if err != nil {
		log.Fatalf("List: %v", err)
	}
	log.Printf("cache holds %d entr(ies)", len(all))

	// --- DELETE ---
	if derr := c.Delete(ctx, id); derr != nil {
		log.Fatalf("Delete: %v", derr)
	}
	// Deleting something already gone is not an error — the intent is met, and
	// an entry may legitimately have expired a moment earlier.
	if derr := c.Delete(ctx, id); derr != nil {
		log.Fatalf("second Delete should be a no-op: %v", derr)
	}

	// A miss is a normal cache outcome, reported as ErrNotFound.
	if gerr := c.Get(ctx, id, &got); errors.Is(gerr, cache.ErrNotFound) {
		log.Printf("%s is gone, as expected", id)
	}

	_ = c.Delete(ctx, bobID)
	log.Println("done")
}
