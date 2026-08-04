// Command cache demonstrates the runtime-go cache module against a live Redis.
//
//	docker compose -f ../../docker/compose.yaml up -d
//	go run ./example/cache
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/resourcename"
	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

type user struct {
	_    struct{} `resource:"//theprotobufproject.com/user/{name}"`
	Name string   `json:"name" resource:"name"`
	Age  int      `json:"age"`
}

func main() {
	ctx := context.Background()

	// The caller owns the connection: this program picks the address, the
	// database index, and when to close it. The cache never dials on its own.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis is not reachable: %v", err)
	}

	// A debug-level slog logger, bound with a component name so every record
	// this cache writes carries it.
	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.Level(telemetry.LevelDebug),
	}))).With(telemetry.Fields{"component": "cache"})

	// Prefix namespaces the keys, so several independent caches can share one
	// Redis database without colliding. The Logger here is for the driver's
	// internals — which key an ID was masked to, which stale members were
	// swept; WithLogging below adds a record per operation.
	c, err := cache.Redis(cache.RedisConfig{
		Client: rdb,
		Prefix: "example",
		Logger: logger,
	})
	if err != nil {
		log.Fatalf("Failed to build cache: %v", err)
	}

	// Cross-cutting behavior wraps any provider. Retry is innermost here, so
	// the telemetry timing covers the whole retried operation rather than each
	// attempt. Swap the order to measure attempts instead.
	//
	// telemetry.NoopMeter stands in for a real provider's Meter; nothing is
	// exported until one is wired in.
	cached := cache.Chain(c,
		cache.WithRetryMiddleware(3, 100*time.Millisecond),
		cache.WithLoggingMiddleware(logger),
		cache.WithTelemetryMiddleware(telemetry.NoopMeter),
	)

	// --- CREATE ---
	alice := user{Name: "Alice", Age: 30}

	// The ID is optional. Setting one lets a resource name act as the cache
	// key; leaving it empty has the provider generate a ULID.
	id, err := resourcename.MarshalResource(alice)
	if err != nil {
		log.Fatalf("Failed to build resource name: %v", err)
	}
	doc := cache.Document{Data: alice, TTL: 30 * time.Second}
	doc.SetID(id)

	created, err := cached.Create(ctx, doc)
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	log.Printf("created %s (TTL %v)", created.ID(), created.TTL)

	// --- GET ---
	got, err := cached.Get(ctx, created.ID())
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	log.Printf("got %s, %v remaining", got.ID(), got.TTL.Round(time.Second))

	var parsed user
	if perr := resourcename.UnmarshalResource(got.ID(), &parsed); perr != nil {
		log.Fatalf("Failed to parse resource name %q: %v", got.ID(), perr)
	}
	log.Printf("resource name parses back to %+v", parsed)

	// --- UPDATE ---
	if uerr := cached.Update(ctx, created.ID(), cache.Document{
		Data: user{Name: "Alice", Age: 31},
		TTL:  time.Minute,
	}); uerr != nil {
		log.Fatalf("Update: %v", uerr)
	}
	log.Println("updated")

	// --- LIST ---
	docs, err := cached.List(ctx)
	if err != nil {
		log.Fatalf("List: %v", err)
	}
	log.Printf("cache holds %d document(s)", len(docs))

	// --- DELETE ---
	if err := cached.Delete(ctx, created.ID()); err != nil {
		log.Fatalf("Delete: %v", err)
	}

	// A missing entry — deleted or expired — reports cache.ErrNotFound, which
	// matches through the generic interface as well as the provider.
	if _, err := cached.Get(ctx, created.ID()); errors.Is(err, cache.ErrNotFound) {
		log.Printf("%s is gone, as expected", created.ID())
	} else {
		log.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}
