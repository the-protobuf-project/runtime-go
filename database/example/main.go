package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/database"
	dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// book is this program's model.
type book struct {
	Title  string `json:"title"`
	Author string `json:"author"`
	Year   int    `json:"year"`
}

func main() {
	ctx := context.Background()

	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(telemetry.LevelDebug)})))

	// You own the connection.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
		DB:   2,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis is not reachable: %v", err)
	}

	// The one provider-specific line.
	db := dbredis.Connect(rdb,
		dbredis.WithPrefix("example"),
		dbredis.WithLogger(logger),
	)

	db = database.Chain(db,
		database.WithRetryMiddleware(3, 100*time.Millisecond),
		database.WithLoggingMiddleware(logger),
		database.WithTelemetryMiddleware(telemetry.NoopMeter),
	)

	// Start from a clean slate so re-runs behave the same.
	if ids, err := db.Keys(ctx); err == nil {
		for _, id := range ids {
			_ = db.Delete(ctx, id)
		}
	}

	// --- CREATE ---
	dune := book{Title: "Dune", Author: "Herbert", Year: 1965}

	id, err := db.Create(ctx, "", dune)
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	log.Printf("stored %s", id)

	// --- DEDUPLICATION ---
	// The store is content-addressed: writing content that already exists
	// returns the id holding it rather than storing a second copy. Field order
	// is not content, so this deduplicates even though the literal differs.
	same, err := db.Create(ctx, "", book{Year: 1965, Title: "Dune", Author: "Herbert"})
	if err != nil {
		log.Fatalf("Create (duplicate): %v", err)
	}
	if same == id {
		log.Printf("identical content deduplicated to %s", same)
	} else {
		log.Fatalf("expected deduplication, got a second record %s", same)
	}

	// --- GET ---
	var got book
	if gerr := db.Get(ctx, id, &got); gerr != nil {
		log.Fatalf("Get: %v", gerr)
	}
	log.Printf("got %+v", got)

	// --- UPDATE ---
	// Updating moves the content index: the old content becomes storable again,
	// and the new content is now claimed by this record. Updating to content
	// another record already holds is refused with ErrDuplicate.
	if uerr := db.Update(ctx, id, book{Title: "Dune", Author: "Frank Herbert", Year: 1965}); uerr != nil {
		log.Fatalf("Update: %v", uerr)
	}
	log.Println("updated")

	// --- TYPED VIEW + PAGING ---
	shelf := database.For[book](db)

	for _, b := range []book{
		{Title: "Neuromancer", Author: "Gibson", Year: 1984},
		{Title: "Snow Crash", Author: "Stephenson", Year: 1992},
	} {
		if _, cerr := shelf.Create(ctx, b); cerr != nil {
			log.Fatalf("typed Create: %v", cerr)
		}
	}

	// Records come back in a stable order, so Limit and Offset page
	// predictably — without one, successive pages could overlap or skip.
	page, err := shelf.List(ctx, database.Limit(2))
	if err != nil {
		log.Fatalf("typed List: %v", err)
	}
	log.Printf("first page holds %d book(s)", len(page))

	ids, err := db.Keys(ctx)
	if err != nil {
		log.Fatalf("Keys: %v", err)
	}
	log.Printf("store holds %d record(s) in total", len(ids))

	// --- DELETE ---
	// Deleting releases the content, so the same value can be stored again.
	if derr := db.Delete(ctx, id); derr != nil {
		log.Fatalf("Delete: %v", derr)
	}

	// Unlike a cache, a missing record is a genuine surprise: records do not
	// expire on their own, so asking to delete one twice is reported.
	if derr := db.Delete(ctx, id); errors.Is(derr, database.ErrNotFound) {
		log.Printf("%s is gone, as expected", id)
	}

	for _, leftover := range ids {
		_ = db.Delete(ctx, leftover)
	}
	log.Println("done")
}
