package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glebarez/sqlite"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/cache"
	cacheredis "github.com/the-protobuf-project/runtime-go/cache/redis"
	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/cached"
	"github.com/the-protobuf-project/runtime-go/database/examples/internal/model"
	"github.com/the-protobuf-project/runtime-go/database/orm"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// The store: SQLite, so this half needs nothing installed.
	sqlClient, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		TranslateError: true,
		Logger:         logger.Discard,
	})
	if err != nil {
		return err
	}
	store, err := orm.NewProvider(sqlClient).SetDatabase(ctx, "")
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// The cache: Redis. A TTL is not optional even though every write
	// invalidates — it covers what falls outside that, like another process
	// writing to the same database without this decorator in front of it.
	cacheClient, err := cacheredis.NewClient(ctx, cacheredis.Config{Address: "localhost:6379"})
	if err != nil {
		return err
	}
	defer func() { _ = cacheClient.Close() }()

	cp := cacheredis.New(cacheClient, cache.Config{
		Prefix:     "examples",
		DefaultTTL: 5 * time.Minute,
	})
	cdb, err := cp.SetDatabase(ctx, "records")
	if err != nil {
		return err
	}
	defer func() {
		_, _ = cp.DropDatabase(context.Background(), "records")
		_ = cdb.Close()
	}()

	// Count the reads that reach the store, which is the only number that says
	// whether the cache is doing anything. It goes *under* the cache: wrapping
	// the same cache database twice would give both decorators one shared
	// single-flight keyed by the same id, and the outer load would wait on an
	// inner one that had joined it — a self-deadlock broken only by the load
	// timeout.
	counted := &countingDriver{Driver: store.Driver}

	// The wiring. Wrap returns a DB, so every call site and the gRPC adapter
	// keep working, and the capabilities the store had come across with it.
	db := cached.Wrap(&database.DB{
		Driver:  counted,
		Tx:      store.Tx,
		Schema:  store.Schema,
		Graph:   store.Graph,
		Series:  store.Series,
		Backend: store.Backend,
		Name:    store.Name,
	}, cached.FromAside(cdb))
	log.Printf("on %s", db.Backend)

	books := model.BookResource()
	if err = db.Schema.EnsureSchema(ctx, books); err != nil {
		return err
	}
	if _, err = db.Create(ctx, books, model.Book("books/dune", "Dune", 1965)); err != nil {
		return err
	}

	// The seed write went through the counter too; start the measurement clean.
	counted.gets.Store(0)

	// --- a hot key under load ---
	// Concurrent misses on one key collapse into one load. That is the property
	// that makes a cache worth putting in front of a database: without it, a
	// popular record expiring means every in-flight request queries at once.
	const readers = 50
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = db.Get(ctx, books, "books/dune")
		}()
	}
	wg.Wait()
	log.Printf("%d concurrent readers reached the store %d time(s)", readers, counted.gets.Load())

	// --- a write is visible immediately ---
	// Every write invalidates what it wrote, so a reader never sees a value the
	// write did not observe.
	if _, err = db.Update(ctx, books, model.Book("books/dune", "Dune (revised)", 1965)); err != nil {
		return err
	}
	after, err := db.Get(ctx, books, "books/dune")
	if err != nil {
		return err
	}
	log.Printf("after the update: %s", model.Field(after, "title"))

	// --- an absence is remembered ---
	// Without this, requests for a record that does not exist reach the store
	// forever, which is exactly the traffic a scraper produces.
	counted.gets.Store(0)
	for range 10 {
		_, _ = db.Get(ctx, books, "books/ghost")
	}
	log.Printf("10 requests for a missing record reached the store %d time(s)", counted.gets.Load())

	return nil
}

// countingDriver counts reads that reach the store.
type countingDriver struct {
	database.Driver
	gets atomic.Int64
}

func (c *countingDriver) Get(ctx context.Context, res *database.Resource, key string) (proto.Message, error) {
	c.gets.Add(1)
	return c.Driver.Get(ctx, res, key)
}
