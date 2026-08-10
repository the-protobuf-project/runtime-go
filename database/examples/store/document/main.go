package main

import (
	"context"
	"log"
	"time"

	"github.com/the-protobuf-project/runtime-go/database/examples/store/internal/model"
	"github.com/the-protobuf-project/runtime-go/database/mongodb"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

const dbName = "examples_document"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	client, err := mongodb.NewClient(ctx, mongodb.Config{
		Address:        "localhost:27017",
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close(context.Background()) }()

	p := mongodb.NewProvider(client)
	db, err := p.SetDatabase(ctx, dbName)
	if err != nil {
		return err
	}
	defer func() {
		_ = p.DropDatabase(context.Background(), dbName)
		_ = db.Close()
	}()
	log.Printf("on %s database %q", db.Backend, db.Name)

	books := model.BookResource()
	if err = db.Schema.EnsureSchema(ctx, books); err != nil {
		return err
	}

	// --- watch, before writing ---
	// A change stream reports writes as they happen. The alternative without
	// one is polling, which turns a single slow query into a permanent load
	// floor. It needs a replica set — the compose file runs one for exactly
	// this reason.
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()

	changes, err := db.Driver.(store.Watcher).Watch(watchCtx, books, store.WatchOptions{})
	if err != nil {
		log.Printf("change streams unavailable: %v", err)
	} else {
		go func() {
			for c := range changes {
				log.Printf("  observed %v on %s", kind(c.Kind), c.Key)
			}
		}()
	}

	// --- write ---
	for _, b := range []struct {
		id, title string
		year      int32
	}{
		{"books/dune", "Dune", 1965},
		{"books/neuromancer", "Neuromancer", 1984},
		{"books/blindsight", "Blindsight", 2006},
	} {
		if _, err = db.Create(ctx, books, model.Book(b.id, b.title, b.year)); err != nil {
			return err
		}
	}

	// --- the server does the filtering ---
	// A page size means what it says: the filter runs in MongoDB, not here, so
	// asking for ten rows reads ten rows.
	recent, err := db.List(ctx, books, store.ListOptions{
		Filter:  "published_year >= 1984",
		OrderBy: "published_year desc",
	})
	if err != nil {
		return err
	}
	log.Printf("published 1984 or later: %d of %d", len(recent.Items), recent.Total)
	for _, m := range recent.Items {
		log.Printf("  %s (%s)", model.Field(m, "title"), model.Field(m, "published_year"))
	}

	// A filter this backend cannot honor is refused rather than half-applied.
	_, err = db.List(ctx, books, store.ListOptions{Filter: "title LIKE Dune"})
	log.Printf("unsupported filter: %v", err)

	// --- bulk ---
	batcher := db.Driver.(store.Batcher)
	got, err := batcher.GetMany(ctx, books, []string{"books/dune", "books/missing", "books/blindsight"})
	if err != nil {
		return err
	}
	log.Printf("bulk read: %s, %v, %s",
		model.Field(got[0], "title"), got[1] == nil, model.Field(got[2], "title"))

	// Give the watcher a moment to report what it saw before the program ends.
	time.Sleep(500 * time.Millisecond)
	return nil
}

func kind(k store.ChangeKind) string {
	switch k {
	case store.ChangeCreated:
		return "a create"
	case store.ChangeUpdated:
		return "an update"
	case store.ChangeDeleted:
		return "a delete"
	default:
		return "a change"
	}
}
