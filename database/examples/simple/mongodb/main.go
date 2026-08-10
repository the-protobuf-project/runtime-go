package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/mongodb"
)

// Book is the whole schema. Nothing else describes it.
type Book struct {
	ID      string    `db:"id,pk"`
	Title   string    `db:"title,unique"`
	Year    int32     `db:"published_year"`
	Cover   []byte    `db:"cover"`
	AddedAt time.Time `db:"added_at,autocreate"`
}

const dbName = "examples_simple_mongo"

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

	// One line binds the struct to a collection. The descriptor is derived once
	// and cached, so this costs nothing to call.
	books, err := database.Collection[Book](db, "books")
	if err != nil {
		return err
	}
	if err = books.EnsureSchema(ctx); err != nil {
		return err
	}
	log.Printf("on %s, storing %T", db.Backend, Book{})

	// --- write and read, as ordinary Go values ---
	id, err := books.Create(ctx, Book{
		ID: "books/dune", Title: "Dune", Year: 1965, Cover: []byte{0x00, 0xff},
	})
	if err != nil {
		return err
	}

	got, err := books.Get(ctx, id)
	if err != nil {
		return err
	}
	log.Printf("read back a %T: %s (%d), cover %v, added %s",
		got, got.Title, got.Year, got.Cover, got.AddedAt.Format(time.RFC3339))

	// The unique tag reached MongoDB and became an index.
	_, err = books.Create(ctx, Book{ID: "books/other", Title: "Dune", Year: 2021})
	log.Printf("duplicate title refused: %v", errors.Is(err, database.ErrAlreadyExists))

	// --- listing, filtered on the server ---
	for _, b := range []Book{
		{ID: "books/neuromancer", Title: "Neuromancer", Year: 1984},
		{ID: "books/blindsight", Title: "Blindsight", Year: 2006},
		{ID: "books/annihilation", Title: "Annihilation", Year: 2014},
	} {
		if _, cerr := books.Create(ctx, b); cerr != nil {
			return cerr
		}
	}

	recent, _, err := books.List(ctx,
		database.Where("published_year >= 1984"),
		database.OrderBy("published_year desc"),
		database.Page(10),
	)
	if err != nil {
		return err
	}
	log.Printf("published 1984 or later: %d", len(recent))
	for _, b := range recent {
		log.Printf("  %s (%d)", b.Title, b.Year)
	}

	total, err := books.Count(ctx)
	if err != nil {
		return err
	}
	log.Printf("%d books in total", total)

	// --- reaching past the simple layer ---
	// Anything this view does not cover takes the descriptor it derived, so a
	// caller is never stuck: the change stream below is the proto layer, over
	// the same rows.
	log.Printf("descriptor derived from the struct: %s -> table %q, key %q",
		books.Resource().Name, books.Resource().Table, books.Resource().PKColumn)

	return nil
}
