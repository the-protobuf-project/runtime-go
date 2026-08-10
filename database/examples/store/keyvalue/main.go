package main

import (
	"context"
	"errors"
	"log"

	goredis "github.com/redis/go-redis/v9"

	"github.com/the-protobuf-project/runtime-go/database/examples/store/internal/model"
	dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()

	// The name is a key segment rather than a Redis numbered database: those
	// are a property of the connection and there are only sixteen, which would
	// cap a multi-tenant program at sixteen tenants.
	p := dbredis.NewProvider(client, dbredis.WithPrefix("examples"))
	db, err := p.SetDatabase(ctx, "keyvalue")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s database %q", db.Backend, db.Name)

	books := model.BookResource()
	defer func() { _ = db.Schema.DropSchema(context.Background(), books) }()

	// EnsureSchema succeeds and does nothing: Redis has no schema, so a program
	// that migrates on startup runs unchanged here and against SQL. That is the
	// honest answer to "is what this descriptor describes ready to use".
	if err = db.Schema.EnsureSchema(ctx, books); err != nil {
		return err
	}

	// --- records ---
	for _, b := range []struct {
		id, title string
		year      int32
	}{
		{"books/dune", "Dune", 1965},
		{"books/neuromancer", "Neuromancer", 1984},
	} {
		if _, err = db.Create(ctx, books, model.Book(b.id, b.title, b.year)); err != nil {
			return err
		}
	}

	got, err := db.Get(ctx, books, "books/dune")
	if err != nil {
		return err
	}
	log.Printf("read: %s", model.Field(got, "title"))

	// Redis has no unique constraints, so the driver enforces the descriptor's
	// with a reservation key. A descriptor that claimed uniqueness the store did
	// not have would be the quiet lie the contract exists to avoid.
	_, err = db.Create(ctx, books, model.Book("books/other", "Dune", 2021))
	log.Printf("duplicate title refused: %v", errors.Is(err, store.ErrAlreadyExists))

	// --- what it cannot do, said plainly ---
	// No transactions: Redis has no rollback across the several keys one write
	// touches, so the capability is absent rather than approximated.
	err = db.Tx.Run(ctx, func(*store.DB) error { return nil })
	log.Printf("transactions: %v", err)

	// List pages and orders by key, and ignores a filter — Redis has no query
	// language, and filtering client-side would read the whole table to return
	// ten rows.
	page, err := db.List(ctx, books, store.ListOptions{PageSize: 10})
	if err != nil {
		return err
	}
	log.Printf("listed %d of %d", len(page.Items), page.Total)

	return nil
}
