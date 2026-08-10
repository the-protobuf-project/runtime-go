package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/orm"
)

// The same struct the mongodb example stores, unchanged.
type Book struct {
	ID      string    `db:"id,pk"`
	Title   string    `db:"title,unique"`
	Year    int32     `db:"published_year"`
	Cover   []byte    `db:"cover"`
	AddedAt time.Time `db:"added_at,autocreate"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// The only backend-specific lines in the file.
	client, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		TranslateError: true, Logger: logger.Discard,
	})
	if err != nil {
		return err
	}
	db, err := orm.NewProvider(client).SetDatabase(ctx, "")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Everything from here down is identical to the mongodb example.
	books, err := database.Collection[Book](db, "books")
	if err != nil {
		return err
	}
	if err = books.EnsureSchema(ctx); err != nil {
		return err
	}
	log.Printf("on %s, storing %T", db.Backend, Book{})

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
	log.Printf("read back a %T: %s (%d), cover %v", got, got.Title, got.Year, got.Cover)

	_, err = books.Create(ctx, Book{ID: "books/other", Title: "Dune", Year: 2021})
	log.Printf("duplicate title refused: %v", errors.Is(err, database.ErrAlreadyExists))

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

	// The same transaction shape, reached through the database underneath.
	err = db.Tx.Run(ctx, func(tx *database.DB) error {
		inTx, cerr := database.Collection[Book](tx, "books")
		if cerr != nil {
			return cerr
		}
		_, cerr = inTx.Create(ctx, Book{ID: "books/tx", Title: "Written in a transaction", Year: 2026})
		return cerr
	})
	if err != nil {
		return err
	}
	total, err := books.Count(ctx)
	if err != nil {
		return err
	}
	log.Printf("%d books after the transaction committed", total)

	return nil
}
