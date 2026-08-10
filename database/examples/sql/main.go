package main

import (
	"context"
	"errors"
	"log"

	"github.com/glebarez/sqlite"
	"google.golang.org/protobuf/types/dynamicpb"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database"
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

	// Step one: the client. It is yours — this package opens it, and closing it
	// stays your job. Hand the same one to several providers and they share a
	// pool.
	//
	// TranslateError is what turns a driver's duplicate-key error into the
	// sentinel the contract promises; without it every backend reports
	// conflicts differently and nothing above can tell them apart.
	client, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		TranslateError: true,
		Logger:         logger.Discard,
	})
	if err != nil {
		return err
	}

	// Step two and three: declare the provider, then select a database. Nothing
	// is reachable until a database is chosen, so there is no default nobody
	// picked to write to by accident.
	db, err := orm.NewProvider(client).SetDatabase(ctx, "")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s", db.Backend)

	books := model.BookResource()

	// The schema comes from the descriptor. Nobody writes the DDL twice.
	if err = db.Schema.EnsureSchema(ctx, books); err != nil {
		return err
	}

	// --- records ---
	if _, err = db.Create(ctx, books, model.Book("books/dune", "Dune", 1965)); err != nil {
		return err
	}
	got, err := db.Get(ctx, books, "books/dune")
	if err != nil {
		return err
	}
	log.Printf("read: %s (%s)", model.Field(got, "title"), model.Field(got, "published_year"))

	// The descriptor marks the title unique, and the store enforces it.
	_, err = db.Create(ctx, books, model.Book("books/other", "Dune", 2021))
	log.Printf("duplicate title: %v", errors.Is(err, database.ErrAlreadyExists))

	// --- transactions ---
	// Run hands over a whole DB rather than only the CRUD half, so a body that
	// needs more than records has it. Here that is two writes that must both
	// land or neither.
	err = db.Tx.Run(ctx, func(tx *database.DB) error {
		if _, cerr := tx.Create(ctx, books, model.Book("books/a", "Ancillary Justice", 2013)); cerr != nil {
			return cerr
		}
		_, cerr := tx.Create(ctx, books, model.Book("books/b", "Blindsight", 2006))
		return cerr
	})
	if err != nil {
		return err
	}

	// A rollback takes the whole body with it.
	sentinel := errors.New("changed my mind")
	_ = db.Tx.Run(ctx, func(tx *database.DB) error {
		if _, err = tx.Create(ctx, books, model.Book("books/c", "Never Stored", 2020)); err != nil {
			return err
		}
		return sentinel
	})
	stored, err := db.Count(ctx, books, database.ListOptions{})
	if err != nil {
		return err
	}
	log.Printf("after one commit and one rollback: %d books", stored)

	// --- typed view ---
	// For binds the driver and the descriptor to one message type, so the
	// descriptor stops being an argument on every call and a read comes back
	// already typed. Generated code would name a *pb.Book here; these examples
	// build their messages dynamically, so the type is dynamicpb's.
	typed, err := database.For[*dynamicpb.Message](db, books)
	if err != nil {
		return err
	}
	page, _, total, err := typed.List(ctx, database.ListOptions{PageSize: 10})
	if err != nil {
		return err
	}
	log.Printf("listed %d of %d", len(page), total)

	// --- what SQL cannot do ---
	// A capability the backend lacks refuses by name rather than panicking or
	// pretending.
	_, err = db.Graph.Neighbors(ctx, database.Ref{Resource: "Book", Key: "books/dune"},
		database.TraverseOptions{MaxDepth: 1})
	log.Printf("graph on SQL: %v", err)

	return nil
}
