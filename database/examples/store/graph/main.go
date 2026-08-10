package main

import (
	"context"
	"errors"
	"log"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/the-protobuf-project/runtime-go/database/arangodb"
	"github.com/the-protobuf-project/runtime-go/database/examples/store/internal/model"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

const dbName = "examples_graph"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	client, err := arangodb.NewClient(ctx, arangodb.Config{
		Endpoints: []string{"http://localhost:8529"},
		Username:  "root",
		Password:  "root",
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	// The registry is what the graph half needs. A Ref carries a resource name
	// rather than a collection, because that is the only identifier portable to
	// a backend where collections do not exist — so turning one back into a
	// collection means holding the registry the program already has.
	p := arangodb.NewProvider(client, arangodb.WithRegistry(model.Registry()))

	if err = p.EnsureDatabase(ctx, dbName); err != nil {
		return err
	}
	defer func() { _ = p.DropDatabase(context.Background(), dbName) }()

	db, err := p.SetDatabase(ctx, dbName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s database %q", db.Backend, db.Name)

	books, authors, wrote := model.BookResource(), model.AuthorResource(), model.WroteResource()
	for _, res := range []*store.Resource{books, authors, wrote} {
		if err = db.Schema.EnsureSchema(ctx, res); err != nil {
			return err
		}
	}

	// --- records, exactly as on any other backend ---
	if _, err = db.Create(ctx, authors, model.Author("authors/herbert", "Frank Herbert")); err != nil {
		return err
	}
	for _, b := range []struct {
		id, title string
		year      int32
	}{
		{"books/dune", "Dune", 1965},
		{"books/messiah", "Dune Messiah", 1969},
	} {
		if _, err = db.Create(ctx, books, model.Book(b.id, b.title, b.year)); err != nil {
			return err
		}
	}

	herbert := store.Ref{Resource: "Author", Key: "authors/herbert"}

	// --- the second half: connections ---
	// A record and the edges that join it, in one transaction. This is why
	// Tx.Run hands over a whole DB: on a backend that is both a record store and
	// a graph, creating a thing and linking it is one operation or it is
	// nothing.
	err = db.Tx.Run(ctx, func(tx *store.DB) error {
		for _, id := range []string{"books/dune", "books/messiah"} {
			if _, err = tx.Graph.Connect(ctx, wrote,
				herbert, store.Ref{Resource: "Book", Key: id},
				model.Wrote("author")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// One hop: what did this author write?
	edges, err := db.Graph.Neighbors(ctx, herbert, store.TraverseOptions{
		Types: []string{"Wrote"}, Direction: store.Outbound, WithProps: true,
	})
	if err != nil {
		return err
	}
	log.Printf("%s wrote %d books", model.Field(mustGet(ctx, db, model.AuthorResource(), "authors/herbert"), "name"), len(edges))
	for _, e := range edges {
		log.Printf("  -> %s as %s", e.To.Key, model.Field(e.Props, "role"))
	}

	// The walk returns refs; loading the records they point at is a separate
	// call, because "what is connected to this" is one query and "load
	// everything it is connected to" is one more per record.
	titles, err := store.Resolve[*dynamicpb.Message](ctx, db, model.Registry(), store.Ends(edges))
	if err != nil {
		return err
	}
	for _, t := range titles {
		log.Printf("  resolved: %s", model.Field(t, "title"))
	}

	// Inbound answers the other direction with the same edges.
	back, err := db.Graph.Neighbors(ctx, store.Ref{Resource: "Book", Key: "books/dune"},
		store.TraverseOptions{Types: []string{"Wrote"}, Direction: store.Inbound})
	if err != nil {
		return err
	}
	log.Printf("Dune was written by %d author(s)", len(back))

	// An edge to something that is not there is refused rather than stored:
	// a dangling edge surfaces much later as a traversal returning nothing for
	// a record that is plainly present.
	_, err = db.Graph.Connect(ctx, wrote, herbert,
		store.Ref{Resource: "Book", Key: "books/ghost"}, nil)
	log.Printf("edge to a missing record: %v", errors.Is(err, store.ErrNotFound))

	return nil
}

func mustGet(ctx context.Context, db *store.DB, res *store.Resource, key string) proto.Message {
	msg, err := db.Get(ctx, res, key)
	if err != nil {
		return nil
	}
	return msg
}
