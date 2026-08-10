// Package database stores Go structs in whichever backend you point it at.
//
//	type Book struct {
//	    ID    string `db:"id,pk"`
//	    Title string `db:"title,unique"`
//	    Year  int32  `db:"published_year"`
//	}
//
//	db, _ := mongodb.NewProvider(client).SetDatabase(ctx, "shop")
//	books, _ := database.Collection[Book](db, "books")
//
//	books.EnsureSchema(ctx)
//	id, _ := books.Create(ctx, Book{ID: "books/dune", Title: "Dune", Year: 1965})
//	b, _ := books.Get(ctx, id)
//
// The same code runs against PostgreSQL, MongoDB, ArangoDB, Redis and
// TimescaleDB — the only line that changes is the one that opens the client.
//
// # This package and store
//
// Underneath is [store], which is the same layer with nothing hidden: a Driver
// over proto messages, driven by a Resource descriptor that protorm generates.
// That is what the gRPC adapter and the chain drivers talk to, and what to reach
// for when the schema already comes from a proto file.
//
// This package derives that descriptor from a struct instead. Nothing else
// differs — a struct stored here and a message stored through [store] are the
// same rows in the same table, and the two can share a database. Reach past this
// package with [Coll.Resource] and [Coll.DB] whenever you need something it does
// not cover: a graph edge, a time-series window, a transaction.
//
// # What it costs
//
// A value written here is converted twice: from the struct into the column map
// the bridge uses, and from there into the proto message the driver takes. Both
// are in-memory reflection rather than round trips, and it buys one core rather
// than two implementations of every backend — but it is why a program whose
// schema is already proto should use [store] directly rather than this.
package database
