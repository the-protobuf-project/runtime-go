// Package mongodb implements the backend-agnostic database.Driver over MongoDB.
//
// A resource is a collection, a record is a document, and the descriptor's
// columns are the document's fields — so what lands in MongoDB is queryable with
// the tools people already use, rather than a blob only this program can read.
// That is the reason to choose MongoDB over a key-value store, and encoding a
// message as one binary field would throw it away.
//
//	client, _ := mongodb.NewClient(ctx, mongodb.Config{Address: "localhost:27017"})
//	defer client.Close(ctx)
//
//	db, _ := mongodb.NewProvider(client).SetDatabase(ctx, "tenant_a")
//	defer db.Close()
//
//	db.Schema.EnsureSchema(ctx, res)   // the collection and its unique indexes
//	db.Create(ctx, res, book)
//
// # What it has
//
// All four capabilities: [database.Transactional], [database.Migrator],
// [database.Batcher] and [database.Watcher] — the last of which is the reason to
// reach for this backend when something has to react to writes, since the
// alternative without a change stream is polling.
//
// # It needs a replica set
//
// Transactions and change streams are both replica-set features; a standalone
// server has neither, and no amount of client-side work substitutes. A
// single-node replica set is enough, and is what this module's compose file
// runs so a program tested here behaves the same in production.
//
// # Two things the encoding gets right
//
// The primary key is stored as _id rather than in a field beside it, so there
// is one idea of identity rather than two that can disagree.
//
// A uint64 past 2^63 is stored as a Decimal128. BSON has no unsigned 64-bit
// integer, and handing one to the driver stores it as a negative int64 that
// reads back as a different number, silently. Decimal128 holds it exactly and
// stays numeric, so it still sorts and compares on the server.
//
// # Filtering is a small subset, deliberately
//
// [database.ListOptions.Filter] accepts conjunctions of `column op value` with
// = != > >= < <=, and refuses anything else by name. That is a fraction of
// AIP-160, and the fraction is the design: a backend that accepts the whole
// grammar and honors part of it returns the wrong records with nothing to say
// it ignored something.
package mongodb
