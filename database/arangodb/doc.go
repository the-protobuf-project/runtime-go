// Package arangodb implements the backend-agnostic store.Driver over
// ArangoDB, and the graph capability alongside it.
//
// A resource is a collection, a record is a document, and the descriptor's
// columns are the document's fields — the same layout the MongoDB driver uses,
// so a program moving between them finds its data where it left it. What
// ArangoDB adds is the second half: the same server holds the edges between
// those documents and walks them, which is the reason to choose it over a
// document store that only holds the documents.
//
//	client, _ := arangodb.NewClient(ctx, arangodb.Config{
//	    Endpoints: []string{"http://localhost:8529"},
//	    Username:  "root", Password: "…",
//	})
//	defer client.Close()
//
//	p := arangodb.NewProvider(client, arangodb.WithRegistry(reg))
//	db, _ := p.SetDatabase(ctx, "tenant_a")
//	defer db.Close()
//
//	db.Schema.EnsureSchema(ctx, userRes)
//	db.Create(ctx, userRes, alice)
//	db.Graph.Connect(ctx, memberRes, aliceRef, acmeRef, nil)
//
// # What it has
//
// [store.Transactional], [store.Migrator], [store.Batcher],
// [store.Graph] and [store.GraphMigrator]. No [store.Watcher]: ArangoDB
// has a write-ahead-log tail, but it reports raw operations against collections
// rather than changes to records, and adapting it would mean guessing which
// resource a document belonged to. The capability is absent rather than
// approximated.
//
// # The registry
//
// Graph operations need one, record operations do not. A [store.Ref] carries
// a resource name rather than a collection, because that is the only identifier
// portable to Neo4j — so turning one back into a collection means holding the
// registry the rest of the program already has. Supply it with [WithRegistry];
// a program that only stores records never needs to.
//
// # Keys
//
// A caller's id is encoded before it becomes a document key, and decoded on the
// way back. Two constraints force it: a key may not contain a slash, and AIP
// resource names routinely do; and a read passes the key in a URL path that the
// driver does not escape again, so percent-encoding — the obvious answer — makes
// every write succeed and every read after it miss. See keys.go for the scheme
// and why the escape character is the dot.
//
// # Transactions declare every collection
//
// ArangoDB wants to know which collections a transaction will touch before it
// starts, and this contract does not — the body decides as it runs. So [Driver.Run]
// declares them all as writable. On RocksDB, which is the only engine modern
// ArangoDB has, a declared collection is locked when it is first written rather
// than when the transaction begins, so declaring one the body never touches
// costs nothing.
//
// # Filtering is a small subset, deliberately
//
// [store.ListOptions.Filter] accepts conjunctions of `column op value` with
// = != > >= < <=, bound rather than interpolated, and refuses anything else by
// name. AQL could express far more; a filter that arrives from a request and
// reaches a query is not the place to find that out.
package arangodb
