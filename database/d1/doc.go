// Package d1 implements the backend-agnostic store.Driver over Cloudflare D1,
// through the official cloudflare-go SDK.
//
// D1 is SQLite served over HTTP. A program running outside a Worker reaches it
// through the REST API's query endpoint, one statement — or one batch of them —
// per request, which shapes everything below:
//
//	client := cloudflare.NewClient(option.WithAPIToken(os.Getenv("CLOUDFLARE_API_TOKEN")))
//	p := d1.NewProvider(d1.Config{Client: client, AccountID: account, DatabaseID: id})
//	db, _ := p.SetDatabase(ctx, "")         // the configured database
//	tenant, _ := p.SetDatabase(ctx, "tenant_a") // the D1 database named tenant_a
//
//	db.Schema.EnsureSchema(ctx, res)
//	id, _ := db.Create(ctx, res, book)
//
// # What a database name means
//
// SQLite has no schemas, so a name cannot qualify a table the way it does on
// PostgreSQL. It names a D1 database instead: [Provider.SetDatabase] looks the
// name up in the account and drives that database, which is what lets one set
// of generated descriptors serve a database per tenant. Creating the database is
// the caller's job — wrangler d1 create, or the SDK — because creating one is an
// account change, not a migration.
//
// # How values travel
//
// The query endpoint binds parameters as strings, so every value is written as
// text and SQLite's column affinity turns it back into what the column holds: an
// INTEGER column stores 42, not '42'. The types are therefore this driver's to
// choose, and [store.Column.SQLType] is not honored — it was written for a
// server whose values arrive typed. Bytes are stored base64 in a TEXT column,
// because a blob cannot be bound as a string; timestamps are stored as
// fixed-width RFC 3339 text in UTC, so ordering and comparing them as text is
// ordering and comparing them as times.
//
// Results come back as JSON. An integer past 2^53 does not survive that
// exchange intact — D1 itself serves it through JavaScript — so a counter that
// large wants a string field.
//
// # What D1 cannot do from here
//
// This driver implements [store.Migrator] and [store.Batcher]. It does not
// implement [store.Transactional]: the REST API has no interactive transaction,
// only a batch of statements fixed before the request is sent, and a body that
// reads and then decides cannot be expressed as one. [Driver.CreateMany] sends
// its inserts as such a batch.
//
// Writes are not retried by D1, and the SDK's client retries a request it
// believes failed. A create retried after it landed reports [store.ErrAlreadyExists]
// for a record that is there — set option.WithMaxRetries(0) on the client if that
// matters more than riding out a transient failure.
//
// The client is yours: this package does not build it and does not close it.
package d1
