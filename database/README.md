# Database

Durable document storage — records live until they are deleted. The root
package defines the contract; providers live in subpackages.

| Provider | Package | Status |
| --- | --- | --- |
| Redis | [`database/redis`](redis) | implemented |
| MongoDB | `database/mongodb` | not yet implemented |
| GORM (SQL) | `database/gorm` | not yet implemented |
| Neo4j | `database/neo4j` | not yet implemented |
| ArangoDB | `database/arangodb` | not yet implemented |

For ephemeral TTL-bound entries see [`cache`](../cache); for messaging see
[`streams`](../streams).

## Not the proto Driver

This is a store for **ad-hoc JSON documents**. The generated-proto CRUD seam —
[`interfaces/store`](../interfaces/store) — operates on `proto.Message` values
through `Resource` descriptors and serves a different job. Both exist on
purpose. `gorm` appears in both lists wearing two different hats.

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/database
```

## Usage

The caller owns the connection.

```go
import (
    goredis "github.com/redis/go-redis/v9"
    "github.com/the-protobuf-project/runtime-go/database"
    dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
)

rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 0})
defer rdb.Close()

db, err := dbredis.New(dbredis.Config{Client: rdb, Prefix: "orders"})
```

### Operations

```go
created, err := db.Create(ctx, database.Document{Data: book})
got, err := db.Get(ctx, created.ID())
err = db.Update(ctx, created.ID(), database.Document{Data: revised})
err = db.Delete(ctx, created.ID())

docs, err := db.List(ctx, database.Query{Limit: 20, Offset: 40})
```

`List` returns documents sorted by ID, so `Limit` and `Offset` page
predictably — without a stable order, successive pages would overlap or skip
records.

### Deduplication

The store is content-addressed. Every payload is canonicalized — map keys
sorted — and hashed with SHA256, and the hash is reserved atomically before the
body is written. Writing content that already exists returns the document that
holds it rather than storing a second copy:

```go
a, _ := db.Create(ctx, database.Document{Data: map[string]any{"x": 1, "y": 2}})
b, _ := db.Create(ctx, database.Document{Data: map[string]any{"y": 2, "x": 1}})
// a.ID() == b.ID() — key order is not content
```

Compare the returned ID against the one you supplied to tell a fresh write from
a deduplicated one. `Update` to content another document already holds is
rejected with `database.ErrDuplicate`; deleting a document releases its content
for reuse.

### Missing records

Unlike a cache, a missing record is a genuine surprise — documents do not expire
on their own — so both `Get` and `Delete` report it:

```go
if err := db.Delete(ctx, id); errors.Is(err, database.ErrNotFound) {
    // it was not there
}
```

### Middleware

```go
db = database.Chain(db,
    database.WithRetryMiddleware(3, 100*time.Millisecond),
    database.WithTelemetryMiddleware(meter),
)
```

`WithRetry` retries reads but **not** writes, and never retries `ErrNotFound`
or `ErrDuplicate` — both are settled answers. `WithTelemetry` records
`database_operations_total`, `database_operation_duration_seconds`, and
`database_documents`; here `ErrNotFound` **does** count as an error, unlike in
the cache.

## Tests

```bash
docker compose -f ../cache/docker/compose.yaml up -d
go test ./...
```
