# Database

The backend-agnostic contract for durable document storage — records live until
they are deleted. This module holds the interface and its decorators — no
backend. Providers implement it in their own modules:

| Provider | Module | Status |
| --- | --- | --- |
| Redis | [`runtime-go/redis`](../redis) | implemented |
| MongoDB | `database/mongodb` | not yet implemented |
| GORM (SQL) | `database/gorm` | not yet implemented |
| Neo4j | `database/neo4j` | not yet implemented |
| ArangoDB | `database/arangodb` | not yet implemented |

For ephemeral TTL-bound entries see [`cache`](../cache); for messaging see
[`streams`](../streams).

## Not the proto Driver

This stores **ad-hoc values**. The generated-proto CRUD seam —
[`interfaces/store`](../interfaces/store) — operates on `proto.Message` values
through `Resource` descriptors and serves a different job. Both exist on
purpose; `gorm` appears in both lists wearing two different hats.

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/database
```

## Usage

You reach a store through a provider's manager, not by constructing one here:

```go
import (
    "github.com/the-protobuf-project/runtime-go/database"
    "github.com/the-protobuf-project/runtime-go/redis"
)

c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379"})
defer c.Close()

_ = c.CreateDatabase(ctx, "orders")
mgr, _ := c.SetDatabase(ctx, "orders")
defer mgr.Close()

books := mgr.Document.KV   // a database.Store
```

### Your model, not ours

There is no document type. A value goes in as it is and comes back decoded into
a destination you own.

```go
type Book struct {
    Title  string `json:"title"`
    Author string `json:"author"`
}

id, err := books.Create(ctx, "", Book{Title: "Dune", Author: "Herbert"})

var got Book
err = books.Get(ctx, id, &got)

err = books.Update(ctx, id, Book{Title: "Dune", Author: "Frank Herbert"})
err = books.Delete(ctx, id)

ids, err := books.Keys(ctx, database.Limit(20), database.Offset(40))

var page []Book
err = books.List(ctx, &page, database.Limit(20))
```

`Keys` and `List` return records in a stable order, so `Limit` and `Offset` page
predictably — without one, successive pages would overlap or skip records.

### Typed views

```go
shelf := database.For[Book](mgr.Document.KV)

id, _ := shelf.Create(ctx, b)
b2, _ := shelf.Get(ctx, id)     // returns a Book
all, _ := shelf.List(ctx)       // returns []Book
```

### Deduplication

Providers may be content-addressed. The Redis one canonicalizes each value —
map keys sorted — hashes it with SHA256, and reserves the hash before writing,
so identical content resolves to a single record:

```go
a, _ := books.Create(ctx, "", map[string]any{"x": 1, "y": 2})
b, _ := books.Create(ctx, "", map[string]any{"y": 2, "x": 1})
// a == b — field order is not content
```

Compare the returned id against the one you supplied to tell a fresh write from
a deduplicated one. `Update` to content another record already holds is refused
with `database.ErrDuplicate`; deleting a record releases its content for reuse.

### Missing records

Unlike a cache, a missing record is a genuine surprise — records do not expire
on their own — so both `Get` and `Delete` report it:

```go
if err := books.Delete(ctx, id); errors.Is(err, database.ErrNotFound) {
    // it was not there
}
```

## Middleware

```go
db := database.Chain(mgr.Document.KV,
    database.WithRetryMiddleware(3, 100*time.Millisecond),
    database.WithLoggingMiddleware(logger),
    database.WithTelemetryMiddleware(meter),
)
```

- **`WithRetry`** retries reads but **not** writes, and never retries
  `ErrNotFound` or `ErrDuplicate` — both are settled answers.
- **`WithLogging`** records debug per success, warn per refused duplicate, error
  per failure, and info when a write was deduplicated to an existing record.
- **`WithTelemetry`** records `database_operations_total`,
  `database_operation_duration_seconds`, and `database_records`. Here
  `ErrNotFound` **does** count as an error, unlike in the cache.

## Tests

This module's tests are pure unit tests over the contract and decorators and
need no server. The provider's integration tests live in
[`runtime-go/redis`](../redis).

```bash
go test ./...
```
