# Cache

Ephemeral, TTL-bound storage with a backend-agnostic contract. Four caching
strategies, implemented once, running on whichever server you point them at.

| Backend | Package | Status |
| --- | --- | --- |
| Redis | [`cache/redis`](./redis) | implemented |
| Dragonfly | [`cache/dragonfly`](./dragonfly) | implemented |
| memcached | [`cache/memcached`](./memcached) | implemented |
| Any RESP server | [`cache/resp`](./resp) | the driver the first two are presets over |

For durable records that never expire see [`database`](../database); for
messaging see [`streams`](../streams).

## Usage

Three steps, in this order:

```go
import (
    "github.com/the-protobuf-project/runtime-go/cache"
    "github.com/the-protobuf-project/runtime-go/cache/redis"
)

client, err := redis.NewClient(ctx, redis.Config{Address: "localhost:6379"})
defer client.Close()

c := redis.New(client, cache.Config{Prefix: "orders", DefaultTTL: time.Minute})

db, err := c.SetDatabase(ctx, 1)
defer db.Close()
```

The client comes from the provider package, so a program that caches never
imports a driver and never needs an import alias. It is also yours to keep: hand
the same client to the database and streams layers and all three share a pool.

Swapping backends is the import line and the constructor:

```go
client, _ := dragonfly.NewClient(ctx, dragonfly.Config{Address: "localhost:6380"})
c := dragonfly.New(client, cache.Config{Prefix: "orders"})
```

## The four strategies

A `DB` is not one more cache interface — it is a set of named strategies over one
database, because storing a value you will enumerate, one you will only read back
by key, and one you want found by e-mail address are three jobs with three
different costs.

```go
db.Document    // whole encoded values, enumerable, at the cost of an index
db.Volatile    // TTL-first, no index, nothing to sweep, no enumeration
db.Indexed     // Document plus lookups by a field other than the id
db.Aside(load) // read-through over your loader
```

### Your model, not ours

There is no document or entry type. A value goes in as it is and comes back
decoded into a destination you own.

```go
type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

id, err := db.Document.Create(ctx, "", User{Name: "Ada"}, cache.TTL(time.Minute))

var got User
err = db.Document.Get(ctx, id, &got)

ids, err := db.Document.Keys(ctx)   // live ids, sweeping expired ones
ttl, err := db.Document.TTL(ctx, id)

var all []User
err = db.Document.List(ctx, &all)
```

### Lookups by something other than the id

```go
id, _ := db.Indexed.Create(ctx, "", user,
    cache.TTL(time.Hour),
    cache.Index("email", user.Email),
    cache.Index("tenant", "acme"),
)

var found []User
err = db.Indexed.ByIndex(ctx, "email", "ada@example.com", &found)

n, err := db.Indexed.DeleteByIndex(ctx, "tenant", "acme")  // group invalidation
```

### Read-through, without the three usual bugs

```go
users := db.Aside(func(ctx context.Context, id string) (any, error) {
    return loadUserFromDatabase(ctx, id)
})

var u User
err := users.GetOrLoad(ctx, id, &u, cache.TTL(time.Minute), cache.Stale(time.Minute))
```

- **No stampede.** Concurrent loads of one id collapse into one execution, and no
  caller blocks past its own context — a request that gives up leaves the load
  running for the others waiting on it.
- **Absence is cached.** A loader reporting `cache.ErrNotFound` has that
  remembered, so requests for something that does not exist stop reaching it.
- **`Stale` means nobody waits.** Past its TTL but inside the stale window, an
  entry is served immediately and refreshed behind the reader. Off by default:
  serving old data is a policy, not an optimization to switch on quietly.

Use `Refresh` rather than `Invalidate` for a value you know just changed —
dropping the entry leaves a window where every reader misses at once.

### Typed views

```go
users := cache.For[User](db.Document)

id, _ := users.Create(ctx, u, cache.TTL(time.Minute))
u2, _ := users.Get(ctx, id)   // returns a User
all, _ := users.List(ctx)     // returns []User
```

## Middleware

```go
docs := cache.Chain(db.Document,
    cache.WithRetryMiddleware(3, 100*time.Millisecond),
    cache.WithLoggingMiddleware(logger),
    cache.WithTelemetryMiddleware(meter),
)
```

The outermost wrapper runs first, so the order above times the whole retried
operation. `WithRetry` retries reads and deletes but **not** writes — `Create`
and `Update` are not idempotent. `ErrNotFound` is never retried.

## What a backend cannot do

Backends differ, and the differences are load-bearing. A strategy that needs a
missing capability reports `cache.ErrUnsupported` with a message naming the
backend and why — the field is never nil, because a nil field panics far from the
wiring mistake that caused it.

| | Redis / Dragonfly | memcached |
| --- | --- | --- |
| `Document` | full | store and fetch by id only |
| `Volatile` | full | full (`Touch` is native, and cleaner than `EXPIRE XX`) |
| `Indexed` | full | none — no sets to index with |
| `Aside` | full, with a cross-process lock | full, collapsing loads per process |
| `SetDatabase(n)` | a real database | a key namespace, `db3:` |
| Enumeration cost | one pipelined round trip per 256 ids | same, via multi-get |

## Adding a backend

Implement [`core.Driver`](./core/driver.go) — eight single-key methods — plus any
of the optional capabilities in [`core/capabilities.go`](./core/capabilities.go)
your server has. Every strategy then works, because none of them is written per
backend. A RESP-speaking server needs even less: see
[`dragonfly`](./dragonfly/dragonfly.go), which is a name and two constants.

## Tests

```bash
go test -race ./...            # unit and concurrency tests, no server needed
docker compose -f docker/compose.yaml up -d
go run ./example               # all three backends
```
