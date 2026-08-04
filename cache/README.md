# Cache

Ephemeral, TTL-bound storage. The root package defines the contract; providers
live in subpackages.

| Provider | Package | Status |
| --- | --- | --- |
| Redis | [`cache/redis`](redis) | implemented |
| Memcached | `cache/memcached` | not yet implemented |
| Cloudflare CDN | `cache/cdn` | not yet implemented |

For durable records that never expire see [`database`](../database); for
messaging see [`streams`](../streams).

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/cache
```

## Usage

The caller owns the connection. A provider never dials, never caches a
connection in a package-level variable, and never closes the client it was
handed — pooling, the database index, TLS, and shutdown all stay yours.

```go
import (
    goredis "github.com/redis/go-redis/v9"
    "github.com/the-protobuf-project/runtime-go/cache"
    cacheredis "github.com/the-protobuf-project/runtime-go/cache/redis"
)

rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 1})
defer rdb.Close()

c, err := cacheredis.New(cacheredis.Config{
    Client: rdb,
    Prefix: "orders", // optional; namespaces the keys
})
```

Two caches built from two different clients are genuinely independent, and two
sharing one client under different prefixes cannot see each other's entries.

### Operations

Every method takes a context — these are network calls, and you decide how long
to wait.

```go
created, err := c.Create(ctx, cache.Document{
    Data: user,
    TTL:  30 * time.Second, // zero means it does not expire
})

got, err := c.Get(ctx, created.ID())
err = c.Update(ctx, created.ID(), cache.Document{Data: updated, TTL: time.Minute})
err = c.Delete(ctx, created.ID())
docs, err := c.List(ctx)
```

`Create` generates an ID unless you set one first, which lets a resource name
act as the key:

```go
doc := cache.Document{Data: user, TTL: time.Minute}
doc.SetID("//theprotobufproject.com/user/alice")
```

### Misses

A missing or expired entry reports `cache.ErrNotFound`, which matches through
the generic interface as well as the provider:

```go
if _, err := c.Get(ctx, id); errors.Is(err, cache.ErrNotFound) {
    // not there
}
```

`Delete` on a missing entry is **not** an error — the intent is already
satisfied, and a cache entry may legitimately have expired a moment earlier.

### Middleware

Cross-cutting behavior wraps any provider instead of being reimplemented inside
each one:

```go
c = cache.Chain(c,
    cache.WithRetryMiddleware(3, 100*time.Millisecond),
    cache.WithTelemetryMiddleware(meter),
)
```

The outermost wrapper runs first, so the order above times the whole retried
operation; swap the two to measure individual attempts.

`WithRetry` retries reads and deletes but **not** writes — `Create` and
`Update` are not idempotent, and replaying a half-applied write can duplicate an
entry rather than repair one. `ErrNotFound` is never retried.

`WithTelemetry` takes a [`telemetry.Meter`](../telemetry); pass
`telemetry.NoopMeter` when nothing is wired up. It records
`cache_operations_total`, `cache_operation_duration_seconds`, and
`cache_gets_total{result=hit|miss}` — a miss counts as a hit/miss outcome, not
an error, so error rates stay meaningful.

## Tests

The tests need a live server and skip without one. Override the target with
`REDIS_TEST_HOST` / `REDIS_TEST_PORT` (default `127.0.0.1:6379`):

```bash
docker compose -f docker/compose.yaml up -d
go test ./...
```
