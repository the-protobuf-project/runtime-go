# Cache

The backend-agnostic contract for ephemeral, TTL-bound storage. This module
holds the interface and its decorators — no backend. Providers implement it in
their own modules:

| Provider | Module | Status |
| --- | --- | --- |
| Redis | [`runtime-go/redis`](../redis) | implemented |
| Memcached | `cache/memcached` | not yet implemented |
| Cloudflare CDN | `cache/cdn` | not yet implemented |

For durable records that never expire see [`database`](../database); for
messaging see [`streams`](../streams).

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/cache
```

## Usage

You reach a cache through a provider's manager, not by constructing one here:

```go
import (
    "github.com/the-protobuf-project/runtime-go/cache"
    "github.com/the-protobuf-project/runtime-go/redis"
)

c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379"})
defer c.Close()

_ = c.CreateDatabase(ctx, "orders")
mgr, _ := c.SetDatabase(ctx, "orders")
defer mgr.Close()

entries := mgr.Document.Cache   // a cache.Cache
```

### Your model, not ours

There is no document or entry type. A value goes in as it is and comes back
decoded into a destination you own, so adding a field to your model is not a
change to this package.

```go
type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

// An empty id has the provider generate one and return it.
id, err := entries.Create(ctx, "", User{Name: "Ada"}, cache.TTL(time.Minute))

var got User
err = entries.Get(ctx, id, &got)

err = entries.Update(ctx, id, User{Name: "Ada", Age: 36}, cache.TTL(time.Hour))
err = entries.Delete(ctx, id)

ids, err := entries.Keys(ctx)      // live ids, sweeping expired ones
ttl, err := entries.TTL(ctx, id)   // zero means it does not expire

var all []User
err = entries.List(ctx, &all)
```

### Typed views

`For` puts a typed view over any `Cache` when you want the compiler to check the
shape. It is a wrapper, not a second client — one provider serves every model,
so it is configured once no matter how many types run through it.

```go
users := cache.For[User](mgr.Document.Cache)

id, _ := users.Create(ctx, u, cache.TTL(time.Minute))
u2, _ := users.Get(ctx, id)        // returns a User
all, _ := users.List(ctx)          // returns []User
```

Views of different types over the same `Cache` see the same entries. Give each
model its own database or prefix when they should not.

### Options, not parameters

TTL is an option so a provider can gain or lose a capability without changing a
signature — which matters when several caches implement this contract. A
provider ignores what it cannot honor.

### Misses

A missing or expired entry reports `cache.ErrNotFound`:

```go
if err := entries.Get(ctx, id, &got); errors.Is(err, cache.ErrNotFound) {
    // not there
}
```

`Delete` on a missing entry is **not** an error — the intent is already
satisfied, and an entry may legitimately have expired a moment earlier.

## Middleware

Cross-cutting behavior wraps any provider instead of being reimplemented inside
each one:

```go
c := cache.Chain(mgr.Document.Cache,
    cache.WithRetryMiddleware(3, 100*time.Millisecond),
    cache.WithLoggingMiddleware(logger),
    cache.WithTelemetryMiddleware(meter),
)
```

The outermost wrapper runs first, so the order above times the whole retried
operation; swap them to measure individual attempts.

- **`WithRetry`** retries reads and deletes but **not** writes — `Create` and
  `Update` are not idempotent, and replaying a half-applied write can duplicate
  an entry rather than repair one. `ErrNotFound` is never retried.
- **`WithLogging`** records a debug line per success, warn per miss, error per
  failure, each with a duration. Providers log their own internals separately;
  the two compose.
- **`WithTelemetry`** records `cache_operations_total`,
  `cache_operation_duration_seconds`, and `cache_gets_total{result=hit|miss}`.
  A miss counts as a hit/miss outcome rather than an error, so error rates stay
  meaningful.

Both take an injected [`telemetry`](../telemetry) `Logger`/`Meter`; pass the
no-op values when nothing is wired up, or use
[`observability`](../observability) for OpenTelemetry-backed ones.

## Tests

This module's tests are pure unit tests over the contract and decorators and
need no server. The provider's integration tests live in
[`runtime-go/redis`](../redis).

```bash
go test ./...
```
