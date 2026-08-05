# Redis

The Redis provider for runtime-go. One client implements all three storage and
messaging contracts — [`cache`](../cache), [`database`](../database), and
[`streams`](../streams) — so a single connection serves the whole runtime.

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/redis
```

## The chain

Open a client, name a logical database, and reach the handlers through the
manager that database hands back:

```go
c, err := redis.New(ctx, redis.Config{
    Address: "localhost",
    Port:    "6379",
    Logger:  logger,          // optional; see Observability below
})
defer c.Close()

_ = c.CreateDatabase(ctx, "orders")
mgr, _ := c.SetDatabase(ctx, "orders")
defer mgr.Close()             // a manager owns its own connection

mgr.Document.Cache            // cache.Cache    — ephemeral, TTL-bound
mgr.Document.KV               // database.Store — durable, content-addressed
mgr.Channel.Stream            // streams.Streams — immediate pub/sub
mgr.Channel.Notify            // streams.Streams — delivery on TTL expiry
```

The handlers are grouped by what they hold rather than by backend feature:
`Document` for things you store and read back, `Channel` for things you send and
receive. Both are struct fields, so the whole surface is visible from the
manager without a call.

## Named databases

Redis numbers its databases 0–15. Naming them keeps that numbering out of
application code: the name→index mapping lives in database 0, and 1 is reserved,
so the first name you create lands on 2.

```go
_ = c.CreateDatabase(ctx, "orders")        // errors if the name exists
idx, _ := c.GetDatabase(ctx, "orders")     // the index it maps to
all, _ := c.ListDatabases(ctx)             // every name and index
_ = c.DeleteDatabase(ctx, "orders")        // drops the name, flushes the data
```

Indices are **reused**. Deleting a database frees its slot, and the next create
claims the lowest free one — a server has only a handful, so an
ever-incrementing counter would exhaust them after a few create/delete cycles.
The claim is atomic, so two callers racing on the same index cannot both win.

`SetDatabase` opens a connection bound to that index and returns a manager over
it. It does **not** re-point the client it was called on, so managers for
different databases coexist and an earlier one keeps working after a later one
is made.

## Your model, not ours

Nothing here defines a document type. Values are stored as you hand them over
and decoded into a destination you own, so a model gaining a field is not a
change to this package.

```go
type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

id, _ := mgr.Document.Cache.Create(ctx, "", User{Name: "Ada"}, cache.TTL(time.Minute))

var got User
_ = mgr.Document.Cache.Get(ctx, id, &got)
```

Use the typed views — `cache.For`, `database.For`, `streams.For` — when you want
the compiler to check the shape. They wrap the same handler, so one provider
still serves every model.

## Prefixes

`Config.Prefix` namespaces every key the client's handlers touch. Use it to
share one Redis database between concerns, or to run independent instances of
the same concern side by side. Named databases give stronger isolation; a prefix
is the lighter option.

Within one database the four handlers already keep separate keyspaces, so a
cache entry, a record, a stream, and a notification never collide.

## Observability

`Config.Logger` and `Config.Meter` take the injected
[`telemetry`](../telemetry) contracts and default to the no-op values, so a
binary that wires nothing pays nothing.

```go
logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
    &slog.HandlerOptions{Level: slog.Level(telemetry.LevelDebug)})))

c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379", Logger: logger})
```

That covers the provider's own detail — which key an id resolved to, which stale
entries were swept, which database a manager bound to. For a uniform record per
operation, wrap a handler with its concern's `WithLogging` decorator; the two
compose. Use [`observability`](../observability) for OpenTelemetry-backed
implementations.

## Scheduled delivery needs a server flag

`mgr.Channel.Notify` delivers when a message's TTL expires, which rides on Redis
keyspace notifications. The server must run with `--notify-keyspace-events Ex`;
without it, scheduled messages simply never fire. `docker/compose.yaml` sets it.

## Layout

```
redis/
  redis.go     Client, Config, New, the database registry surface
  manager.go   DBManager, DocumentHandler, ChannelHandler
  cache.go     ephemeral, TTL-bound storage
  kv.go        durable, content-addressed storage
  stream.go    stream lifecycle (both delivery modes)
  pubsub.go    publish and subscribe
  keys.go      every key the handlers use
  internal/
    conn/      connection and the named-database registry
    codec/     encoding and canonical hashing
```

## Running it

```bash
docker compose -f docker/compose.yaml up -d
go run ./example/chain     # exercises every handler end to end
go test ./...
```

The tests need a live server and skip without one. Override the target with
`REDIS_TEST_HOST` / `REDIS_TEST_PORT` (default `127.0.0.1:6379`).
