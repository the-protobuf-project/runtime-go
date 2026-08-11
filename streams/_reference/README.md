# Reference

The NATS JetStream and Redis clients written before this module was
restructured, kept for what they encode about each backend rather than to be
compiled.

The directory name begins with an underscore, so the Go tool ignores it: the
code imports `github.com/machanirobotics/loom/...` and
`github.com/machanirobotics/pulse/...`, neither of which resolves here, and it
would otherwise break the build. Their `go.mod` files have been removed — they
existed only to keep this code out of the parent module, which the underscore
now does.

## NATS

What was taken from it, and what was deliberately not:

- **Kept**: declaring a stream idempotently by letting create fall back to
  update when the name is already in use. A stream declaration is a statement of
  what should exist, not an event, so running it twice should be quiet.
- **Kept**: `EnsureConsumer`'s fetch-then-create, for the same reason applied to
  consumers, and the pause/resume and ordered-consumer operations, which are
  real JetStream capabilities worth exposing.
- **Changed**: `CreateStream` and `UpdateStream` logged a warning when `Info()`
  failed and then dereferenced the nil result on the next line. The failure a
  warning was acknowledging was the one that panicked.
- **Changed**: the pull subscription opened a fresh iterator per message with
  `PullMaxMessages(1)`, spending a round trip on each. Messages are fetched in
  batches.
- **Changed**: it acknowledged a message immediately after handing it to the
  channel, so a consumer that died holding one never saw it again. That is
  at-most-once wearing at-least-once's name, which is the specific failure
  `streams.Durable` exists to prevent — acknowledgement belongs to the consumer,
  after the work, which is why `streams.Delivery` carries `Ack` rather than
  calling it.
- **Changed**: "not found" and "already exists" were detected with
  `strings.Contains` over the error text. Errors wrap sentinels
  (`streams.ErrNotFound`) that survive rewording upstream.
- **Changed**: operations took a variadic `helpers.NatsContext` that defaulted to
  a background context when omitted, so a caller's deadline was optional and
  easy to drop. `ctx context.Context` is the first argument.

## Redis

- **Kept**: scheduled delivery built from a TTL key plus a companion `:data` key,
  woken by `__keyevent@N__:expired`. The expiry event carries only the key name,
  so the payload has to outlive the key that announced it — that pairing is the
  idea, and `redis.ConnectScheduled` is it.
- **Changed**: `Publish` slept five seconds and then published the same payload a
  second time. Every publish cost five seconds and every subscriber saw
  everything twice.
- **Changed**: `Set` called `Get`, logged the error, and dereferenced the result
  anyway, so asking for a stream that did not exist panicked instead of
  returning `streams.ErrNotFound`.
- **Changed**: `Update` deleted the stream and then created it, leaving nothing
  in place if the create failed.
- **Changed**: `List` scanned the keyspace and typed each key individually.
- **Not taken**: `cache.go`, `kv.go`, `json.go`, `document.go` and `database.go`
  are a general-purpose Redis client. Caching lives in `runtime-go/cache` and
  storage in `runtime-go/database`, both of which already have Redis providers;
  a third would be a third answer to a question already answered twice.
- **Note**: despite the names, `StreamPublisher` and `StreamSubscriber` used
  Redis Pub/Sub — `PUBLISH`/`SUBSCRIBE`, with `XADD` writing only the stream's
  metadata record. There were no consumer groups and no `XACK`, so nothing here
  is a Redis Streams implementation to port; that is written fresh against
  `streams.Durable`.

Both clients wrote their logs to a package-level `shared.Pulse.Logger`. A
library that logs to a global decides for the program that imports it; the
providers here take a `telemetry.Logger` and default to a no-op.
