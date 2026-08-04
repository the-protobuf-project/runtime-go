# Streams

Messaging. The root package defines the contract; providers live in
subpackages.

| Provider | Package | Status |
| --- | --- | --- |
| Redis | [`streams/redis`](redis) | implemented |
| NATS | `streams/nats` | not yet implemented |

For storage see [`cache`](../cache) and [`database`](../database).

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/streams
```

## Usage

The caller owns the connection.

```go
import (
    goredis "github.com/redis/go-redis/v9"
    "github.com/the-protobuf-project/runtime-go/streams"
    streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 0})
defer rdb.Close()

p, err := streamsredis.New(streamsredis.Config{Client: rdb, Prefix: "app"})
```

### Streams and subjects

A stream declares the subjects it accepts. Publishing or subscribing to one it
does not declare fails with `streams.ErrUnknownSubject`, so a typo surfaces at
the call that made it instead of creating a topic nobody reads.

```go
s, err := p.Create(ctx, streams.Stream{
    Name:     "events",
    Subjects: []string{"user.login", "user.logout"},
})

m, err := p.Bind(ctx, s.ID())
```

### Publish and subscribe

Subscribe first: the subscription is live by the time `Subscribe` returns, so a
message published afterwards is delivered rather than raced.

```go
ctx, cancel := context.WithCancel(ctx)
defer cancel()

msgs, err := m.Subscribe(ctx, "user.login")
go func() {
    for msg := range msgs {
        log.Printf("%s -> %v", msg.ID(), msg.Data)
    }
}()

err = m.Publish(ctx, "user.login", streams.Message{Data: payload})
```

`Publish` delivers exactly once and does not block.

**The context is the subscription's lifetime.** The channel is closed when it is
done, and cancelling is the only way to stop delivery. Walk away without
cancelling and the delivery goroutine and its server-side subscription live as
long as the process.

### Expiry notifications

`Notifications()` returns a second set of streams whose messages are delivered
**when their TTL expires**, not when they are published — a scheduled reminder,
a lease timeout, a delayed retry.

```go
notify := p.Notifications()

n, _ := notify.Create(ctx, streams.Stream{Name: "reminders", Subjects: []string{"pill"}})
nm, _ := notify.Bind(ctx, n.ID())

reminders, _ := nm.Subscribe(ctx, "pill")
err = nm.Publish(ctx, "pill", streams.Message{
    Data: map[string]any{"body": "take a pill"},
    TTL:  30 * time.Minute, // required — a zero TTL could never fire
})
```

Notification streams live in their own key namespace, so they never appear in
an ordinary `List` and vice versa.

**This needs a server flag.** Delivery rides on Redis keyspace notifications, so
the server must run with `--notify-keyspace-events Ex`. Without it, published
notifications simply never fire. `docker/compose.yaml` sets it.

### Middleware

```go
pub := streams.ChainPublisher(m,
    streams.WithPublisherRetryMiddleware(3, 100*time.Millisecond),
    streams.WithPublisherTelemetryMiddleware(meter),
)
```

Retrying a publish is safe in a way a store write is not: a redelivered message
is a duplicate the consumer can dedupe, whereas a dropped one is simply lost.
`ErrUnknownSubject` is never retried. `WithPublisherTelemetry` records
`streams_published_total` and `streams_publish_duration_seconds`, labeled by
subject.

## Tests

```bash
docker compose -f docker/compose.yaml up -d
go test ./...
```
