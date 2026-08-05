# Streams

The backend-agnostic contract for messaging. This module holds the interfaces
and their decorators — no backend. Providers implement it in their own modules:

| Provider | Module | Status |
| --- | --- | --- |
| Redis | [`runtime-go/redis`](../redis) | implemented |
| NATS | `streams/nats` | not yet implemented |

For storage see [`cache`](../cache) and [`database`](../database).

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/streams
```

## Usage

You reach streams through a provider's manager:

```go
import (
    "github.com/the-protobuf-project/runtime-go/streams"
    "github.com/the-protobuf-project/runtime-go/redis"
)

c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379"})
defer c.Close()

_ = c.CreateDatabase(ctx, "events")
mgr, _ := c.SetDatabase(ctx, "events")
defer mgr.Close()

events := mgr.Channel.Stream   // a streams.Streams
```

### Streams and subjects

A stream declares the subjects it accepts. Publishing or subscribing to one it
does not declare fails with `streams.ErrUnknownSubject`, so a typo surfaces at
the call that made it instead of creating a topic nobody reads.

```go
s, _ := events.Create(ctx, streams.Stream{
    Name:     "users",
    Subjects: []string{"user.created", "user.deleted"},
})

m, _ := events.Bind(ctx, s.ID)   // a streams.Manager
```

### Publish and subscribe

Subscribe first: the subscription is live by the time `Subscribe` returns, so a
value published afterwards is delivered rather than raced.

```go
ctx, cancel := context.WithCancel(ctx)
defer cancel()

msgs, _ := m.Subscribe(ctx, "user.created")
go func() {
    for msg := range msgs {
        var u User
        if err := msg.Decode(&u); err == nil {
            log.Printf("%s -> %+v", msg.ID, u)
        }
    }
}()

id, err := m.Publish(ctx, "user.created", User{Name: "Ada"})
```

`Publish` delivers exactly once and does not block. Your model goes out as it
is; `Message.Decode` reads it back into a destination you own.

**The context is the subscription's lifetime.** The channel closes when it is
done, and canceling is the only way to stop delivery. Walk away without
canceling and the delivery goroutine and its server-side subscription live as
long as the process.

### Typed views

```go
users := streams.For[User](m)

users.Publish(ctx, "user.created", u)

msgs, _ := users.Subscribe(ctx, "user.created")
for u := range msgs { … }   // u is a User
```

A message that fails to decode as `T` is skipped rather than delivered as a zero
value. Use the untyped `Manager` when you need to see those.

### Scheduled delivery

Some providers can deliver when a TTL expires rather than on publish — a
reminder, a lease timeout, a delayed retry. On the Redis provider that is a
second handler with its own namespace:

```go
notify := mgr.Channel.Notify

n, _ := notify.Create(ctx, streams.Stream{Name: "reminders", Subjects: []string{"pill"}})
nm, _ := notify.Bind(ctx, n.ID)

reminders, _ := nm.Subscribe(ctx, "pill")
_, err := nm.Publish(ctx, "pill", Reminder{Body: "take a pill"},
    streams.TTL(30*time.Minute))
```

A TTL is **required** there — delivery is the expiry, so a message without one
could never fire. Conversely an immediate stream **rejects** a TTL rather than
publishing now and letting you believe it was scheduled.

Redis delivers these through keyspace notifications, so the server must run with
`--notify-keyspace-events Ex`. See the [provider README](../redis).

## Middleware

```go
pub := streams.ChainPublisher(m,
    streams.WithPublisherRetryMiddleware(3, 100*time.Millisecond),
    streams.WithPublisherLoggingMiddleware(logger),
    streams.WithPublisherTelemetryMiddleware(meter),
)
```

Retrying a publish is safe in a way a store write is not: a redelivered message
is a duplicate the consumer can dedupe, whereas a dropped one is simply lost.
`ErrUnknownSubject` is never retried.

`WithSubscriberLogging` records when a subscription opens, each delivery, and
when it closes — the close record carries the delivered count, and its absence
is how you spot a consumer that leaked.

## Tests

This module's tests are pure unit tests over the contract and decorators and
need no server. The provider's integration tests live in
[`runtime-go/redis`](../redis).

```bash
go test ./...
```
