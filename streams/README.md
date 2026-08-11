# Streams

Messaging with a backend-agnostic contract. One publish/subscribe interface over
pub/sub, stored logs and scheduled delivery; a provider implements what its
backend can do and says so where it cannot.

| Backend | Package | Delivery |
| --- | --- | --- |
| Redis pub/sub | [`streams/redis`](./redis) | immediate, nothing kept |
| Redis scheduled | [`streams/redis`](./redis) | on TTL expiry |
| Redis Streams | [`streams/redis`](./redis) | durable, redelivered until acknowledged |
| Core NATS | [`streams/nats`](./nats) | immediate, nothing kept |
| NATS JetStream | [`streams/nats`](./nats) | durable, redelivered until acknowledged |
| Kafka | [`streams/kafka`](./kafka) | durable, partitioned, replayable by offset |

For ephemeral, TTL-bound entries see [`cache`](../cache); for durable records see
[`database`](../database).

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/streams
```

## Usage

You own the client; a provider is built around one and never dials or closes it.

```go
import (
    goredis "github.com/redis/go-redis/v9"
    "github.com/the-protobuf-project/runtime-go/streams"
    streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
defer rdb.Close()

events := streamsredis.Connect(rdb)   // a streams.Streams
```

Everything after that line is the interface, so pointing this at NATS means
changing the import and the constructor and nothing else:

```go
nc, _ := gonats.Connect(gonats.DefaultURL)
defer nc.Close()

events, _ := streamsnats.ConnectJetStream(nc)
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

Redis reads those subjects as literal names. NATS reads them as patterns, so a
stream declaring `user.*` accepts `user.created`.

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

## Capabilities

`Publisher` and `Subscriber` are the honest intersection — send a value, receive
it, no promise it survives a restart — because that is what Redis pub/sub and
core NATS actually do. Anything past that is a capability a provider has or does
not, and asking for one it lacks returns `ErrUnsupported` naming the provider
rather than silently downgrading to weaker delivery than you asked for.

### Durable delivery

`Durable` is the difference between a message being handed over and a message
being *lent*. A named consumer's position lives on the server, so it outlives
the process reading it, and a delivered message stays deliverable until someone
acknowledges it.

```go
d, err := streams.AsDurable(m)   // fails, by name, on pub/sub providers
if err != nil {
    return err
}

deliveries, _ := d.Consume(ctx, "order.placed", "billing")
for msg := range deliveries {
    if err := handle(msg); err != nil {
        msg.Nak(ctx)   // hand it back
        continue
    }
    msg.Ack(ctx)       // after the work, not on receipt
}
```

Acknowledge **after** the work. Acknowledging on receipt turns at-least-once
into at-most-once, which is the guarantee you were avoiding by reaching for
`Durable` in the first place.

The consumer name is the identity that survives a restart. Two processes under
one name share its position and split the work; a process that dies and comes
back resumes where the name left off. `Delivery.Attempt` counts how many times
the message has been delivered — it is the one signal for breaking a redelivery
loop, since the same bytes arriving for the fifth time look exactly like the
first.

Backed by Redis Streams (`ConnectDurable`), JetStream (`ConnectJetStream`) and
Kafka (`kafka.Connect`).

`Delivery.Attempt` is zero on Kafka, which is the contract's answer for a
provider that cannot count: Kafka redelivers the same bytes with no record of
having done so. Redis and JetStream both count and report a real number.

### Ordering

`streams.PartitionKey` decides which messages are ordered relative to each
other. It means something only on Kafka, which orders within a partition and
nowhere else:

```go
// Both land on one partition, so they are seen in the order they were sent.
m.Publish(ctx, "order.placed", a, streams.PartitionKey(accountID))
m.Publish(ctx, "order.shipped", b, streams.PartitionKey(accountID))
```

Redis and NATS order everything in one place and have no partition to choose, so
they ignore it — which is safe, and is what the contract permits: a backend that
orders everything loses nothing by being told what could have shared an order.

Kafka also acknowledges **sequentially**. It tracks one offset per partition
rather than one per message, so acknowledging a delivery marks everything before
it in that partition as handled too. Handlers that acknowledge out of order will
mark messages they never finished.

### Replay

`Positioned` reads a stored log from somewhere other than now:

```go
p, _ := streams.AsPositioned(m)
deliveries, _ := p.ConsumeFrom(ctx, "order.placed", "audit", streams.FromEarliest)
```

The position applies when the consumer is created and not after. A consumer that
already exists keeps the position it has — resetting on every attach would
replay the log on every restart, which is the opposite of what a durable
consumer is for.

### Scheduled delivery

Some providers deliver when a TTL expires rather than on publish — a reminder, a
lease timeout, a delayed retry. On Redis that is a separate constructor with its
own key namespace:

```go
notify := streamsredis.ConnectScheduled(rdb)

n, _ := notify.Create(ctx, streams.Stream{Name: "reminders", Subjects: []string{"pill"}})
nm, _ := notify.Bind(ctx, n.ID)

reminders, _ := nm.Subscribe(ctx, "pill")
_, err := nm.Publish(ctx, "pill", Reminder{Body: "take a pill"},
    streams.TTL(30*time.Minute))
```

A TTL is **required** there — delivery is the expiry, so a message without one
could never fire. Conversely every other provider **rejects** a TTL rather than
publishing now and letting you believe it was scheduled.

Redis delivers these through keyspace notifications, so the server must run with
`--notify-keyspace-events Ex`. NATS has no scheduled delivery at all and says so.

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
`ErrUnknownSubject` and `ErrUnsupported` are never retried — neither answer
changes on a second attempt.

On JetStream the message id is also sent as the JetStream message id, so a retry
after an ambiguous failure is collapsed by the server rather than appended twice.

`WithSubscriberLogging` records when a subscription opens, each delivery, and
when it closes — the close record carries the delivered count, and its absence
is how you spot a consumer that leaked.

## Tests

The contract, its decorators and `core` are covered by unit tests that need no
server. The provider suites are integration tests.

NATS starts a server in-process, so it needs nothing installed. Redis needs a
live one and skips without it — including the keyspace events the scheduled
tests rely on:

```bash
docker compose -f docker/compose.yaml up -d
go test ./...
```

Runnable demonstrations of every delivery mode live in
[`examples`](./examples).
