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
| MQTT 5 | [`streams/mqtt`](./mqtt) | durable by session, not replayable |
| RabbitMQ | [`streams/rabbitmq`](./rabbitmq) | durable by queue, true negative acknowledgement |
| ZeroMQ | [`streams/zeromq`](./zeromq) | brokerless, immediate, nothing kept |

For ephemeral, TTL-bound entries see [`cache`](../cache); for durable records see
[`database`](../database).

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/streams
```

## Usage

`Connect` dials, so nothing but this module is imported and the package name is
the backend's:

```go
import (
    "github.com/the-protobuf-project/runtime-go/streams"
    "github.com/the-protobuf-project/runtime-go/streams/redis"
)

events, err := redis.Connect(ctx, "localhost:6379")   // a streams.Streams
defer events.(streams.Closer).Close()
```

Everything after that line is the interface, so pointing this at another backend
means changing the import and the constructor and nothing else:

```go
import "github.com/the-protobuf-project/runtime-go/streams/nats"

events, err := nats.ConnectJetStream(ctx, "nats://localhost:4222")
```

**`Connect` dials and owns; `Use` takes a client you built.** Every provider has
`Connect`; the two whose client is worth sharing with the rest of a program also
have `Use`:

```go
rdb := goredis.NewClient(&goredis.Options{ /* TLS, cluster, pooling */ })
defer rdb.Close()

events := redis.Use(rdb)   // this package will not close what it did not open
```

A provider that dialed implements `streams.Closer`; one built by `Use` also
implements it, as a no-op, so a caller may close either without knowing which it
has.

| | Dials | Takes a client |
| --- | --- | --- |
| Redis | `Connect`, `ConnectScheduled`, `ConnectDurable` | `Use`, `UseScheduled`, `UseDurable` |
| NATS | `Connect`, `ConnectJetStream` | `Use`, `UseJetStream` |
| Kafka | `Connect` | — |
| RabbitMQ | `Connect` | — |
| MQTT | `Connect` | — |
| ZeroMQ | `Publish`, `Subscribe` | — |

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

Backed by Redis Streams (`ConnectDurable`), JetStream (`ConnectJetStream`),
Kafka, RabbitMQ and MQTT.

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

**MQTT is durable but not positioned**, which is the clearest argument for
keeping the two capabilities apart. An MQTT session really does hold a
consumer's subscriptions and its unacknowledged messages while it is away, so
`Durable` is honest there. But a session is a queue, not a log: there is nothing
behind it to seek, so `AsPositioned` refuses by name. A contract that had fused
the two would have had to either lie about replay or throw away durability MQTT
genuinely has.

| Provider | Durable | Positioned | `Attempt` | `Nak` returns it |
| --- | --- | --- | --- | --- |
| Redis Streams | ✓ | ✓ | counted | after the reclaim interval |
| NATS JetStream | ✓ | ✓ | counted | immediately |
| Kafka | ✓ | ✓ | 0 — cannot count | on partition reassignment |
| RabbitMQ | ✓ | ✗ | counted | immediately |
| MQTT 5 | ✓ | ✗ | 0 — cannot count | on reconnect |
| Redis pub/sub, core NATS, ZeroMQ | ✗ | ✗ | — | — |

RabbitMQ and MQTT are durable without being replayable — a queue and a session
both hold what a consumer has not handled, but neither is a log you can seek in.
Kafka cannot count redeliveries; RabbitMQ counts them on quorum queues and
otherwise reports at least the second attempt from the redelivered flag.

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
server. Most provider suites start their broker in-process and need nothing
installed either: Kafka via `kfake`, NATS via its embedded server, MQTT via
`mochi-mqtt`. ZeroMQ is brokerless, so it needs nothing at all.

Redis and RabbitMQ are the exceptions. Both skip without a live server — Redis
also needs the keyspace events the scheduled tests rely on, which is why the
compose file sets `--notify-keyspace-events Ex`:

```bash
docker compose -f docker/compose.yaml up -d
go test ./...
```

A suite that skips reports `ok`, so check for `SKIP` before believing a green
run covered those two:

```bash
go test ./... -v | grep -c -- '--- SKIP'
```

A runnable demonstration of every provider lives in [`examples`](./examples) —
one directory each, showing what that backend does that the others cannot. The
ZeroMQ one needs nothing running at all.
