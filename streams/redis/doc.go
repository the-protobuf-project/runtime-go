// Package redis implements [streams.Streams] over Redis.
//
// You bring the connection:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 2})
//	defer rdb.Close()
//
//	s := redis.Connect(rdb)             // immediate, over pub/sub
//	n := redis.ConnectScheduled(rdb)    // on TTL expiry, over keyspace events
//	d := redis.ConnectDurable(rdb)      // durable, over Redis Streams
//
// This package never dials, never holds a connection in a package-level
// variable, and never closes the client it was handed.
//
// # Which constructor
//
// The first two hand a message over and forget it: a subscriber that is not
// attached when it is published never sees it, and one that dies holding it
// loses it. [ConnectDurable] appends to a log instead, tracks each named
// consumer's position on the server, and keeps a delivered message pending
// until it is acknowledged — so it is the only one of the three whose manager
// satisfies [streams.Durable] and [streams.Positioned].
//
// They are separate constructors rather than one with a flag because the
// difference is not a setting. A program that believes it has at-least-once
// delivery and actually has at-most-once loses messages under exactly the
// conditions it added the durability for.
//
// # Swapping providers
//
// Connect returns the [streams.Streams] interface, so the connection object is
// the only thing that changes when you move to another backend:
//
//	s := redis.Connect(rdb)          // Redis
//	s := nats.Connect(nc)            // NATS — same interface, same calls
//
// # Scheduled delivery needs a server flag
//
// [ConnectScheduled] rides on Redis keyspace notifications, so the server must
// run with `--notify-keyspace-events Ex`. Without it, published messages simply
// never fire.
//
// # Headers
//
// A message's [streams.Headers] (the stream's defaults with the publish's own
// on top) travel where each mode can carry them:
//
//   - Durable (Redis Streams): as fields of the entry beside "msg", header
//     "lang" as the field "h:lang", so XRANGE and any other Redis client sees
//     them. The "msg" frame stays version 1. A ':' is refused in a header key,
//     so "h:<key>" never collides with this package's own "msg" and "meta"
//     fields. Every delivery reads them back, a reclaimed one included.
//   - Pub/sub and scheduled: inside the frame, as version 2 of
//     [github.com/the-protobuf-project/runtime-go/streams/core.PackWith],
//     because a PUBLISH or a string key has nowhere else to
//     put them. A message without headers is still a version-1 frame, which
//     every reader understands.
//
// A stream's default headers are stored in its metadata, and checked by Create
// and Update so a bad default fails there.
//
// # Expiry, retain, and delayed redelivery
//
// [streams.Expires] and [streams.Retain] are refused with
// [streams.ErrUnsupported] in every mode: Redis has no per-entry expiry in a
// stream and pub/sub holds nothing to expire, and none of the modes keeps a
// last value per subject.
//
// A durable [streams.Delivery.NakAfter] is real: the entry stays pending and
// its idle time is set back (XCLAIM to itself with IDLE and JUSTID, which
// leaves the delivery count alone) so that the group's reclaim, which takes
// entries idle for [WithReclaimAfter], reaches it only once the delay has
// passed. Redis cannot set an idle time into the future, so the longest delay
// is the reclaim interval ([streams.Delivery.MaxRedeliveryDelay]); a longer one
// is refused by name, and with reclaiming off there is no delayed redelivery
// at all. The delay is held by the server and survives a restart.
//
// # Acknowledgements
//
// A durable consumer's acks are coalesced: those that arrive while one XACK is
// in flight go out together as one multi-id XACK. [WithConfirmAcks] decides
// whether Ack waits for Redis to answer (the default) or returns once queued.
//
// # Surviving a server restart
//
// A durable consumer whose connection drops (a restart, a failover) waits and
// reads again rather than closing its channel: the group, its position, and
// its pending list are on the server, and whatever it held unacknowledged is
// reclaimed once it is back. A consumer that stops while a read is in flight
// lets the read finish and hands back what it took, so a message published at
// that moment is not stranded until the reclaim interval passes.
//
// A command that meets a dropped connection is repeated by the client only
// when repeating it cannot do its work twice: reads, acks, group changes,
// deletes, and the metadata writes. A PUBLISH, an XADD of a message, or a
// scheduled message is never sent twice, because a connection that drops after
// the server ran the command and before its reply arrived looks exactly like
// one that dropped before sending it. Those are repeated only when the
// connection was refused outright, which proves nothing was sent; otherwise the
// caller decides, with [streams.WithPublisherRetry] where a duplicate is
// acceptable.
package redis
