// Package rabbitmq delivers [streams] over RabbitMQ.
//
// It satisfies [streams.Publisher], [streams.Subscriber] and
// [streams.Durable], but not [streams.Positioned] — the same split as MQTT,
// and for the same reason. A durable queue really does hold a consumer's
// messages while it is away, and RabbitMQ is the only provider here with a
// true negative acknowledgement, so [streams.Delivery.Nak] returns a message
// immediately rather than waiting for a timeout. But a queue is not a log: a
// message that has been acknowledged is gone, so there is no position but now.
//
// # How a stream maps onto AMQP
//
// A stream is a topic exchange and each subject is a routing key. A durable
// consumer is a durable queue bound to the exchange for its subject, named
// after the consumer — which is what makes the name, rather than the process,
// the thing that outlives a restart. Two processes consuming under one name
// take from one queue and so split the work, which is AMQP's round-robin
// rather than anything this package arranges.
//
// [streams.Subscriber.Subscribe] binds an exclusive, auto-deleting queue
// instead. It is removed when the connection drops, so nothing accumulates for
// a subscriber that has gone away — which is what makes it the undurable half.
//
// # Headers
//
// [streams.Headers] travel in the AMQP basic-properties headers table, each
// value a long string, on single publishes, every publishing channel, and
// batches alike. The body stays the version-1 frame, so the management UI,
// rabbitmqadmin, or any AMQP client sees the headers as ordinary message
// properties. A publish with no headers sends no table at all.
//
// Received messages, durable deliveries and redeliveries included, carry the
// string-valued entries of the table. Two kinds of key are RabbitMQ's and are
// refused with [streams.ErrInvalidHeader] at Create, Update and every publish,
// and skipped on receipt: the "x-" prefix (x-delivery-count, x-death, the
// plugins' x-delay), and CC and BCC, which RabbitMQ reads as extra routing
// keys. A key over 255 bytes is refused too, AMQP's limit for a field name. A
// stream's default headers are kept with its declaration in this process,
// like the rest of its metadata; a message's own are in the message, so they
// survive in a durable queue across a broker restart.
//
// # Expiry, retain, delayed nak
//
// [streams.Expires] is the per-message expiration property, whole
// milliseconds rounded up, at most 2^32 - 1. RabbitMQ drops an expired
// message rather than delivering it, but only once it reaches the head of its
// queue: a message behind one that has not expired stays in the queue, counted
// in its depth, until everything ahead of it is consumed, and is then dropped.
//
// [streams.Retain] is refused: a queue forgets a message once it is consumed,
// and a queue bound later starts empty, so there is no last value to hand a
// late subscriber.
//
// [streams.Delivery.NakAfter] does not wait, and
// [streams.Delivery.CanDelayRedelivery] is false: the message is requeued for
// immediate redelivery and the call reports [streams.ErrUnsupported].
//
// # Connections and channels
//
// [Connect] takes a URL — amqp:// or amqps://, with [WithTLS] for a private CA
// or mutual TLS and [WithCredentials] to keep a password out of the URL — and
// owns the connection it dials. It implements [streams.Closer]; close it when
// you are done, and cancel the contexts of anything consuming first. [Use]
// takes a connection you dialed instead, and opens its channels on that;
// closing that store closes only the publishing channels, never the
// connection.
//
// A connection [Connect] dialed keeps itself up. One that drops is dialed
// again; a publish waits for it as long as its context allows, and every
// consumer attaches again on a fresh channel. A durable queue loses nothing in
// the gap, and what was delivered but not acknowledged is redelivered. A
// connection given to [Use] is not redialed: recovering it is its owner's job.
//
// # Publishes are confirmed
//
// The publishing channels are in confirm mode, so Publish returns once the
// broker has taken the message rather than once it has been written to a
// socket: for a persistent message routed to a durable queue, written to
// disk. A negative confirm is an error rather than a silent loss. Single
// publishes are spread round-robin over [WithPublishChannels] channels (4 by
// default), so concurrent callers are confirmed side by side; a batch stays on
// one channel, in order. A publish in flight when the connection drops is sent again on the
// next one, and [streams.Batch] sends a whole batch before waiting once for
// its confirms.
package rabbitmq
