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
// # Connections and channels
//
// [Connect] takes a URL — amqp:// or amqps://, with [WithTLS] for a private CA
// or mutual TLS and [WithCredentials] to keep a password out of the URL — and
// owns the connection it dials. It implements [streams.Closer]; close it when
// you are done, and cancel the contexts of anything consuming first. [Use]
// takes a connection you dialed instead, and opens its channels on that.
//
// A connection [Connect] dialed keeps itself up. One that drops is dialed
// again; a publish waits for it as long as its context allows, and every
// consumer attaches again on a fresh channel. A durable queue loses nothing in
// the gap, and what was delivered but not acknowledged is redelivered. A
// connection given to [Use] is not redialed: recovering it is its owner's job.
//
// # Publishes are confirmed
//
// The publishing channel is in confirm mode, so Publish returns once the
// broker has taken the message rather than once it has been written to a
// socket. A publish in flight when the connection drops is sent again on the
// next one, and [streams.Batch] sends a whole batch before waiting once for
// its confirms.
package rabbitmq
