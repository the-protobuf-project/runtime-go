// Package nats delivers [streams] over NATS, in two forms.
//
// [Connect] is core NATS: a message goes to whoever is listening at that
// moment and is then gone. It satisfies [streams.Publisher] and
// [streams.Subscriber] and nothing beyond them, because that is all core NATS
// does — there is no log behind it to replay and no record of what anyone
// handled.
//
// [ConnectJetStream] is JetStream: the same subjects, backed by a stored log.
// It satisfies [streams.Durable] and [streams.Positioned] as well, so a
// consumer can be named, resume where that name left off, and have a message
// redelivered until it says the work is done.
//
// The two are separate constructors rather than one with a flag because the
// difference is not a setting. A program that believes it has at-least-once
// delivery and actually has at-most-once loses messages under exactly the
// conditions it added the durability for, and [streams.AsDurable] is what tells
// the two apart — it succeeds on the JetStream manager and refuses, by name, on
// the core one.
//
// # Wire compatibility
//
// Payloads are frames from the core package, a JetStream publish carries the
// message id in Nats-Msg-Id, and a JetStream stream keeps the fields its
// config has no room for under runtime-go.* metadata keys. A Go service and a
// runtime-rs one share a server, a stream, and a durable consumer.
//
// # Headers
//
// [streams.Headers] travel as NATS headers, on core publishes and JetStream
// ones alike, so nats sub --headers-only and nats stream view show them and
// the payload is untouched. A message without headers goes out exactly as
// before: a plain PUB on core NATS, and only Nats-Msg-Id on JetStream. The
// Nats- prefix is the server's (Nats-Msg-Id, Nats-TTL and Nats-Expected-*
// change what it does with a message), so a key starting with it is refused
// with [streams.ErrInvalidHeader] at Create, Update and every publish, and
// Nats- headers on a received message are left off [streams.Message.Headers].
//
// A stream's default headers live where the rest of its declaration does: in
// the process for core NATS, and in the JetStream stream's metadata under
// runtime-go.headers as one JSON object, so they survive a server restart and
// another process binding the stream stamps the same ones.
//
// # Expiry, retain, and delayed redelivery
//
//   - [streams.Expires]: JetStream sends it as the per-message Nats-TTL
//     header, which the server (2.11 or later) honors on a stream with
//     allow_msg_ttl, which every stream this package creates or updates has.
//     The server counts whole seconds, so an expiry is rounded up to the next
//     second and one under a second is refused. Core NATS refuses it: nothing
//     is held, so nothing can expire.
//   - [streams.Retain] is refused on both. Core NATS keeps nothing; JetStream's
//     only last-value form is a stream capped at one message per subject,
//     which would discard the log every durable consumer is reading.
//   - [streams.Delivery.NakAfter] on JetStream is a -NAK with a delay: the
//     server holds the redelivery back. Core NATS has no deliveries to settle.
package nats
