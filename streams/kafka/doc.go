// Package kafka delivers [streams] over Apache Kafka.
//
// Kafka is a stored log with consumer groups, so it satisfies the whole
// contract: [streams.Publisher], [streams.Subscriber], [streams.Durable] and
// [streams.Positioned]. It is also the only provider for which
// [streams.Options.PartitionKey] does anything — Kafka orders within a
// partition and nowhere else, so two messages that must be seen in order need
// the same key.
//
// # This provider dials
//
// The others take a client you built and never close it. This one takes seed
// brokers instead, because a Kafka consumer group is chosen when a client is
// constructed rather than when it reads: every named consumer needs a
// connection of its own. Handing this package one client would mean it could
// never honor a second [streams.Durable.Consume].
//
// It therefore implements [streams.Closer]. Close it when you are done with it,
// and cancel the contexts of anything consuming first. Close flushes every
// producer before releasing it.
//
// Options passed with [WithClientOptions] (TLS, SASL, timeouts) reach every
// client built here and are never changed, except that the producers get
// acks from every in-sync replica and idempotence when the options do not say
// otherwise. Publishes are spread over [WithProducers] producers, eight by
// default; a partition key always takes the same one.
//
// # A stream is a group of topics
//
// A stream declares subjects; each becomes its own Kafka topic, named
// <prefix>.<stream-id>.<subject> (<stream-id>.<subject> without a prefix). One topic per subject rather than one per stream,
// because a consumer group's offsets are per topic — sharing a topic across
// subjects would make a group that wants one subject read, and commit, all of
// them.
//
// The stream's own metadata lives in a compacted topic alongside them, since
// Kafka has nowhere on a topic to hang a description or a user id. Topic
// names, the metadata topic, its record keys and its JSON are shared with the
// Rust crate, so a Go and a Rust service declare and read the same streams on
// one cluster.
//
// # Headers, expiry, retain, delayed nak
//
// [streams.Headers] travel as Kafka record headers, each value its UTF-8
// bytes, on single publishes through every producer of the pool and on every
// record of a batch; the payload stays the version-1 frame, and a publish
// without headers writes no header block at all. Kafka's own tools see them
// (kafka-console-consumer.sh --property print.headers=true). Kafka reserves no
// header names, so only [streams.Headers.Validate] applies, at Create, Update
// and publish. On receipt a repeated key takes its last value, as Kafka's
// lastHeader does, a null value reads as empty, and a header another producer
// wrote that is not UTF-8 is skipped with a warning. A stream's default
// headers are part of its declaration in the metadata topic, so they survive a
// restart; a message's are in its record, so a redelivery or a replay brings
// them back.
//
// Kafka has no per-message expiry, so a non-zero [streams.Expires] is
// refused: a record stays until the topic's retention removes it.
// [streams.Retain] is refused too: the nearest thing, a compacted topic, keeps
// only the newest record per key for the whole subject, changing what every
// reader sees. [streams.Delivery.NakAfter] does not delay
// ([streams.Delivery.CanDelayRedelivery] is false): a partition is read in
// order, so holding one record back would hold back everything behind it.
package kafka
