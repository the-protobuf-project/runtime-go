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
// and cancel the contexts of anything consuming first.
//
// # A stream is a group of topics
//
// A stream declares subjects; each becomes its own Kafka topic, named
// <stream-id>.<subject>. One topic per subject rather than one per stream,
// because a consumer group's offsets are per topic — sharing a topic across
// subjects would make a group that wants one subject read, and commit, all of
// them.
//
// The stream's own metadata lives in a compacted topic alongside them, since
// Kafka has nowhere on a topic to hang a description or a user id.
package kafka
