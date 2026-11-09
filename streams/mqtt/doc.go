// Package mqtt delivers [streams] over MQTT 5.
//
// MQTT satisfies [streams.Publisher], [streams.Subscriber] and
// [streams.Durable], but not [streams.Positioned] — and that split is the
// interesting thing about it.
//
// A persistent session is real durability: a named client's subscriptions and
// its unacknowledged QoS 1 messages are kept by the broker while it is away, so
// a consumer that dies and comes back is handed what it missed and what it
// never finished. But a session is a queue, not a log. There is nothing behind
// it to seek, so there is no position but now, and [streams.AsPositioned]
// refuses by name.
//
// This is why the contract keeps Durable and Positioned apart. Redis Streams,
// JetStream and Kafka all have both; MQTT has exactly one of them, and a
// contract that had fused them would have had to either lie about replay or
// throw away durability it really has.
//
// # This provider dials
//
// A durable consumer *is* an MQTT session, identified by client id and
// negotiated when the connection opens, so every named consumer needs a
// connection of its own and [Connect] takes an address rather than a client.
// [Use] publishes through a connection you built, but its subscribers still
// dial their own.
//
// Addresses are host:port or a URL: mqtt://, tls://, mqtts://, ws:// or
// wss://. [WithTLS] covers TLS and mutual TLS, and [WithCredentials] a username
// and password.
//
// # Connections keep themselves up
//
// Every connection is an autopaho connection manager, so one that drops is
// dialed again. A publish waits for that for as long as its context allows. A
// subscription the broker kept nothing for is made again on the new
// connection, so an undurable subscriber misses only what was published while
// it was down; a durable consumer's session is resumed and misses nothing.
//
// # Publishing
//
// A publish returns once the broker has acknowledged it, or fails after
// [WithPublishTimeout] (10 seconds by default). [WithPublishConnections] spreads
// publishes over several connections by a hash of the topic, so a topic always
// takes the same one and keeps its order. A batch is pipelined: every message
// is put on the wire in order and the call waits for all the acknowledgements
// together, one round trip for the batch rather than one per message.
//
// # Headers, expiry, retain
//
//   - Headers travel as MQTT 5 user properties on the PUBLISH, one per header
//     in key order: the stream's [streams.Stream.Headers] with the publish's
//     own ([streams.Header], [streams.WithHeaders]) on top, on every message of
//     a batch too. The payload stays the version-1 frame, so a peer that knows
//     nothing of headers reads it unchanged, and any MQTT 5 tool sees them
//     (the broker's own subscriber, printing user properties). Subscriptions and durable
//     consumers put a message's user properties back on it as
//     [streams.Message.Headers], including on a redelivery after a restart,
//     since the broker stores them with the queued message. MQTT reserves no
//     user property names, so nothing is refused beyond
//     [streams.Headers.Validate]; Create and Update refuse invalid stream
//     headers.
//   - [streams.Expires] is the MQTT 5 Message Expiry Interval, in whole
//     seconds, rounded up: a sub-second expiry is one second, never zero. The
//     broker discards a message that has waited that long in a session's queue
//     or as a retained value; one handed to a connected subscriber at once is
//     delivered whatever its expiry.
//   - [streams.Retain] is the retain flag: the broker keeps the message as the
//     topic's last value and hands it, framed and decodable, to every
//     subscriber that joins later. A durable consumer is handed it when its
//     subscription is new, not again on each resume. [ClearRetained] clears it
//     with MQTT's own empty retained publish, which subscriptions skip.
//   - [streams.Delivery.NakAfter] cannot wait: MQTT has no negative
//     acknowledgement and no redelivery delay, so
//     [streams.Delivery.CanDelayRedelivery] is false and the message stays
//     outstanding until the session reconnects.
//
// # Licensing
//
// This package builds on github.com/eclipse/paho.golang, which is dual
// licensed: every source file carries `SPDX-License-Identifier: EPL-2.0 OR
// BSD-3-Clause`. It is used here under BSD-3-Clause, which is permissive and
// compatible with this repository's Apache-2.0 license. Note that the paho
// repository's root LICENSE file names only EPL-2.0, so a license scanner
// reading that file alone will report EPL.
package mqtt
