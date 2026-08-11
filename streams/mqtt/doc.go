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
// Like the Kafka provider and unlike the others, this one takes an address
// rather than a client. A durable consumer *is* an MQTT session, identified by
// client id and negotiated when the connection opens, so every named consumer
// needs a connection of its own. It implements [streams.Closer] accordingly.
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
