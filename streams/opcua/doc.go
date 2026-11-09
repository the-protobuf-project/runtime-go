// Package opcua delivers [streams] over OPC UA.
//
// OPC UA is not a message broker, and this provider does not pretend it is. A
// server holds an address space of nodes, each with a current value; a client
// writes values and monitors them for change. That maps onto the contract
// closely enough to be useful and differently enough to be worth stating:
//
//   - A subject is a node id in string form — `ns=2;s=Line1.Temperature`, or
//     `nsu=urn:plant;i=1001` naming the namespace by URI — matched literally.
//     Subjects are parsed when a stream is declared, so a malformed id fails at
//     [streams.Streams.Create] with [streams.ErrUnsupported] rather than at the
//     first publish.
//   - Publishing writes the node's Value attribute and returns once the server
//     has answered. A batch is one Write per value, each answered before the
//     next: a server applies the values of one Write request in an order it
//     chooses (Part 4, 5.11.4.1), so one request could leave the node holding
//     any of them rather than the last.
//   - Subscribing adds a monitored item to the one client subscription the
//     store keeps. The node's current value, which OPC UA reports first, is
//     skipped unless [WithInitialValue] asks for it; each change follows.
//
// A namespace index is a position in the server's namespace table and can
// change when the server's configuration does; a URI is stable. A subject
// naming its namespace by URI is resolved when the stream is bound, so a
// namespace the server lacks fails [streams.Streams.Bind], and again on each
// new session, so a server that renumbers its namespaces when it restarts is
// still written to, and monitored on, the node the URI names.
//
// # State, not events
//
// A node holds one value, so what a subscriber sees is a series of states, not
// a log of every write. A monitored item reports on a change of status, value
// or source timestamp, so a write that repeats the last value is still a
// message; but a server samples rather than queues, and writes that land
// faster than the sampling interval can be coalesced into the latest.
// [streams.Prefetch] sets the monitored item's queue size ([WithQueueSize]
// without it), which is how much of that a server will hold between
// publishing intervals.
//
// For the same reason there is no [streams.Durable] and no
// [streams.Positioned]: nothing is lent, so there is nothing to acknowledge,
// and history access is a server feature this package does not lean on.
// [streams.Group] and [streams.TTL] are refused by name, and so are headers
// ([streams.Stream.Headers] at Create and Update, and a publish's own),
// [streams.Expires] and [streams.Retain]: a Write sets a node's value, status
// and timestamps and has nowhere to carry headers, applies at once or fails so
// nothing waits that could expire, and a node always holds its last value with
// no retain flag to set. OPC UA PubSub carries headers as MQTT 5 user
// properties.
//
// # One subscription, and reconnects
//
// OPC UA servers limit subscriptions per session and charge per subscription
// for every publishing cycle, so a store keeps one, created with the first
// Subscribe and deleted with the last; canceling a Subscribe's context deletes
// its item. Close ends every subscription's channel and deletes the
// subscription, and leaves a client given to [Use] connected.
//
// When the connection drops, gopcua reconnects and transfers or recreates the
// subscription with its items; the channels carry on across that, and a write
// made while the session is down fails rather than waits. After a recreate the
// server reports each item's current value, and that one is delivered: it is
// what was written while the session was gone. A channel holds [WithBuffer]
// notifications ahead of its reader and drops the newest past that, so one
// stalled reader cannot stall the session.
//
// # Native values
//
// A node has a type, and the programs around an OPC UA server — PLCs, HMIs,
// historians — read that type, not this module's frame. So a value OPC UA has
// a type for (bool, sized integers, floats, string, []byte, time.Time and
// slices of them) is written as itself, and read back by
// [streams.Message.Decode] into the matching Go type. Anything else — a struct,
// a protobuf message — is encoded with the configured codec and written as a
// ByteString, which only nodes of that type accept.
//
// A *ua.DataValue is written as given, its status and source timestamp
// included, as a gateway forwarding a device's reading would send it. A plain
// value carries no timestamps, and the server stamps the write with its own
// time.
//
// Go's int has no OPC UA counterpart, so it is refused rather than guessed at:
// an Int64 written to an Int32 node is rejected by the server anyway, and
// saying so at the call names the fix.
//
// A native value carries no message id, because nothing gave it one. Its
// [streams.Message.ID] is the source timestamp instead, which is what
// identifies a sample in OPC UA.
//
// # Licensing
//
// This package builds on github.com/gopcua/opcua, which is MIT licensed and
// compatible with this repository's Apache-2.0 license.
package opcua
