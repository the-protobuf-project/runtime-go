// Package opcua delivers [streams] over OPC UA.
//
// OPC UA is not a message broker, and this provider does not pretend it is. A
// server holds an address space of nodes, each with a current value; a client
// writes values and monitors them for change. That maps onto the contract
// closely enough to be useful and differently enough to be worth stating:
//
//   - A subject is a node id — `ns=2;s=Line1.Temperature` — matched literally.
//     Subjects are parsed when a stream is declared, so a malformed id fails at
//     [streams.Streams.Create] rather than at the first publish.
//   - Publishing writes the node's Value attribute.
//   - Subscribing creates a monitored item, which reports the node's current
//     value first and then each change. A subscriber therefore learns the state
//     of a node on attach, which no other provider here does.
//
// # State, not events
//
// A node holds one value, so what a subscriber sees is a series of states, not
// a log of every write. Writing the value a node already holds is not a change
// and is not reported, and a server samples rather than queues: writes that
// land faster than the sampling interval can be coalesced into the latest.
// [streams.Prefetch] sets the monitored item's queue size, which is how much of
// that a server will hold between publishing intervals.
//
// For the same reason there is no [streams.Durable] and no
// [streams.Positioned]: nothing is lent, so there is nothing to acknowledge,
// and history access is a server feature this package does not lean on.
// [streams.Group] and [streams.TTL] are refused by name.
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
