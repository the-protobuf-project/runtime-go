// Command opcua demonstrates the streams contract over OPC UA.
//
// It starts gopcua's server in this process, with two nodes, and talks to it
// over a real connection. It needs nothing running.
//
// It shows what makes OPC UA unlike the brokers: a subject is a node holding
// one value, so a subscriber is handed the node's current state on attach and
// each change after it. A temperature written as a float64 stays a Double on
// the server, readable by anything that speaks OPC UA, while a struct travels
// framed in a ByteString. And because nothing is lent, both [streams.AsDurable]
// and [streams.AsPositioned] refuse by name.
package main
