// Command zeromq demonstrates the streams contract over ZeroMQ PUB/SUB.
//
// There is no broker: this program binds a socket and connects to it in one
// process, where a real deployment would have two. It needs nothing running.
//
// It also shows the limit — ZeroMQ keeps nothing, so both [streams.AsDurable]
// and [streams.AsPositioned] refuse by name.
package main
