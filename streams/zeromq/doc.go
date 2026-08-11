// Package zeromq delivers [streams] over ZeroMQ PUB/SUB.
//
// It satisfies [streams.Publisher] and [streams.Subscriber] and nothing beyond
// them. There is no broker here at all: a publisher binds a socket and
// subscribers connect to it directly, so there is nowhere for a message to be
// stored, no record of what anyone handled, and nothing to replay.
// [streams.AsDurable] and [streams.AsPositioned] both refuse by name.
//
// # Subscribe is best-effort, not raced-free
//
// [streams.Subscriber] promises that a subscription is active before Subscribe
// returns, so a value published afterwards is delivered rather than raced.
// **This provider approximates that promise and cannot guarantee it.**
//
// ZeroMQ's subscription travels from subscriber to publisher asynchronously and
// is not acknowledged, so there is no event to wait on — this is the
// slow-joiner problem, and it is a property of the protocol rather than a gap
// in this package. Messages published before the subscription arrives are
// dropped silently by the publisher, which does not yet know anyone wants them.
//
// Subscribe waits [WithSettle] before returning to make the window small.
// Raise it on a slow or long-haul link. Where a message must not be missed,
// reach for a provider with a broker behind it — every other one in this
// module qualifies.
//
// # This provider binds
//
// A stream is a socket, not a name on a server. The provider is told an
// endpoint and given a role: [Publish] binds it and [Subscribe] connects to it.
// That asymmetry is ZeroMQ's, not this contract's, and it is why the two are
// separate constructors — a process that calls the wrong one gets a silent
// no-op rather than an error, so the choice is made where it can be seen.
//
// Both implement [streams.Closer]. Close releases the sockets.
package zeromq
