// Package zeromq delivers [streams] over ZeroMQ PUB/SUB.
//
// It satisfies [streams.Publisher] and [streams.Subscriber] and nothing beyond
// them. There is no broker here at all: a publisher binds a socket and
// subscribers connect to it directly, so there is nowhere for a message to be
// stored, no record of what anyone handled, and nothing to replay.
// [streams.AsDurable] and [streams.AsPositioned] both refuse by name.
//
// # Subscribe is best-effort, not race-free
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
// Both implement [streams.Closer]. Close releases the sockets and ends every
// subscription opened through the provider; a publish or subscribe after it
// fails. A publisher bound to a TCP port of 0 reports the port the system
// chose through [Endpoint].
//
// # Wire layout
//
// Two frames: the topic "<stream id>|<subject>", then the packed message. The
// topic is a frame of its own so a SUB socket filters on it without touching
// the payload. ZeroMQ's filter is a byte prefix, so a subscription also
// compares the whole topic frame: "user" never receives "users".
//
// # Headers, expiry, retain
//
// ZeroMQ frames carry bytes and nothing else, so a message's [streams.Headers]
// (the stream's defaults with the publish's own on top) ride inside the
// payload frame, as version 2 of the frame (core.PackWith). A message
// without headers is still a version-1 frame, byte for byte what this package
// always wrote. A stream's default headers live in its declaration, so each
// side stamps the defaults it declared, and Create and Update refuse invalid
// ones.
//
// [streams.Expires] and [streams.Retain] are refused with
// [streams.ErrUnsupported]: there is no queue to expire a message from and
// nothing kept to hand a later subscriber. With no deliveries to settle there
// is no delayed nak either.
package zeromq
