package core

import "github.com/the-protobuf-project/runtime-go/streams"

// DefaultPrefetch is how many messages a provider holds ahead of the reader
// when the caller does not say.
//
// It is not zero, and that is the point. An unbuffered hand-off means a slow
// handler blocks the goroutine feeding it, and on a provider whose
// subscriptions share one connection — Redis pub/sub, MQTT — that one slow
// reader stalls every other subscription on it. A small buffer decouples them
// without holding so much that a crash redelivers a great deal of work.
const DefaultPrefetch = 64

// Prefetch resolves how deep a delivery channel should be.
//
// A negative value is treated as unbuffered rather than rejected: a caller who
// asks for no buffering at all wants the hand-off to be synchronous, and that
// is a legitimate thing to want when ordering matters more than throughput.
func Prefetch(o streams.Options) int {
	switch {
	case o.Prefetch > 0:
		return o.Prefetch
	case o.Prefetch < 0:
		return 0
	default:
		return DefaultPrefetch
	}
}
