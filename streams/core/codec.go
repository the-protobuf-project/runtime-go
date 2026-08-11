package core

import (
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Resolve turns a provider's configured codec into the pair every provider
// needs: the one to publish with, and the registry to decode through.
//
// They are not the same thing. A program publishes with one codec but must read
// whatever its peers wrote, so the registry holds that codec *and* JSON — which
// is why an unconfigured provider still reads a JSON message from a program
// that never changed the default.
func Resolve(codec streams.Codec) (streams.Codec, *streams.Registry) {
	if codec == nil {
		codec = streams.JSON
	}
	return codec, streams.NewRegistry(codec)
}

// ResolveAll is [Resolve] plus the metrics every provider reports through, so a
// constructor wires the whole encoding-and-measurement half in one line.
func ResolveAll(codec streams.Codec, meter telemetry.Meter) (streams.Codec, *streams.Registry, *Metrics) {
	c, r := Resolve(codec)
	return c, r, NewMetrics(meter)
}
