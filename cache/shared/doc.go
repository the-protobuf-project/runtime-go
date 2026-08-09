// Package shared holds the telemetry this module's code logs and measures
// through.
//
// It is a thin binding: [github.com/the-protobuf-project/runtime-go/observability]
// does the work of standing up the backend, and this only names the service so
// cache records are distinguishable from a sibling module's.
//
// Nothing here is required. The cache package takes a [telemetry.Logger] and
// [telemetry.Meter] through its own config and decorators, so an application
// that wires its own — or none — never imports this package and never links the
// OTel SDK behind it. This exists so a caller who just wants it to work can
// write one line.
package shared
