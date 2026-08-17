// Package shared holds what every protocol runtime in this module would
// otherwise copy.
//
// There is one thing in that category so far, and it is the same in both
// protocols for the same reason: an agent call arrives over HTTP and leaves as
// a gRPC call, so whatever the backend authenticates on has to be carried
// across that boundary deliberately. [HeadersMiddleware] lifts the configured
// headers off the request, [ForwardMetadata] puts them back on the outgoing
// gRPC context, and the [HeaderMapping] between them is the caller's to name.
//
// Nothing here is protocol-specific, and nothing here should become so — a
// runtime's own vocabulary belongs in that runtime's package.
package shared
