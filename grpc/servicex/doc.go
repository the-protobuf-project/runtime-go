// Package servicex is the shared runtime of the generated servers: the
// options every server takes, the error vocabulary they speak (AIP-193
// google.rpc.Status with an ErrorInfo reason), the request checks the standard
// methods share, and the change-event boundary a dispatcher implements.
//
// Generated servers import it from runtime-go rather than each run emitting
// its own copy. It speaks the standard outcome sentinels of grpc/problem,
// which runtime-go's repositories (database/repox) speak too, so a
// repository's not found is a server's not found. A store-backed server also
// hands its translator repox.ReferenceStatus; see NewTranslator.
package servicex
