// Package adapter is the generic gRPC layer between a protorm-generated service
// and a store.Driver. The generated per-service shims (target=grpc) are thin:
// they decode the proto-specific request, call one of this Service's
// resource-name-keyed helpers, and encode the response. Everything else —
// resolving the resource descriptor, dispatching to the driver, and translating
// store sentinel errors into gRPC status codes — lives here once, so it is
// identical across every resource and every backend.
//
// The adapter depends only on the store contract and grpc status/codes, never on
// a concrete backend or on the HybridServer; wire a service into a server with
// the existing srv.RegisterGRPC(...).
//
// # Example
//
// Build a [Service] from any driver plus the resource registry; the generated
// per-service shim calls its resource-name-keyed helpers:
//
//	reg := store.NewRegistry(grpcx.Resources...)
//	svc := adapter.New(orm.New(db), reg) // or evm.New(cfg), fabric.New(), ...
//
//	book, err := svc.Get(ctx, "Book", "books/dune") // ErrNotFound -> codes.NotFound
package adapter
