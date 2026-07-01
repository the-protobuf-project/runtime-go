// Package fabric is a placeholder store.Driver for Hyperledger Fabric chaincode.
// It proves the backend seam generalizes past EVM: a Fabric driver would marshal
// the same record (via the store bridge) into chaincode invoke/query arguments,
// with writes returning a pending store.WriteResult until the transaction is
// committed, exactly as the evm driver does. The methods are stubs returning
// [store.ErrUnimplemented] until that bridge lands.
//
// # Example
//
// The stub already satisfies [store.Driver], so it drops into the same wiring as
// any other backend — every call currently returns store.ErrUnimplemented:
//
//	svc := adapter.New(fabric.New(), store.NewRegistry(grpcx.Resources...))
//	_, err := svc.Get(ctx, "Book", "books/dune") // errors.Is(err, store.ErrUnimplemented)
package fabric
