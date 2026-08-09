// Package fabric is a placeholder database.Driver for Hyperledger Fabric chaincode.
// It proves the backend seam generalizes past EVM: a Fabric driver would marshal
// the same record (via the store bridge) into chaincode invoke/query arguments,
// with writes returning a pending database.WriteResult until the transaction is
// committed, exactly as the evm driver does. The methods are stubs returning
// [database.ErrUnimplemented] until that bridge lands.
//
// # Example
//
// The stub already satisfies [database.Driver], so it drops into the same wiring as
// any other backend — every call currently returns database.ErrUnimplemented:
//
//	svc := adapter.New(fabric.New(), database.NewRegistry(grpcx.Resources...))
//	_, err := svc.Get(ctx, "Book", "books/dune") // errors.Is(err, database.ErrUnimplemented)
package fabric
