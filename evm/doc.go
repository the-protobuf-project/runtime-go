// Package evm implements the backend-agnostic store.Driver against an EVM chain,
// driving the Solidity storage contracts protorm generates. It is the chain
// counterpart of the orm driver: the same dynamic philosophy, but the record is
// ABI-encoded for a contract call instead of mapped to SQL.
//
// Encoding is dynamic — there is no per-contract abigen binding. A driver is
// configured with each resource's contract ABI (the generated abis/<Contract>.json)
// and address; [encodeRecord] / [decodeRecord] move a proto message through the
// shared store bridge and the go-ethereum ABI codec, so one driver serves every
// resource.
//
// Read/write asymmetry is explicit. Reads (Get/Exists/Count/List) are synchronous
// eth_calls. Writes (Create/Update/Delete) submit a transaction and, by default,
// return a pending [store.WriteResult] carrying the tx hash — an AIP-151
// long-running operation handle — rather than blocking; set Config.AwaitReceipt
// to wait for the receipt and surface a synchronous result. A reverted call is
// translated back into the store sentinel errors via the contract's require
// strings (see [revertToError]).
//
// On-chain integration (deploying a contract, sending real transactions) is
// verified against a local devnet such as anvil; the ABI codec and revert
// mapping are unit-tested here without a network.
//
// # Example
//
// Swap the backend without touching the proto API — the same registry and
// adapter, now driven by a Solidity contract instead of SQL:
//
//	book, _ := evm.NewContract(bookABIJSON, common.HexToAddress("0x…")) // abis/Book.json
//	driver := evm.New(evm.Config{
//	    Backend:   client, // *ethclient.Client
//	    Signer:    txOpts, // *bind.TransactOpts; nil disables writes
//	    Resources: map[string]evm.Contract{"Book": book},
//	})
//	svc := adapter.New(driver, store.NewRegistry(grpcx.Resources...))
package evm
