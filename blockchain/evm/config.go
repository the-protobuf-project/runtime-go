package evm

// config.go holds the driver's wiring: the chain backend, the signer, and the
// per-resource contract (parsed ABI + deployed address).

import (
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

// Backend is what the driver needs of a chain client: contract calls and
// transactions, plus receipt lookup for the await path. *ethclient.Client
// satisfies it.
type Backend interface {
	bind.ContractBackend
	bind.DeployBackend
}

// Contract pairs a resource's parsed ABI with its deployed address.
type Contract struct {
	ABI     abi.ABI
	Address common.Address
}

// NewContract parses an ABI JSON document (the generated abis/<Contract>.json)
// for a deployed address.
func NewContract(abiJSON string, address common.Address) (Contract, error) {
	parsed, err := parseABI(abiJSON)
	if err != nil {
		return Contract{}, err
	}
	return Contract{ABI: parsed, Address: address}, nil
}

// Config wires a Driver. Resources maps a resource name to its deployed
// contract. Signer is required for writes (build it from a private key with
// bind.NewKeyedTransactorWithChainID); a nil Signer disables Create/Update/Delete.
type Config struct {
	Backend   Backend
	Signer    *bind.TransactOpts
	Resources map[string]Contract

	// AwaitReceipt makes a write block until its transaction is mined and return a
	// synchronous result. The default (false) returns immediately with a pending
	// WriteResult carrying the tx hash — the long-running-operation handle.
	AwaitReceipt bool

	// VerifySchema makes the driver read each contract's SCHEMA_VERSION on first
	// use and refuse it if it differs from the resource's generated fingerprint —
	// catching a deployed contract whose immutable storage layout has drifted from
	// the client. Verified once per resource, then cached.
	VerifySchema bool
}
