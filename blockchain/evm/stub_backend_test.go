package evm_test

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// errUnexpectedCall is returned by every stubBackend method. These tests cover
// the registry boundary only — constructing a driver must not talk to a chain —
// so a call reaching the backend means the test is exercising more than it
// intends, and should fail rather than proceed on a zero value.
var errUnexpectedCall = errors.New("evm_test: stub backend was called")

// stubBackend satisfies evm.Backend (bind.ContractBackend + bind.DeployBackend)
// without talking to a chain. The registry tests only need a non-nil backend to
// construct a driver.
type stubBackend struct{}

// ContractCaller.
func (stubBackend) CodeAt(context.Context, common.Address, *big.Int) ([]byte, error) {
	return nil, errUnexpectedCall
}
func (stubBackend) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	return nil, errUnexpectedCall
}

// ContractTransactor.
func (stubBackend) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return nil, errUnexpectedCall
}
func (stubBackend) PendingCodeAt(context.Context, common.Address) ([]byte, error) {
	return nil, errUnexpectedCall
}
func (stubBackend) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	return 0, errUnexpectedCall
}
func (stubBackend) SuggestGasPrice(context.Context) (*big.Int, error) {
	return nil, errUnexpectedCall
}
func (stubBackend) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return nil, errUnexpectedCall
}
func (stubBackend) EstimateGas(context.Context, ethereum.CallMsg) (uint64, error) {
	return 0, errUnexpectedCall
}
func (stubBackend) SendTransaction(context.Context, *types.Transaction) error {
	return errUnexpectedCall
}
func (stubBackend) TransactionByHash(context.Context, common.Hash) (*types.Transaction, bool, error) {
	return nil, false, errUnexpectedCall
}

// ContractFilterer.
func (stubBackend) FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	return nil, errUnexpectedCall
}
func (stubBackend) SubscribeFilterLogs(context.Context, ethereum.FilterQuery, chan<- types.Log) (ethereum.Subscription, error) {
	return nil, errUnexpectedCall
}

// DeployBackend.
func (stubBackend) TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return nil, errUnexpectedCall
}
