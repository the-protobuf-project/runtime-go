package evm

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/store"
)

// errNoSigner is returned by a write when the driver has no signer configured.
var errNoSigner = errors.New("evm: no signer configured; writes are disabled")

// Driver is a store.Driver backed by EVM storage contracts. It encodes records
// dynamically from each resource's ABI — no per-contract binding.
type Driver struct {
	cfg      Config
	verified sync.Map // resource name -> struct{} once its SCHEMA_VERSION checks out
}

// New returns a Driver wired by cfg.
func New(cfg Config) *Driver { return &Driver{cfg: cfg} }

// compile-time proof the chain engine satisfies the backend-agnostic contract.
var _ store.Driver = (*Driver)(nil)

// bound resolves a resource's deployed contract and a bound handle for calls,
// verifying the contract's schema fingerprint on first use when configured.
func (d *Driver) bound(ctx context.Context, res *store.Resource) (Contract, *bind.BoundContract, error) {
	c, ok := d.cfg.Resources[res.Name]
	if !ok {
		return Contract{}, nil, fmt.Errorf("evm: no contract configured for resource %q", res.Name)
	}
	b := bind.NewBoundContract(c.Address, c.ABI, d.cfg.Backend, d.cfg.Backend, d.cfg.Backend)
	if err := d.verifySchema(ctx, res, b); err != nil {
		return Contract{}, nil, err
	}
	return c, b, nil
}

// verifySchema reads the contract's SCHEMA_VERSION once per resource and refuses
// a layout that has drifted from the generated client.
func (d *Driver) verifySchema(ctx context.Context, res *store.Resource, b *bind.BoundContract) error {
	if !d.cfg.VerifySchema || res.SchemaVersion == "" {
		return nil
	}
	if _, ok := d.verified.Load(res.Name); ok {
		return nil
	}
	var out []any
	if err := b.Call(&bind.CallOpts{Context: ctx}, &out, "SCHEMA_VERSION"); err != nil {
		return fmt.Errorf("evm: reading SCHEMA_VERSION for %q: %w", res.Name, err)
	}
	onchain, _ := out[0].([32]byte)
	if !schemaMatches(onchain, res.SchemaVersion) {
		return fmt.Errorf("evm: resource %q schema drift: contract 0x%x != client %s", res.Name, onchain, res.SchemaVersion)
	}
	d.verified.Store(res.Name, struct{}{})
	return nil
}

// schemaMatches reports whether an on-chain 32-byte fingerprint equals the
// client's 0x-prefixed hex fingerprint.
func schemaMatches(onchain [32]byte, want string) bool {
	b, err := hex.DecodeString(strings.TrimPrefix(want, "0x"))
	if err != nil || len(b) != 32 {
		return false
	}
	return bytes.Equal(onchain[:], b)
}

func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	_, b, err := d.bound(ctx, res)
	if err != nil {
		return nil, err
	}
	var out []any
	if err := b.Call(&bind.CallOpts{Context: ctx}, &out, "get", key); err != nil {
		return nil, d.classify(err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("evm: get(%q) returned no value", key)
	}
	cols, err := decodeRecord(res, out[0])
	if err != nil {
		return nil, err
	}
	return store.ColumnsToMessage(res, cols)
}

func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	_, b, err := d.bound(ctx, res)
	if err != nil {
		return false, err
	}
	var out []any
	if err := b.Call(&bind.CallOpts{Context: ctx}, &out, "exists", key); err != nil {
		return false, d.classify(err)
	}
	ok, _ := out[0].(bool)
	return ok, nil
}

func (d *Driver) Count(ctx context.Context, res *store.Resource, _ store.ListOptions) (int64, error) {
	_, b, err := d.bound(ctx, res)
	if err != nil {
		return 0, err
	}
	var out []any
	if err := b.Call(&bind.CallOpts{Context: ctx}, &out, "count", nil); err != nil {
		return 0, d.classify(err)
	}
	n, _ := out[0].(*big.Int)
	if n == nil {
		return 0, nil
	}
	return n.Int64(), nil
}

// List reads every record via the on-chain list() and paginates client-side.
// For large datasets point reads at the generated subgraph instead; the contract
// list() is bounded by node eth_call limits.
func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	_, b, err := d.bound(ctx, res)
	if err != nil {
		return store.ListResult{}, err
	}
	var out []any
	if err := b.Call(&bind.CallOpts{Context: ctx}, &out, "list"); err != nil {
		return store.ListResult{}, d.classify(err)
	}
	slice := reflect.ValueOf(out[0])
	if slice.Kind() != reflect.Slice {
		return store.ListResult{}, fmt.Errorf("evm: list() did not return an array")
	}
	total := int64(slice.Len())
	offset, limit := page(opts, int(total))
	items := make([]proto.Message, 0, limit)
	for i := offset; i < offset+limit; i++ {
		cols, err := decodeRecord(res, slice.Index(i).Interface())
		if err != nil {
			return store.ListResult{}, err
		}
		msg, err := store.ColumnsToMessage(res, cols)
		if err != nil {
			return store.ListResult{}, err
		}
		items = append(items, msg)
	}
	next := ""
	if int64(offset+limit) < total {
		next = fmt.Sprintf("%d", offset+limit)
	}
	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	return d.write(ctx, res, msg, "create", true)
}

func (d *Driver) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	return d.write(ctx, res, msg, "update", false)
}

func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
	_, b, err := d.bound(ctx, res)
	if err != nil {
		return err
	}
	if d.cfg.Signer == nil {
		return errNoSigner
	}
	tx, err := b.Transact(d.txOpts(ctx), "remove", key)
	if err != nil {
		return d.classify(err)
	}
	_, err = d.await(ctx, tx)
	return err
}

// write encodes msg and submits a create/update transaction.
func (d *Driver) write(ctx context.Context, res *store.Resource, msg proto.Message, method string, onCreate bool) (store.WriteResult, error) {
	c, b, err := d.bound(ctx, res)
	if err != nil {
		return store.WriteResult{}, err
	}
	if d.cfg.Signer == nil {
		return store.WriteResult{}, errNoSigner
	}
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	fillManaged(res, cols, onCreate)
	tt, err := inputTuple(c.ABI, method)
	if err != nil {
		return store.WriteResult{}, err
	}
	record, err := encodeRecord(tt, res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	tx, err := b.Transact(d.txOpts(ctx), method, record)
	if err != nil {
		return store.WriteResult{}, d.classify(err)
	}
	pending, err := d.await(ctx, tx)
	if err != nil {
		return store.WriteResult{}, err
	}
	wr := store.WriteResult{Message: msg, Pending: pending}
	if pending {
		wr.Metadata = map[string]string{"txHash": tx.Hash().Hex(), "operation": "operations/" + tx.Hash().Hex()}
	}
	return wr, nil
}

// await returns whether the write is still pending. When AwaitReceipt is set it
// blocks for the receipt (returning an error on a reverted transaction) and
// reports not-pending; otherwise it returns pending immediately.
func (d *Driver) await(ctx context.Context, tx *types.Transaction) (pending bool, err error) {
	if !d.cfg.AwaitReceipt {
		return true, nil
	}
	rcpt, err := bind.WaitMined(ctx, d.cfg.Backend, tx)
	if err != nil {
		return false, fmt.Errorf("evm: awaiting receipt for %s: %w", tx.Hash().Hex(), err)
	}
	if rcpt.Status != types.ReceiptStatusSuccessful {
		return false, fmt.Errorf("evm: transaction %s reverted", tx.Hash().Hex())
	}
	return false, nil
}

// txOpts clones the configured signer with the call context.
func (d *Driver) txOpts(ctx context.Context) *bind.TransactOpts {
	o := *d.cfg.Signer
	o.Context = ctx
	return &o
}

// classify turns a contract call/transaction error into a store sentinel when it
// carries a recognized revert reason.
func (d *Driver) classify(err error) error {
	return revertToError(err, revertReason(err))
}

// revertReason extracts a Solidity revert string from a go-ethereum error, via
// the structured revert data when present, else the error text.
func revertReason(err error) string {
	if err == nil {
		return ""
	}
	type dataError interface{ ErrorData() any }
	if de, ok := err.(dataError); ok {
		if hexData, ok := de.ErrorData().(string); ok {
			if b, e := hexutil.Decode(hexData); e == nil {
				if reason, e2 := abi.UnpackRevert(b); e2 == nil {
					return reason
				}
			}
		}
	}
	const marker = "execution reverted:"
	if i := strings.Index(err.Error(), marker); i >= 0 {
		return strings.TrimSpace(err.Error()[i+len(marker):])
	}
	return ""
}

// page resolves an offset-based page window from opts against total.
func page(opts store.ListOptions, total int) (offset, limit int) {
	limit = int(opts.PageSize)
	if limit <= 0 || limit > total {
		limit = total
	}
	if opts.PageToken != "" {
		fmt.Sscanf(opts.PageToken, "%d", &offset)
	}
	if offset > total {
		offset = total
	}
	if offset+limit > total {
		limit = total - offset
	}
	return offset, limit
}
