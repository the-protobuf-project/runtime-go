package runtime

import (
	"context"

	"github.com/the-protobuf-project/runtime-go/network"
)

// BatchOp is one mutation queued in a Tx (a root field, its arguments, and the typed result it
// fills). Generated mutation handlers build these via their <Method>Op methods; callers rarely
// construct one directly.
type BatchOp = network.BatchOp

// Tx batches several mutations into one GraphQL document so they commit atomically — the engine
// runs all of them in a single transaction (all succeed or all roll back), removing the need for
// best-effort compensation when a write spans tables. Build one from the root mutation handler,
// queue ops, then Commit:
//
//	var promo  schemaql.InsertPromocodeResourceResponse
//	var amount schemaql.InsertBookingMoneyResponse
//	tx := svc.Mutation.Tx()
//	tx.Add(svc.Mutation.Promocode.Resource.CreateOp(promoInput, &promo))
//	tx.Add(svc.Mutation.Booking.Money.CreateOp(amountInput, &amount))
//	if err := tx.Commit(ctx); err != nil { /* nothing was written */ }
//	// promo and amount are now filled.
type Tx struct {
	gql *GraphQLClient
	ops []BatchOp
}

// NewTx returns a Tx that will commit through gql.
func NewTx(gql *GraphQLClient) *Tx { return &Tx{gql: gql} }

// Add queues a mutation op (returned by a handler's <Method>Op) and returns the Tx for chaining.
func (t *Tx) Add(op BatchOp) *Tx {
	t.ops = append(t.ops, op)
	return t
}

// Len reports how many ops are queued.
func (t *Tx) Len() int { return len(t.ops) }

// Commit runs every queued op as one atomic GraphQL mutation and fills each op's result pointer.
// A nil/empty Tx commits nothing and returns nil. On error no result pointer is written.
func (t *Tx) Commit(ctx context.Context) error {
	if t == nil || len(t.ops) == 0 {
		return nil
	}
	return (<-t.gql.BatchMutate(ctx, t.ops)).Error
}
