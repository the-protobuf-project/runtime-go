package arangodb

import (
	"context"
	"fmt"

	"github.com/arangodb/go-driver/v2/arangodb"
	arangoshared "github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/the-protobuf-project/runtime-go/database"
)

var _ database.Transactional = (*Driver)(nil)

// Run executes fn inside a stream transaction, committing when it returns nil
// and rolling back on any error or panic.
//
// # Why every collection is declared
//
// ArangoDB wants to be told which collections a transaction will touch before it
// starts, and the contract here does not know — the body decides as it runs. So
// this declares every collection in the database as writable.
//
// That is affordable rather than reckless: on RocksDB, which is the only engine
// modern ArangoDB has, a declared collection is not locked when the transaction
// begins but when it is first written, so declaring one the body never touches
// costs nothing. It would be a different trade on the old MMFiles engine, which
// took the locks up front.
//
// The alternative was to make the caller declare collections, which would put an
// ArangoDB detail into a contract that four other backends share.
func (d *Driver) Run(ctx context.Context, fn func(tx *database.DB) error) error {
	if d.tx != nil {
		return fmt.Errorf("arangodb: already inside a transaction")
	}
	db, err := d.database(ctx, nil)
	if err != nil {
		return err
	}

	names, err := d.collectionNames(ctx, db)
	if err != nil {
		return err
	}

	tx, err := db.BeginTransaction(ctx, arangodb.TransactionCollections{Write: names}, nil)
	if err != nil {
		return fmt.Errorf("arangodb: cannot begin a transaction: %w", err)
	}

	bound := &Driver{client: d.client, dbName: d.dbName, registry: d.registry, tx: tx}
	committed := false
	defer func() {
		if committed {
			return
		}
		// Abort on the way out of any path that did not commit, including a
		// panic — an abandoned stream transaction holds its snapshot on the
		// server until it times out.
		_ = tx.Abort(context.WithoutCancel(ctx), nil)
	}()

	if ferr := fn(database.Build(bound, "arangodb", d.dbName, nil)); ferr != nil {
		return ferr
	}
	if cerr := tx.Commit(ctx, nil); cerr != nil {
		return fmt.Errorf("arangodb: cannot commit: %w", cerr)
	}
	committed = true
	return nil
}

// collectionNames lists the non-system collections in a database.
func (d *Driver) collectionNames(ctx context.Context, db arangodb.Database) ([]string, error) {
	colls, err := db.Collections(ctx)
	if err != nil {
		return nil, fmt.Errorf("arangodb: cannot list collections: %w", err)
	}
	names := make([]string, 0, len(colls))
	for _, c := range colls {
		if props, perr := c.Properties(ctx); perr == nil && props.IsSystem {
			continue
		}
		names = append(names, c.Name())
	}
	return names, nil
}

// EnsureSchema creates the collection res describes and the indexes its columns
// imply, and is safe to call repeatedly.
//
// The unique index is the part that cannot be deferred. ArangoDB would create
// the collection on first write, but a column the descriptor marks Unique is
// only unique if something enforces it — and a descriptor claiming a constraint
// the store does not have is the quiet lie this contract exists to avoid.
func (d *Driver) EnsureSchema(ctx context.Context, res *database.Resource) error {
	if res == nil {
		return fmt.Errorf("arangodb: EnsureSchema needs a resource")
	}
	db, err := d.database(ctx, res)
	if err != nil {
		return err
	}
	// An edge resource needs an edge collection, not a document one. ArangoDB
	// will not let a graph definition name a document collection as an edge, and
	// the failure surfaces at EnsureGraph rather than here — so the descriptor's
	// own IsEdge decides it at creation.
	props := &arangodb.CreateCollectionPropertiesV2{}
	if res.IsEdge {
		edge := arangodb.CollectionTypeEdge
		props.Type = &edge
	}
	if _, cerr := db.CreateCollectionV2(ctx, res.Table, props); cerr != nil && !arangoshared.IsConflict(cerr) {
		return fmt.Errorf("arangodb: cannot create %s: %w", res.Table, cerr)
	}

	coll, err := db.GetCollection(ctx, res.Table, &arangodb.GetCollectionOptions{SkipExistCheck: true})
	if err != nil {
		return fmt.Errorf("arangodb: cannot open %s: %w", res.Table, err)
	}
	for _, c := range res.Columns {
		if !c.Unique || c.PrimaryKey {
			continue // _key is unique by construction
		}
		name := "uniq_" + c.Name
		unique := true
		if _, _, ierr := coll.EnsurePersistentIndex(ctx, []string{c.Name},
			&arangodb.CreatePersistentIndexOptions{Unique: &unique, Name: name}); ierr != nil {
			return fmt.Errorf("arangodb: cannot create the unique index on %s: %w", c.Name, ierr)
		}
	}
	return nil
}

// DropSchema removes the collection res describes, and everything in it.
func (d *Driver) DropSchema(ctx context.Context, res *database.Resource) error {
	if res == nil {
		return fmt.Errorf("arangodb: DropSchema needs a resource")
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return err
	}
	if rerr := coll.Remove(ctx); rerr != nil {
		if arangoshared.IsNotFound(rerr) {
			return nil
		}
		return fmt.Errorf("arangodb: cannot drop %s: %w", res.Table, rerr)
	}
	return nil
}

// HasSchema reports whether the collection res describes is already there.
func (d *Driver) HasSchema(ctx context.Context, res *database.Resource) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("arangodb: HasSchema needs a resource")
	}
	db, err := d.database(ctx, res)
	if err != nil {
		return false, err
	}
	ok, eerr := db.CollectionExists(ctx, res.Table)
	if eerr != nil {
		return false, fmt.Errorf("arangodb: cannot check %s: %w", res.Table, eerr)
	}
	return ok, nil
}
