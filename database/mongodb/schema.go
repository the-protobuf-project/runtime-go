package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Run executes fn inside a transaction, committing when it returns nil and
// rolling back on any error or panic.
//
// # It needs a replica set
//
// MongoDB has no multi-document transactions on a standalone server — not a
// slower version, none at all — so this reports the driver's own error there
// rather than pretending. A single-node replica set is enough and is what the
// compose file in this module runs, precisely so that a program tested here
// behaves the same in production.
func (d *Driver) Run(ctx context.Context, fn func(*store.DB) error) error {
	session, err := d.client.StartSession()
	if err != nil {
		return fmt.Errorf("mongodb: cannot start a session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sc context.Context) (any, error) {
		// The driver handed to fn carries the session in its context, so every
		// operation inside joins the transaction. It keeps the same database
		// selection, so a call inside behaves exactly as it would outside except
		// for when it becomes visible.
		bound := &sessionDriver{Driver: d, ctx: sc}
		return nil, fn(store.Build(bound, "mongodb", d.db, nil))
	})
	if err != nil {
		return err
	}
	return nil
}

// sessionDriver binds every operation to one transaction's context.
//
// The context a caller passes to a method inside the transaction body is
// deliberately ignored in favor of the session's: a write that ran on the
// caller's context would silently execute outside the transaction, commit
// separately, and survive a rollback.
type sessionDriver struct {
	*Driver
	ctx context.Context
}

func (s *sessionDriver) Create(_ context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	return s.Driver.Create(s.ctx, res, msg)
}

func (s *sessionDriver) Get(_ context.Context, res *store.Resource, key string) (proto.Message, error) {
	return s.Driver.Get(s.ctx, res, key)
}

func (s *sessionDriver) Update(_ context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	return s.Driver.Update(s.ctx, res, msg)
}

func (s *sessionDriver) Delete(_ context.Context, res *store.Resource, key string) error {
	return s.Driver.Delete(s.ctx, res, key)
}

func (s *sessionDriver) List(_ context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	return s.Driver.List(s.ctx, res, opts)
}

func (s *sessionDriver) Count(_ context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	return s.Driver.Count(s.ctx, res, opts)
}

func (s *sessionDriver) Exists(_ context.Context, res *store.Resource, key string) (bool, error) {
	return s.Driver.Exists(s.ctx, res, key)
}

// EnsureSchema creates the collection res describes and the indexes its columns
// imply, and is safe to call repeatedly.
//
// MongoDB would create the collection on first write anyway; what cannot be
// deferred is the unique index. A column the descriptor marks Unique is only
// unique if something enforces it, and a descriptor that claims a constraint the
// store does not have is exactly the quiet lie this contract exists to avoid.
func (d *Driver) EnsureSchema(ctx context.Context, res *store.Resource) error {
	if res == nil {
		return fmt.Errorf("mongodb: EnsureSchema needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return err
	}

	db := coll.Database()
	if cerr := db.CreateCollection(ctx, res.Table); cerr != nil && !isNamespaceExists(cerr) {
		return fmt.Errorf("mongodb: cannot create %s: %w", res.Table, cerr)
	}

	for _, c := range res.Columns {
		if !c.Unique || c.PrimaryKey {
			continue // _id is unique by construction
		}
		name := "uniq_" + c.Name
		_, ierr := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: c.Name, Value: 1}},
			Options: options.Index().SetUnique(true).SetName(name),
		})
		if ierr != nil {
			return fmt.Errorf("mongodb: cannot create the unique index on %s: %w", c.Name, ierr)
		}
	}
	return nil
}

// DropSchema removes the collection res describes, and everything in it.
func (d *Driver) DropSchema(ctx context.Context, res *store.Resource) error {
	if res == nil {
		return fmt.Errorf("mongodb: DropSchema needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return err
	}
	if derr := coll.Drop(ctx); derr != nil {
		return fmt.Errorf("mongodb: cannot drop %s: %w", res.Table, derr)
	}
	return nil
}

// HasSchema reports whether the collection res describes is already there.
func (d *Driver) HasSchema(ctx context.Context, res *store.Resource) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("mongodb: HasSchema needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return false, err
	}
	names, lerr := coll.Database().ListCollectionNames(ctx, bson.M{"name": res.Table})
	if lerr != nil {
		return false, fmt.Errorf("mongodb: cannot list collections: %w", lerr)
	}
	return len(names) > 0, nil
}

// isNamespaceExists reports the error MongoDB returns for a collection that is
// already there, which EnsureSchema treats as success.
func isNamespaceExists(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 48 // NamespaceExists
	}
	return strings.Contains(err.Error(), "already exists")
}
