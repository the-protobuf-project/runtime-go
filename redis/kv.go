package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/redis/internal/codec"
	"github.com/the-protobuf-project/runtime-go/redis/internal/conn"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

// kvHandler is durable, content-addressed storage.
//
// Records never expire. Every value is canonicalized and hashed, and the hash
// is reserved before the value is written, so identical content always resolves
// to one id.
type kvHandler struct {
	conn *conn.Conn
	keys keys
	log  telemetry.Logger
}

var _ database.Store = (*kvHandler)(nil)

func newKV(c *conn.Conn, prefix string, log telemetry.Logger, _ telemetry.Meter) database.Store {
	return &kvHandler{conn: c, keys: newKeys(prefix, kindKV), log: log}
}

// Create stores a value, deduplicating by content.
//
// The hash is reserved with SETNX before the value is written, so two writers
// racing on identical content cannot both win. When the hash is already held,
// the id holding it is returned instead of storing a copy — compare it against
// the id you supplied to tell a fresh write from a deduplicated one.
func (k *kvHandler) Create(ctx context.Context, id string, value any, _ ...database.Option) (string, error) {
	hash, canonical, err := codec.Canonicalize(value)
	if err != nil {
		k.log.Error(ctx, "could not canonicalize the value", err, telemetry.Fields{"id": id})
		return "", err
	}

	if id == "" {
		id = ulid.Generate().GetRandomCode()
		k.log.Debug(ctx, "generated an id", telemetry.Fields{"id": id})
	}

	k.log.Debug(ctx, "reserving content hash", telemetry.Fields{"id": id, "hash": hash})

	reserved, err := conn.SetNX(ctx, k.conn.Redis(), k.keys.byContent(hash), id)
	if err != nil {
		k.log.Error(ctx, "could not reserve the content hash", err,
			telemetry.Fields{"id": id, "hash": hash})
		return "", fmt.Errorf("redis: cannot reserve content hash: %w", err)
	}

	if !reserved {
		owner, oerr := k.resolveOwner(ctx, hash, id)
		if oerr != nil {
			return "", oerr
		}
		if owner != "" {
			k.log.Info(ctx, "content already stored, deduplicated",
				telemetry.Fields{"requested": id, "existing": owner, "hash": hash})
			return owner, nil
		}
		// resolveOwner cleared an orphaned reservation; claim it now.
		reserved, err = conn.SetNX(ctx, k.conn.Redis(), k.keys.byContent(hash), id)
		if err != nil {
			return "", fmt.Errorf("redis: cannot reserve content hash: %w", err)
		}
		if !reserved {
			k.log.Warn(ctx, "another writer claimed the content first",
				telemetry.Fields{"id": id, "hash": hash})
			return "", fmt.Errorf("%w: content is held by another record", database.ErrDuplicate)
		}
	}

	pipe := k.conn.Redis().TxPipeline()
	pipe.Set(ctx, k.keys.entry(id), canonical, 0)
	pipe.SAdd(ctx, k.keys.index(), id)
	pipe.Set(ctx, k.keys.contentOf(id), hash, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		// Release only a reservation this call made. Rolling back
		// unconditionally would delete the hash of a pre-existing record on the
		// deduplicated path above, destroying its index and letting a later
		// write store a duplicate under a new id.
		if delErr := k.conn.Redis().Del(ctx, k.keys.byContent(hash)).Err(); delErr != nil {
			err = errors.Join(err, delErr)
		}
		k.log.Error(ctx, "could not write the record", err, telemetry.Fields{"id": id})
		return "", fmt.Errorf("redis: cannot create record %s: %w", id, err)
	}

	k.log.Debug(ctx, "record written", telemetry.Fields{"id": id, "hash": hash, "bytes": len(canonical)})
	return id, nil
}

// resolveOwner returns the id holding a hash, or "" when the reservation is
// orphaned — held by a record whose body is gone, from a write that failed
// between reserving and storing. An orphan is cleared so the content can be
// stored again rather than being permanently unwritable.
func (k *kvHandler) resolveOwner(ctx context.Context, hash, id string) (string, error) {
	owner, err := k.conn.Redis().Get(ctx, k.keys.byContent(hash)).Result()
	if err != nil {
		k.log.Error(ctx, "could not read the content owner", err, telemetry.Fields{"hash": hash})
		return "", fmt.Errorf("redis: cannot read the owner of content hash: %w", err)
	}

	n, err := k.conn.Redis().Exists(ctx, k.keys.entry(owner)).Result()
	if err != nil {
		return "", fmt.Errorf("redis: cannot check record %s: %w", owner, err)
	}
	if n > 0 {
		return owner, nil
	}

	k.log.Warn(ctx, "clearing an orphaned content reservation",
		telemetry.Fields{"hash": hash, "orphan": owner, "requested": id})
	if err := k.conn.Redis().Del(ctx, k.keys.byContent(hash)).Err(); err != nil {
		return "", fmt.Errorf("redis: cannot clear an orphaned reservation: %w", err)
	}
	return "", nil
}

// Get decodes a record into dest.
func (k *kvHandler) Get(ctx context.Context, id string, dest any) error {
	k.log.Debug(ctx, "reading record", telemetry.Fields{"id": id})

	body, err := k.conn.Redis().Get(ctx, k.keys.entry(id)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			k.log.Debug(ctx, "record not found", telemetry.Fields{"id": id})
			return fmt.Errorf("%w: %s", database.ErrNotFound, id)
		}
		k.log.Error(ctx, "could not read the record", err, telemetry.Fields{"id": id})
		return fmt.Errorf("redis: cannot read record %s: %w", id, err)
	}

	if err := codec.Decode(body, dest); err != nil {
		k.log.Error(ctx, "stored record does not decode into the destination", err,
			telemetry.Fields{"id": id, "dest": fmt.Sprintf("%T", dest)})
		return err
	}
	return nil
}

// Update replaces a record's value, keeping the content index consistent.
//
// Content already held by a different record is refused: allowing it would
// leave two ids claiming one hash and break the guarantee that identical
// content resolves to a single record.
func (k *kvHandler) Update(ctx context.Context, id string, value any, _ ...database.Option) error {
	hash, canonical, err := codec.Canonicalize(value)
	if err != nil {
		k.log.Error(ctx, "could not canonicalize the value", err, telemetry.Fields{"id": id})
		return err
	}

	k.log.Debug(ctx, "updating record", telemetry.Fields{"id": id, "hash": hash})

	n, err := k.conn.Redis().Exists(ctx, k.keys.entry(id)).Result()
	if err != nil {
		return fmt.Errorf("redis: cannot check record %s: %w", id, err)
	}
	if n == 0 {
		k.log.Debug(ctx, "record not found, nothing updated", telemetry.Fields{"id": id})
		return fmt.Errorf("%w: %s", database.ErrNotFound, id)
	}

	owner, err := k.conn.Redis().Get(ctx, k.keys.byContent(hash)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("redis: cannot check the content hash: %w", err)
	}
	if owner != "" && owner != id {
		k.log.Warn(ctx, "refusing an update to content held by another record",
			telemetry.Fields{"id": id, "owner": owner, "hash": hash})
		return fmt.Errorf("%w: content already stored as %s", database.ErrDuplicate, owner)
	}

	// The hash currently held, so the stale reservation is released in the same
	// transaction that claims the new one.
	oldHash, err := k.conn.Redis().Get(ctx, k.keys.contentOf(id)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("redis: cannot read the current content hash: %w", err)
	}

	pipe := k.conn.Redis().TxPipeline()
	pipe.Set(ctx, k.keys.entry(id), canonical, 0)
	pipe.Set(ctx, k.keys.contentOf(id), hash, 0)
	pipe.Set(ctx, k.keys.byContent(hash), id, 0)
	if oldHash != "" && oldHash != hash {
		pipe.Del(ctx, k.keys.byContent(oldHash))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		k.log.Error(ctx, "could not update the record", err, telemetry.Fields{"id": id})
		return fmt.Errorf("redis: cannot update record %s: %w", id, err)
	}

	k.log.Debug(ctx, "record updated", telemetry.Fields{"id": id, "released": oldHash})
	return nil
}

// Delete removes a record and releases its content.
//
// Unlike a cache, a missing record is reported: records do not expire on their
// own, so asking to delete one that is not there means the caller's view of the
// store is wrong.
func (k *kvHandler) Delete(ctx context.Context, id string) error {
	k.log.Debug(ctx, "deleting record", telemetry.Fields{"id": id})

	n, err := k.conn.Redis().Exists(ctx, k.keys.entry(id)).Result()
	if err != nil {
		return fmt.Errorf("redis: cannot check record %s: %w", id, err)
	}
	if n == 0 {
		k.log.Debug(ctx, "record not found, nothing deleted", telemetry.Fields{"id": id})
		return fmt.Errorf("%w: %s", database.ErrNotFound, id)
	}

	// Read the hash before the transaction; releasing it is what lets the same
	// content be stored again later.
	hash, err := k.conn.Redis().Get(ctx, k.keys.contentOf(id)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("redis: cannot read the content hash of %s: %w", id, err)
	}

	pipe := k.conn.Redis().TxPipeline()
	pipe.Del(ctx, k.keys.entry(id))
	pipe.SRem(ctx, k.keys.index(), id)
	pipe.Del(ctx, k.keys.contentOf(id))
	if hash != "" {
		pipe.Del(ctx, k.keys.byContent(hash))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		k.log.Error(ctx, "could not delete the record", err, telemetry.Fields{"id": id})
		return fmt.Errorf("redis: cannot delete record %s: %w", id, err)
	}

	k.log.Debug(ctx, "record deleted", telemetry.Fields{"id": id, "released": hash})
	return nil
}

// Keys returns record ids in a stable order, narrowed by the options.
//
// Redis sets have no order, so the ids are sorted before Limit and Offset are
// applied — without that, paging would return overlapping or missing records
// between calls.
func (k *kvHandler) Keys(ctx context.Context, opts ...database.Option) ([]string, error) {
	o := database.NewOptions(opts...)

	ids, err := k.conn.Redis().SMembers(ctx, k.keys.index()).Result()
	if err != nil {
		k.log.Error(ctx, "could not read the record index", err, nil)
		return nil, fmt.Errorf("redis: cannot read the record index: %w", err)
	}
	slices.Sort(ids)

	total := len(ids)
	if o.Offset > 0 {
		if o.Offset >= len(ids) {
			k.log.Debug(ctx, "offset is past the end", telemetry.Fields{"offset": o.Offset, "total": total})
			return []string{}, nil
		}
		ids = ids[o.Offset:]
	}
	if o.Limit > 0 && o.Limit < len(ids) {
		ids = ids[:o.Limit]
	}

	k.log.Debug(ctx, "listed record ids", telemetry.Fields{
		"returned": len(ids), "total": total, "limit": o.Limit, "offset": o.Offset,
	})
	return ids, nil
}

// List decodes records into dest, which must point to a slice, in the same
// order as [kvHandler.Keys].
func (k *kvHandler) List(ctx context.Context, dest any, opts ...database.Option) error {
	ids, err := k.Keys(ctx, opts...)
	if err != nil {
		return err
	}

	slice, elem, err := sliceTarget(dest)
	if err != nil {
		k.log.Error(ctx, "list destination is not a slice pointer", err,
			telemetry.Fields{"dest": fmt.Sprintf("%T", dest)})
		return err
	}

	out := reflect.MakeSlice(slice.Type(), 0, len(ids))
	for _, id := range ids {
		item := reflect.New(elem)
		if gerr := k.Get(ctx, id, item.Interface()); gerr != nil {
			if errors.Is(gerr, database.ErrNotFound) {
				// The body is gone but the index member survived — a delete
				// that failed partway. Drop the stale member and carry on.
				k.log.Warn(ctx, "sweeping a stale index member", telemetry.Fields{"id": id})
				_ = k.conn.Redis().SRem(ctx, k.keys.index(), id).Err()
				continue
			}
			return gerr
		}
		out = reflect.Append(out, item.Elem())
	}

	slice.Set(out)
	k.log.Debug(ctx, "listed records", telemetry.Fields{"count": out.Len()})
	return nil
}
