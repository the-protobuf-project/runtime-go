package redis

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/core"
)

// Driver is a database.Driver backed by Redis.
//
// Records are stored as proto wire format under one key each, with a set of
// primary keys alongside so they can be listed. That is the whole layout — see
// [keys].
//
// # Why not a column map
//
// Every other driver here writes a [database.Resource]'s columns into something
// column-shaped: SQL columns, ABI arguments. Redis stores opaque bytes, so
// there is nothing to spread a message across and the only question is how to
// encode it. Proto wire format is the answer that cannot lose anything: JSON
// would turn a bytes column into base64 that decodes back as the base64 text
// rather than the bytes, and a uint64 past 2^53 into a float that comes back a
// different number. Both are silent.
//
// The descriptor is still what drives the primary key, the unique columns and
// the managed values; it just does not decide the encoding.
type Driver struct {
	rdb  goredis.UniversalClient
	keys keys
}

var (
	_ database.Driver   = (*Driver)(nil)
	_ database.Migrator = (*Driver)(nil)
	_ database.Batcher  = (*Driver)(nil)
)

// New returns a driver over a client you own, writing every resource on the
// schema its descriptor names.
//
// This package does not dial the client and does not close it. Use
// [NewProvider] to select a database at runtime instead.
func New(rdb goredis.UniversalClient, opts ...Option) *Driver {
	cfg := newConfig(opts...)
	return &Driver{rdb: rdb, keys: newKeys(cfg.prefix, "")}
}

// Create stores msg as a new record.
//
// # What is atomic and what is not
//
// The primary key is claimed with SET NX, so two writers racing on one key
// cannot both win. Each unique column is claimed the same way. What is not
// atomic is the sequence: a failure partway is rolled back explicitly by
// releasing what this call claimed, and a process that dies mid-Create can
// leave a reservation with no record behind it.
//
// That orphan is not silent — the next Create of the same value fails with
// [database.ErrAlreadyExists] naming a key that does not exist, which is the
// symptom to look for. A real transaction is the fix and Redis does not have
// one across these keys; a Lua script would, and is the upgrade path if this
// turns out to matter.
func (d *Driver) Create(ctx context.Context, res *database.Resource, msg proto.Message) (database.WriteResult, error) {
	if res == nil {
		return database.WriteResult{}, fmt.Errorf("redis: Create needs a resource")
	}
	msg, err := fillManaged(res, msg, true)
	if err != nil {
		return database.WriteResult{}, err
	}
	key, err := database.KeyOf(res, msg)
	if err != nil {
		return database.WriteResult{}, err
	}
	if key == "" {
		return database.WriteResult{}, fmt.Errorf("redis: resource %q has an empty primary key", res.Name)
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return database.WriteResult{}, fmt.Errorf("redis: cannot encode %s: %w", res.Name, err)
	}

	won, err := d.rdb.SetNX(ctx, d.keys.record(res, key), body, 0).Result()
	if err != nil {
		return database.WriteResult{}, fmt.Errorf("redis: cannot store %s: %w", key, err)
	}
	if !won {
		return database.WriteResult{}, fmt.Errorf("%w: %s", database.ErrAlreadyExists, key)
	}

	claimed, conflict, err := d.claimUnique(ctx, res, msg, key)
	if err != nil || conflict != "" {
		// Undo what this call did, in reverse: the record last, so nothing can
		// observe a record whose reservations have already gone.
		d.release(ctx, claimed)
		_ = d.rdb.Del(ctx, d.keys.record(res, key)).Err()
		if err != nil {
			return database.WriteResult{}, err
		}
		return database.WriteResult{}, fmt.Errorf("%w: %s is already held", database.ErrAlreadyExists, conflict)
	}

	if err := d.rdb.SAdd(ctx, d.keys.ids(res), key).Err(); err != nil {
		return database.WriteResult{}, fmt.Errorf("redis: cannot index %s: %w", key, err)
	}
	return database.WriteResult{Message: msg}, nil
}

// Get returns the record under key.
func (d *Driver) Get(ctx context.Context, res *database.Resource, key string) (proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("redis: Get needs a resource")
	}
	body, err := d.rdb.Get(ctx, d.keys.record(res, key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("%w: %s", database.ErrNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("redis: cannot read %s: %w", key, err)
	}
	return decode(res, body)
}

// Update overwrites the record identified by msg's primary key.
//
// Reservations on unique columns are moved as part of the write: a value that
// changed releases the old key and claims the new one. Without that, changing a
// user's e-mail would leave the old address reserved forever and nobody could
// ever use it again.
func (d *Driver) Update(ctx context.Context, res *database.Resource, msg proto.Message) (database.WriteResult, error) {
	if res == nil {
		return database.WriteResult{}, fmt.Errorf("redis: Update needs a resource")
	}
	msg, err := fillManaged(res, msg, false)
	if err != nil {
		return database.WriteResult{}, err
	}
	key, err := database.KeyOf(res, msg)
	if err != nil {
		return database.WriteResult{}, err
	}

	old, err := d.Get(ctx, res, key)
	if err != nil {
		return database.WriteResult{}, err // ErrNotFound travels as itself
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return database.WriteResult{}, fmt.Errorf("redis: cannot encode %s: %w", res.Name, err)
	}

	claimed, conflict, err := d.moveUnique(ctx, res, old, msg, key)
	if err != nil {
		return database.WriteResult{}, err
	}
	if conflict != "" {
		d.release(ctx, claimed)
		return database.WriteResult{}, fmt.Errorf("%w: %s is already held", database.ErrAlreadyExists, conflict)
	}

	if err := d.rdb.Set(ctx, d.keys.record(res, key), body, 0).Err(); err != nil {
		d.release(ctx, claimed)
		return database.WriteResult{}, fmt.Errorf("redis: cannot update %s: %w", key, err)
	}
	return database.WriteResult{Message: msg}, nil
}

// Delete removes the record under key and releases what it reserved.
//
// Unlike a cache, a missing record is reported: records here do not expire on
// their own, so asking to delete one that is not there means the caller's view
// of the store is wrong.
func (d *Driver) Delete(ctx context.Context, res *database.Resource, key string) error {
	if res == nil {
		return fmt.Errorf("redis: Delete needs a resource")
	}
	old, err := d.Get(ctx, res, key)
	if err != nil {
		return err
	}

	doomed := []string{d.keys.record(res, key)}
	for _, u := range uniqueValues(res, old) {
		// Escaped exactly as claimUnique wrote it — an unescaped value here
		// would delete a key nothing holds and leave the real reservation
		// behind, so the value could never be used again.
		doomed = append(doomed, d.keys.unique(res, u.column, escape(u.value)))
	}
	pipe := d.rdb.TxPipeline()
	pipe.Del(ctx, doomed...)
	pipe.SRem(ctx, d.keys.ids(res), key)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: cannot delete %s: %w", key, err)
	}
	return nil
}

// Exists reports whether a record with the given key is there.
func (d *Driver) Exists(ctx context.Context, res *database.Resource, key string) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("redis: Exists needs a resource")
	}
	n, err := d.rdb.Exists(ctx, d.keys.record(res, key)).Result()
	if err != nil {
		return false, fmt.Errorf("redis: cannot check %s: %w", key, err)
	}
	return n > 0, nil
}

// Count returns how many records this resource holds.
//
// opts.Filter is ignored, as [database.ListOptions] permits: Redis has no query
// language, and filtering here would mean reading every record to count them.
// Count stays O(1) and answers the question it can.
func (d *Driver) Count(ctx context.Context, res *database.Resource, _ database.ListOptions) (int64, error) {
	if res == nil {
		return 0, fmt.Errorf("redis: Count needs a resource")
	}
	n, err := d.rdb.SCard(ctx, d.keys.ids(res)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: cannot count %s: %w", res.Name, err)
	}
	return n, nil
}

// List returns a page of records.
//
// # What it honors and what it cannot
//
// Paging works: ids are read with a cursor, sorted, and cut by an offset token,
// so a page is stable between calls. Ordering works for the primary key, in
// either direction. opts.Filter and an OrderBy naming any other column are
// ignored, which [database.ListOptions] permits — Redis has no query language, and
// pretending otherwise by filtering client-side would make a page size mean
// nothing and read the whole table to return ten rows.
//
// The cost is honest and worth stating: this is O(records) in the id set on
// every call, whatever the page size. It is a store, not a query engine.
func (d *Driver) List(ctx context.Context, res *database.Resource, opts database.ListOptions) (database.ListResult, error) {
	if res == nil {
		return database.ListResult{}, fmt.Errorf("redis: List needs a resource")
	}
	ids, err := d.allIDs(ctx, res)
	if err != nil {
		return database.ListResult{}, err
	}
	total := int64(len(ids))

	slices.Sort(ids)
	if descending(opts.OrderBy, res) {
		slices.Reverse(ids)
	}

	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)
	if offset >= int64(len(ids)) {
		return database.ListResult{Items: nil, Total: total}, nil
	}
	page := ids[offset:]
	if limit < int64(len(page)) {
		page = page[:limit]
	}

	msgs, err := d.GetMany(ctx, res, page)
	if err != nil {
		return database.ListResult{}, err
	}
	items := make([]proto.Message, 0, len(msgs))
	for i, m := range msgs {
		if m == nil {
			// The id survived but the record is gone — a delete that failed
			// partway. Drop the stale member and skip it.
			_ = d.rdb.SRem(ctx, d.keys.ids(res), page[i]).Err()
			continue
		}
		items = append(items, m)
	}

	return database.ListResult{
		Items:         items,
		NextPageToken: core.EncodeToken(offset, int64(len(page)), total),
		Total:         total,
	}, nil
}

// allIDs reads the id set with a cursor rather than SMEMBERS.
//
// SMEMBERS builds the entire reply before sending any of it, and Redis is
// single-threaded while it does — a resource with a million records would stall
// every other client for the length of that one command. A cursor is the same
// total work spread over many small replies.
func (d *Driver) allIDs(ctx context.Context, res *database.Resource) ([]string, error) {
	var (
		out    []string
		seen   = map[string]struct{}{}
		cursor uint64
	)
	for {
		batch, next, err := d.rdb.SScan(ctx, d.keys.ids(res), cursor, "", scanBatch).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: cannot read the index of %s: %w", res.Name, err)
		}
		for _, id := range batch {
			// A cursor may hand back a member more than once.
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

// decode turns stored bytes back into a message of the resource's type.
func decode(res *database.Resource, body []byte) (proto.Message, error) {
	if res.New == nil {
		return nil, fmt.Errorf("redis: resource %q has no New constructor", res.Name)
	}
	msg := res.New()
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, fmt.Errorf("redis: stored %s does not decode: %w", res.Name, err)
	}
	return msg, nil
}

// descending reports whether an AIP-132 order expression asks for the primary
// key in reverse. Any other column is ignored — see [Driver.List].
func descending(orderBy string, res *database.Resource) bool {
	if orderBy == "" {
		return false
	}
	fields := strings.Fields(strings.ToLower(orderBy))
	if len(fields) == 0 {
		return false
	}
	if !strings.EqualFold(fields[0], res.PKColumn) {
		return false
	}
	return len(fields) > 1 && fields[1] == "desc"
}

// fillManaged returns msg with the values a driver supplies rather than the
// message.
//
// Redis stores the whole encoded message rather than a column map, so this
// round-trips through one to reach [core.FillManaged] and back. The trip is
// lossless — every value stays a Go value with no encoding in between — and is
// skipped entirely for a resource with nothing to fill, which is most of them.
func fillManaged(res *database.Resource, msg proto.Message, onCreate bool) (proto.Message, error) {
	if !core.HasManaged(res) {
		return msg, nil
	}
	cols, err := database.MessageToColumns(res, msg)
	if err != nil {
		return nil, err
	}
	core.FillManaged(res, cols, onCreate)
	return database.ColumnsToMessage(res, cols)
}
