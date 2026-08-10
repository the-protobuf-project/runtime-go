package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Driver is a store.Driver backed by Redis.
//
// Records are stored as proto wire format under one key each, with a set of
// primary keys alongside so they can be listed. That is the whole layout — see
// [keys].
//
// # Why not a column map
//
// Every other driver here writes a [store.Resource]'s columns into something
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
	_ store.Driver   = (*Driver)(nil)
	_ store.Migrator = (*Driver)(nil)
	_ store.Batcher  = (*Driver)(nil)
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
// [store.ErrAlreadyExists] naming a key that does not exist, which is the
// symptom to look for. A real transaction is the fix and Redis does not have
// one across these keys; a Lua script would, and is the upgrade path if this
// turns out to matter.
func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("redis: Create needs a resource")
	}
	msg, err := fillManaged(res, msg, true)
	if err != nil {
		return store.WriteResult{}, err
	}
	key, err := store.KeyOf(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	if key == "" {
		return store.WriteResult{}, fmt.Errorf("redis: resource %q has an empty primary key", res.Name)
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return store.WriteResult{}, fmt.Errorf("redis: cannot encode %s: %w", res.Name, err)
	}

	won, err := d.rdb.SetNX(ctx, d.keys.record(res, key), body, 0).Result()
	if err != nil {
		return store.WriteResult{}, fmt.Errorf("redis: cannot store %s: %w", key, err)
	}
	if !won {
		return store.WriteResult{}, fmt.Errorf("%w: %s", store.ErrAlreadyExists, key)
	}

	claimed, conflict, err := d.claimUnique(ctx, res, msg, key)
	if err != nil || conflict != "" {
		// Undo what this call did, in reverse: the record last, so nothing can
		// observe a record whose reservations have already gone.
		d.release(ctx, claimed)
		_ = d.rdb.Del(ctx, d.keys.record(res, key)).Err()
		if err != nil {
			return store.WriteResult{}, err
		}
		return store.WriteResult{}, fmt.Errorf("%w: %s is already held", store.ErrAlreadyExists, conflict)
	}

	if err := d.rdb.ZAdd(ctx, d.keys.ids(res), goredis.Z{Score: 0, Member: key}).Err(); err != nil {
		return store.WriteResult{}, fmt.Errorf("redis: cannot index %s: %w", key, err)
	}
	return store.WriteResult{Message: msg}, nil
}

// Get returns the record under key.
func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("redis: Get needs a resource")
	}
	body, err := d.rdb.Get(ctx, d.keys.record(res, key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("%w: %s", store.ErrNotFound, key)
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
func (d *Driver) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("redis: Update needs a resource")
	}
	msg, err := fillManaged(res, msg, false)
	if err != nil {
		return store.WriteResult{}, err
	}
	key, err := store.KeyOf(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}

	old, err := d.Get(ctx, res, key)
	if err != nil {
		return store.WriteResult{}, err // ErrNotFound travels as itself
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return store.WriteResult{}, fmt.Errorf("redis: cannot encode %s: %w", res.Name, err)
	}

	claimed, conflict, err := d.moveUnique(ctx, res, old, msg, key)
	if err != nil {
		return store.WriteResult{}, err
	}
	if conflict != "" {
		d.release(ctx, claimed)
		return store.WriteResult{}, fmt.Errorf("%w: %s is already held", store.ErrAlreadyExists, conflict)
	}

	if err := d.rdb.Set(ctx, d.keys.record(res, key), body, 0).Err(); err != nil {
		d.release(ctx, claimed)
		return store.WriteResult{}, fmt.Errorf("redis: cannot update %s: %w", key, err)
	}
	return store.WriteResult{Message: msg}, nil
}

// Delete removes the record under key and releases what it reserved.
//
// Unlike a cache, a missing record is reported: records here do not expire on
// their own, so asking to delete one that is not there means the caller's view
// of the store is wrong.
func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
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
	pipe.ZRem(ctx, d.keys.ids(res), key)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: cannot delete %s: %w", key, err)
	}
	return nil
}

// Exists reports whether a record with the given key is there.
func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
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
// opts.Filter is ignored, as [store.ListOptions] permits: Redis has no query
// language, and filtering here would mean reading every record to count them.
// Count stays O(1) and answers the question it can.
func (d *Driver) Count(ctx context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	if res == nil {
		return 0, fmt.Errorf("redis: Count needs a resource")
	}
	if err := refuseFilter(opts.Filter); err != nil {
		return 0, err
	}
	n, err := d.rdb.ZCard(ctx, d.keys.ids(res)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: cannot count %s: %w", res.Name, err)
	}
	return n, nil
}

// List returns a page of records.
//
// # What it honors and what it cannot
//
// Paging and ordering both happen in the server. The index is a sorted set, so
// a page is one range read in log time and costs the same whatever the table
// holds — it used to be a plain set, which meant reading every id into this
// process to sort and slice it. Ordering is by primary key, either direction; an
// OrderBy naming any other column falls back to the key, which
// [store.ListOptions] permits.
//
// A filter is refused rather than ignored — see [refuseFilter]. Redis has no
// query language, and honoring one client-side would read the whole table to
// return ten rows while making the page size mean nothing.
func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	if res == nil {
		return store.ListResult{}, fmt.Errorf("redis: List needs a resource")
	}
	if err := refuseFilter(opts.Filter); err != nil {
		return store.ListResult{}, err
	}
	total := core.NoTotal
	if !opts.OmitTotal {
		var cerr error
		if total, cerr = d.Count(ctx, res, opts); cerr != nil {
			return store.ListResult{}, cerr
		}
	}

	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)
	fetch := core.FetchLimit(limit, opts.OmitTotal)

	// The page comes back already ordered and already cut. The index used to be
	// an unordered set, which meant reading every id into this process to sort
	// and slice it — a million records allocated a million strings to return
	// fifty, on every call and once per concurrent caller. A sorted set does the
	// same work in the server, in log time, and hands back only the page.
	key := d.keys.ids(res)
	stop := offset + fetch - 1
	var (
		page []string
		perr error
	)
	if descending(opts.OrderBy, res) {
		page, perr = d.rdb.ZRevRange(ctx, key, offset, stop).Result()
	} else {
		page, perr = d.rdb.ZRange(ctx, key, offset, stop).Result()
	}
	if perr != nil {
		return store.ListResult{}, fmt.Errorf("redis: cannot read the index of %s: %w", res.Name, perr)
	}
	if len(page) == 0 {
		return store.ListResult{Items: nil, Total: total}, nil
	}
	page, next := core.TrimPage(page, offset, limit, total, opts.OmitTotal)

	msgs, err := d.GetMany(ctx, res, page)
	if err != nil {
		return store.ListResult{}, err
	}
	items := make([]proto.Message, 0, len(msgs))
	for i, m := range msgs {
		if m == nil {
			// The id survived but the record is gone — a delete that failed
			// partway. Drop the stale member and skip it.
			_ = d.rdb.ZRem(ctx, d.keys.ids(res), page[i]).Err()
			continue
		}
		items = append(items, m)
	}

	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

// decode turns stored bytes back into a message of the resource's type.
func decode(res *store.Resource, body []byte) (proto.Message, error) {
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
func descending(orderBy string, res *store.Resource) bool {
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
func fillManaged(res *store.Resource, msg proto.Message, onCreate bool) (proto.Message, error) {
	if !core.HasManaged(res) {
		return msg, nil
	}
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return nil, err
	}
	core.FillManaged(res, cols, onCreate)
	return store.ColumnsToMessage(res, cols)
}

// refuseFilter reports a filter this backend cannot apply.
//
// Redis has no query language, so a filter can only be honored by reading every
// record into this process — which would make a page size mean nothing and read
// the whole table to return ten rows. The contract's rule is that a backend
// either honors a filter or refuses it by name, and silently returning the
// unfiltered set is the one outcome it forbids: the wrong records, with nothing
// to say anything was ignored.
func refuseFilter(filter string) error {
	if strings.TrimSpace(filter) == "" {
		return nil
	}
	return fmt.Errorf(
		"%w: redis has no query language, so it cannot apply the filter %q; "+
			"list by key and narrow in the caller, or hold this resource in a backend that can query",
		store.ErrUnimplemented, filter)
}
