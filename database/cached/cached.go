package cached

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// ErrOverloaded is what a [Cache] reports when it will not start another load.
//
// It is declared here rather than taken from one cache's package so this
// decorator stays swappable: [FromAside] translates the runtime-go cache's own
// sentinel into this one, and another implementation returns it directly.
//
// A driver seeing it reads through to the backing store rather than failing —
// the load happens either way, and that way it runs on the caller's goroutine,
// which something upstream is already bounding.
var ErrOverloaded = errors.New("cached: the cache is not accepting new loads")

// Cache is the read-through storage this decorator needs.
//
// It is a narrow interface rather than a concrete type so the decorator is not
// welded to one cache. [FromAside] adapts the runtime-go cache module, which is
// what most callers want; anything else that can collapse concurrent loads and
// remember an absence satisfies this in about thirty lines.
type Cache interface {
	// Aside returns a read-through view that calls load on a miss.
	//
	// One view per resource, built once at wiring time. A load reporting an
	// error wrapping [store.ErrNotFound] must have that absence remembered, or
	// requests for a record that does not exist reach the backing store forever
	// — which is the traffic a scraper produces.
	Aside(load func(ctx context.Context, key string) ([]byte, error)) Aside
}

// Aside is one resource's read-through view.
type Aside interface {
	// GetOrLoad fills dest with the record's encoded bytes, calling the loader
	// on a miss. Concurrent calls for one key must collapse into one load.
	//
	// It reports an error wrapping [store.ErrNotFound] for a record that is not
	// there, including one whose absence was remembered earlier.
	GetOrLoad(ctx context.Context, key string, dest *[]byte) error

	// Invalidate drops the entry for each key, and any remembered absence with
	// it — something that did not exist a moment ago and does now would
	// otherwise stay invisible until that absence expired on its own.
	Invalidate(ctx context.Context, keys ...string) error
}

// Driver is a [store.Driver] that reads through a cache and writes through to
// the driver underneath.
//
// # What is cached and what is not
//
// Get is cached, per record, keyed by primary key. Everything else is not, and
// each for a reason rather than for want of time:
//
//   - List and Count depend on the whole set, so any write to any record can
//     change them. Caching them correctly means knowing which listings contain
//     which records, which is a different and much harder problem than caching
//     a record by its key. A stale listing is also the kind of wrong that looks
//     right.
//   - Exists asks a yes-or-no question the backing store answers cheaply.
//     Serving it from the cache would mean transferring a whole record to
//     answer it, and populating the cache with records nobody asked to read.
//
// Writes go to the backing store first and invalidate the cache after. A record
// is never served from the cache after a write it did not observe, except in
// the window between those two steps — see [Driver.Create].
type Driver struct {
	next  store.Driver
	cache Cache

	mu     sync.RWMutex
	asides map[string]Aside // resource name -> its read-through view
}

var _ store.Driver = (*Driver)(nil)

// New returns a driver that reads next through c.
//
// Prefer [Wrap] where you have a [store.DB]: this returns a bare Driver, so the
// capabilities the underlying database had — transactions, migrations — are not
// reachable through the result.
func New(next store.Driver, c Cache) *Driver {
	return &Driver{next: next, cache: c, asides: map[string]Aside{}}
}

// Wrap returns db with its driver read through c, keeping everything else.
//
// This is the form to use. The capability fields are carried across unchanged,
// so wrapping a database in a cache cannot quietly cost it transactions or
// migrations — and the transaction runner is wrapped too, so writes committed
// inside one invalidate what they touched.
func Wrap(db *store.DB, c Cache) *store.DB {
	if db == nil {
		return nil
	}
	d := New(db.Driver, c)
	return &store.DB{
		Driver:  d,
		Tx:      &cachedTx{next: db.Tx, driver: d},
		Schema:  db.Schema,
		Backend: db.Backend + "+cache",
		Name:    db.Name,
		Release: db.Release,
	}
}

// aside returns the read-through view for a resource, building it once.
//
// One per resource rather than one overall, because a view is bound to a loader
// and the loader has to know which resource it is loading. They are cheap: a
// cache that collapses loads shares that machinery across the views it makes.
func (d *Driver) aside(res *store.Resource) Aside {
	d.mu.RLock()
	a, ok := d.asides[res.Name]
	d.mu.RUnlock()
	if ok {
		return a
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if a, ok = d.asides[res.Name]; ok {
		return a
	}
	a = d.cache.Aside(func(ctx context.Context, key string) ([]byte, error) {
		msg, err := d.next.Get(ctx, res, key)
		if err != nil {
			return nil, err // ErrNotFound travels as itself, to be remembered
		}
		body, merr := proto.Marshal(msg)
		if merr != nil {
			return nil, fmt.Errorf("cached: cannot encode %s: %w", res.Name, merr)
		}
		return body, nil
	})
	d.asides[res.Name] = a
	return a
}

// Get returns the record under key, from the cache where it is there.
//
// The bytes cached are proto wire format, not the message. A cache encodes what
// it is given and the general-purpose way to do that is JSON, which is wrong for
// a proto message twice over: it does not know the field names, and for a
// dynamic message it has nothing to marshal at all. Wire format in, wire format
// out, and the cache only ever sees opaque bytes.
func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("cached: Get needs a resource")
	}
	var body []byte
	if err := d.aside(res).GetOrLoad(ctx, key, &body); err != nil {
		if isOverloaded(err) {
			// The cache is refusing new work rather than queueing it. Reading
			// through directly is the right answer: the load happens either
			// way, and this way it runs on the caller's own goroutine, which
			// something upstream is already bounding.
			return d.next.Get(ctx, res, key)
		}
		return nil, err
	}
	if res.New == nil {
		return nil, fmt.Errorf("cached: resource %q has no New constructor", res.Name)
	}
	msg := res.New()
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, fmt.Errorf("cached: cached %s does not decode: %w", res.Name, err)
	}
	return msg, nil
}

// Create stores a record and drops whatever the cache believed about its key.
//
// The invalidation is not housekeeping. A Get for a key that was not there
// remembers the absence, so a record created afterwards would stay invisible
// until that memory expired — the one bug a read-through cache produces that
// looks exactly like a database that lost a write.
func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("cached: Create needs a resource")
	}
	out, err := d.next.Create(ctx, res, msg)
	if err != nil {
		return out, err
	}
	key, kerr := store.KeyOf(res, out.Message)
	if kerr != nil {
		key, kerr = store.KeyOf(res, msg)
	}
	if kerr == nil {
		d.forget(ctx, res, key)
	}
	return out, nil
}

// Update overwrites a record and drops the cached copy.
func (d *Driver) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("cached: Update needs a resource")
	}
	out, err := d.next.Update(ctx, res, msg)
	if err != nil {
		return out, err
	}
	if key, kerr := store.KeyOf(res, msg); kerr == nil {
		d.forget(ctx, res, key)
	}
	return out, nil
}

// Delete removes a record and drops the cached copy.
func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
	if res == nil {
		return fmt.Errorf("cached: Delete needs a resource")
	}
	if err := d.next.Delete(ctx, res, key); err != nil {
		return err
	}
	d.forget(ctx, res, key)
	return nil
}

// List passes through; see [Driver].
func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	return d.next.List(ctx, res, opts)
}

// Count passes through; see [Driver].
func (d *Driver) Count(ctx context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	return d.next.Count(ctx, res, opts)
}

// Exists passes through; see [Driver].
func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	return d.next.Exists(ctx, res, key)
}

// Unwrap returns the driver underneath, for reads that must not be cached.
func (d *Driver) Unwrap() store.Driver { return d.next }

// forget drops a cached record.
//
// A failure here is logged by nothing and fails nothing: the write it followed
// has already happened, and reporting a cache error as a write error would tell
// a caller its write failed when it did not. The entry is wrong until it
// expires, which is the reason a cache in front of a store wants a TTL even
// though every write invalidates.
func (d *Driver) forget(ctx context.Context, res *store.Resource, key string) {
	if key == "" {
		return
	}
	_ = d.aside(res).Invalidate(ctx, key)
}

// cachedTx runs a transaction on the underlying database and invalidates what it
// wrote once it commits.
//
// Reads inside the transaction go straight to the backing store, never to the
// cache: a transaction has to see its own writes, and a cache that has not
// observed them cannot show them. Writes are recorded rather than invalidated as
// they happen, because a rollback would otherwise have thrown away entries that
// are still correct.
type cachedTx struct {
	next   store.Transactional
	driver *Driver
}

var _ store.Transactional = (*cachedTx)(nil)

func (t *cachedTx) Run(ctx context.Context, fn func(*store.DB) error) error {
	var touched []touch
	err := t.next.Run(ctx, func(inner *store.DB) error {
		// The body sees a DB whose driver records what it wrote, and whose
		// other capabilities are the transaction's own — a cache in front of a
		// transaction would be showing it data it has not committed.
		rec := &recorder{Driver: inner.Driver}
		bound := *inner
		bound.Driver = rec
		rerr := fn(&bound)
		touched = rec.touched
		return rerr
	})
	if err != nil {
		return err
	}
	// Committed, so what it wrote is now what a reader should see.
	for _, w := range touched {
		t.driver.forget(ctx, w.res, w.key)
	}
	return nil
}

// touch is one record a transaction wrote.
type touch struct {
	res *store.Resource
	key string
}

// recorder is the driver handed to a transaction body: every write passes
// through to the real transactional driver and is remembered so the cache can be
// corrected after the commit.
type recorder struct {
	store.Driver
	touched []touch
}

func (r *recorder) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	out, err := r.Driver.Create(ctx, res, msg)
	if err == nil {
		if key, kerr := store.KeyOf(res, out.Message); kerr == nil {
			r.touched = append(r.touched, touch{res, key})
		}
	}
	return out, err
}

func (r *recorder) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	out, err := r.Driver.Update(ctx, res, msg)
	if err == nil {
		if key, kerr := store.KeyOf(res, msg); kerr == nil {
			r.touched = append(r.touched, touch{res, key})
		}
	}
	return out, err
}

func (r *recorder) Delete(ctx context.Context, res *store.Resource, key string) error {
	err := r.Driver.Delete(ctx, res, key)
	if err == nil {
		r.touched = append(r.touched, touch{res, key})
	}
	return err
}

// isOverloaded reports whether a cache refused to start another load.
func isOverloaded(err error) bool {
	return errors.Is(err, ErrOverloaded)
}
