package store

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Instrument returns db with every operation measured and recorded.
//
//	db = Instrument(db,
//	    WithMeter(obs.Meter()),
//	    WithLogger(obs.Logger()),
//	)
//
// It takes and returns a [DB] rather than a [Driver] for a reason worth stating:
// wrapping the driver alone would hand back something that no longer satisfies
// [Batcher] or [Watcher], because a wrapper only implements what it was written
// to implement. A backend would silently lose the capability that makes bulk
// reads cheap, at the exact moment someone turned on the metrics that would have
// shown them why it got slow. This carries both across, and leaves [DB.Tx],
// [DB.Schema], [DB.Graph] and [DB.Series] exactly as they were.
//
// Both a meter and a logger are optional. With neither, this returns db
// unchanged rather than a wrapper that costs a call and records nothing.
func Instrument(db *DB, opts ...ObserveOption) *DB {
	if db == nil {
		return nil
	}
	cfg := observeConfig{meter: telemetry.NoopMeter, logger: telemetry.NoopLogger}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if !cfg.wanted {
		return db
	}

	base := newObserver(db.Driver, db.Backend, cfg)

	// A capability the inner driver has must survive the wrapping, and one it
	// lacks must not appear. That is why this switches rather than always
	// implementing both: a Batcher that exists only because something was
	// instrumented would fail at the call rather than at construction, which is
	// the whole failure mode the capability split exists to prevent.
	var d Driver = base
	b, hasBatcher := db.Driver.(Batcher)
	w, hasWatcher := db.Driver.(Watcher)
	switch {
	case hasBatcher && hasWatcher:
		d = &observedBatcherWatcher{observer: base, b: b, w: w}
	case hasBatcher:
		d = &observedBatcher{observer: base, b: b}
	case hasWatcher:
		d = &observedWatcher{observer: base, w: w}
	}

	out := *db
	out.Driver = d
	return &out
}

// ObserveOption configures [Instrument].
type ObserveOption func(*observeConfig)

type observeConfig struct {
	meter  telemetry.Meter
	logger telemetry.Logger
	wanted bool
}

// WithMeter records an operation count and a duration histogram per operation.
//
// The meter is injected rather than resolved from a package-level global, so a
// binary that never wires telemetry pays nothing and no import can start a
// background exporter behind the caller's back.
func WithMeter(m telemetry.Meter) ObserveOption {
	return func(c *observeConfig) {
		if m == nil {
			m = telemetry.NoopMeter
		}
		c.meter = m
		c.wanted = true
	}
}

// WithLogger records one line per operation: debug on success, warn on a record
// that was not there, error on a real failure.
func WithLogger(l telemetry.Logger) ObserveOption {
	return func(c *observeConfig) {
		if l == nil {
			l = telemetry.NoopLogger
		}
		c.logger = l
		c.wanted = true
	}
}

// observer instruments the seven CRUD methods.
type observer struct {
	next    Driver
	backend string
	log     telemetry.Logger

	ops  telemetry.Counter
	dur  telemetry.Histogram
	rows telemetry.Counter
}

var _ Driver = (*observer)(nil)

func newObserver(next Driver, backend string, cfg observeConfig) *observer {
	return &observer{
		next:    next,
		backend: backend,
		log:     cfg.logger,
		ops:     cfg.meter.Counter("database_operations_total", telemetry.WithUnit("1")),
		dur:     cfg.meter.Histogram("database_operation_duration_seconds", telemetry.WithUnit("s")),
		rows:    cfg.meter.Counter("database_rows_read_total", telemetry.WithUnit("1")),
	}
}

// Unwrap returns the driver underneath, for a caller that needs the uninstrumented one.
func (o *observer) Unwrap() Driver { return o.next }

// record reports one completed operation.
//
// The labels are the backend, the operation and the resource — all three
// bounded, which is what makes them safe to put on a metric. The record's key is
// deliberately not among them: it is unbounded, and a label per key is how a
// metrics pipeline falls over at exactly the scale this instrumentation was
// added to survive.
//
// A missing record is reported as an outcome rather than an error, because an
// absent record is a normal answer to a question and counting it as a failure
// makes an error rate useless.
func (o *observer) record(ctx context.Context, op string, res *Resource, start time.Time, err error) {
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		outcome = "not_found"
	case errors.Is(err, ErrAlreadyExists):
		outcome = "conflict"
	case errors.Is(err, ErrUnimplemented):
		outcome = "unimplemented"
	default:
		outcome = "error"
	}

	labels := telemetry.Labels{
		"backend":   o.backend,
		"operation": op,
		"resource":  resourceName(res),
		"outcome":   outcome,
	}
	o.ops.Add(ctx, 1, labels)
	o.dur.Record(ctx, time.Since(start).Seconds(), labels)

	fields := telemetry.Fields{
		"backend":   o.backend,
		"operation": op,
		"resource":  resourceName(res),
		"duration":  time.Since(start).String(),
	}
	switch outcome {
	case "ok":
		o.log.Debug(ctx, "database "+op, fields)
	case "not_found":
		o.log.Warn(ctx, "database "+op+": not found", fields)
	default:
		o.log.Error(ctx, "database "+op+" failed", err, fields)
	}
}

func resourceName(res *Resource) string {
	if res == nil {
		return "unknown"
	}
	return res.Name
}

func (o *observer) Create(ctx context.Context, res *Resource, msg proto.Message) (WriteResult, error) {
	start := time.Now()
	out, err := o.next.Create(ctx, res, msg)
	o.record(ctx, "create", res, start, err)
	return out, err
}

func (o *observer) Get(ctx context.Context, res *Resource, key string) (proto.Message, error) {
	start := time.Now()
	msg, err := o.next.Get(ctx, res, key)
	o.record(ctx, "get", res, start, err)
	return msg, err
}

func (o *observer) Update(ctx context.Context, res *Resource, msg proto.Message) (WriteResult, error) {
	start := time.Now()
	out, err := o.next.Update(ctx, res, msg)
	o.record(ctx, "update", res, start, err)
	return out, err
}

func (o *observer) Delete(ctx context.Context, res *Resource, key string) error {
	start := time.Now()
	err := o.next.Delete(ctx, res, key)
	o.record(ctx, "delete", res, start, err)
	return err
}

// List also counts the rows it returned.
//
// That count is what turns a slow listing from a mystery into a fact: a page
// taking a second because it returned ten thousand rows and one taking a second
// because the query is bad look identical in a duration histogram alone.
func (o *observer) List(ctx context.Context, res *Resource, opts ListOptions) (ListResult, error) {
	start := time.Now()
	out, err := o.next.List(ctx, res, opts)
	o.record(ctx, "list", res, start, err)
	if err == nil {
		o.rows.Add(ctx, float64(len(out.Items)), telemetry.Labels{
			"backend": o.backend, "resource": resourceName(res),
		})
	}
	return out, err
}

func (o *observer) Count(ctx context.Context, res *Resource, opts ListOptions) (int64, error) {
	start := time.Now()
	n, err := o.next.Count(ctx, res, opts)
	o.record(ctx, "count", res, start, err)
	return n, err
}

func (o *observer) Exists(ctx context.Context, res *Resource, key string) (bool, error) {
	start := time.Now()
	ok, err := o.next.Exists(ctx, res, key)
	o.record(ctx, "exists", res, start, err)
	return ok, err
}

// The three shapes that carry a driver's optional capabilities across the
// wrapping. Each instruments its own methods rather than passing them straight
// through, because a bulk read is exactly the call worth measuring.

type observedBatcher struct {
	*observer
	b Batcher
}

var _ Batcher = (*observedBatcher)(nil)

func (o *observedBatcher) CreateMany(ctx context.Context, res *Resource, msgs []proto.Message) ([]WriteResult, error) {
	start := time.Now()
	out, err := o.b.CreateMany(ctx, res, msgs)
	o.record(ctx, "create_many", res, start, err)
	return out, err
}

func (o *observedBatcher) GetMany(ctx context.Context, res *Resource, keys []string) ([]proto.Message, error) {
	start := time.Now()
	out, err := o.b.GetMany(ctx, res, keys)
	o.record(ctx, "get_many", res, start, err)
	if err == nil {
		o.rows.Add(ctx, float64(len(out)), telemetry.Labels{
			"backend": o.backend, "resource": resourceName(res),
		})
	}
	return out, err
}

type observedWatcher struct {
	*observer
	w Watcher
}

var _ Watcher = (*observedWatcher)(nil)

func (o *observedWatcher) Watch(ctx context.Context, res *Resource, opts WatchOptions) (<-chan Change, error) {
	start := time.Now()
	ch, err := o.w.Watch(ctx, res, opts)
	o.record(ctx, "watch", res, start, err)
	return ch, err
}

type observedBatcherWatcher struct {
	*observer
	b Batcher
	w Watcher
}

var (
	_ Batcher = (*observedBatcherWatcher)(nil)
	_ Watcher = (*observedBatcherWatcher)(nil)
)

func (o *observedBatcherWatcher) CreateMany(ctx context.Context, res *Resource, msgs []proto.Message) ([]WriteResult, error) {
	return (&observedBatcher{observer: o.observer, b: o.b}).CreateMany(ctx, res, msgs)
}

func (o *observedBatcherWatcher) GetMany(ctx context.Context, res *Resource, keys []string) ([]proto.Message, error) {
	return (&observedBatcher{observer: o.observer, b: o.b}).GetMany(ctx, res, keys)
}

func (o *observedBatcherWatcher) Watch(ctx context.Context, res *Resource, opts WatchOptions) (<-chan Change, error) {
	return (&observedWatcher{observer: o.observer, w: o.w}).Watch(ctx, res, opts)
}
