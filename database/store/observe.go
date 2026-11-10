package store

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Instrument returns db with every operation traced, measured and logged.
//
//	db = store.Instrument(db, store.WithSignals(telemetry.Signals{
//	    Meter: obs.Meter(), Tracer: obs.Tracer(), Logger: obs.Log(),
//	}))
//
// Every call is a [telemetry.Operation] of component "database" on the
// backend: a client span named database.<operation> (create, get, update,
// delete, list, count, exists, create_many, get_many, watch, transaction,
// ensure_schema, drop_schema, has_schema, connect, disconnect, neighbors,
// traverse, ensure_graph, drop_graph, ensure_hypertable, range, aggregate),
// and a share of runtime.operation.count and runtime.operation.duration,
// labeled component, backend, operation and outcome, with the records each
// returned or wrote in runtime.records.read and runtime.records.written. It is
// runtime-rs's store::instrument, operation for operation.
//
// Labels stay bounded: the resource is a span attribute, never a label, and a
// record's key appears nowhere. A missing record is the not_found outcome and
// a duplicate the conflict outcome, neither of which marks the span failed,
// because an absent record is a normal answer and counting it as a failure
// makes an error rate useless.
//
// It takes and returns a [DB] rather than a [Driver] because a wrapper only
// implements what it was written to implement: wrapping the driver alone would
// lose [Batcher] or [Watcher], and leave the transaction, schema, graph and
// series halves unobserved. Each capability the backend has survives,
// instrumented, and one it lacks does not appear. A transaction's own [DB] is
// instrumented as well, so the operations inside it are reported; they run
// under the context the caller passes them, since [Transactional.Run] hands fn
// no context of its own.
//
// With no signals this returns db unchanged rather than a wrapper that costs a
// call and records nothing.
func Instrument(db *DB, opts ...ObserveOption) *DB {
	if db == nil {
		return nil
	}
	var cfg telemetry.Signals
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if !cfg.Enabled() {
		return db
	}
	return instrumentDB(db, &observer{obs: cfg.Observer("database"), backend: db.Backend, name: db.Name})
}

// ObserveOption configures [Instrument].
type ObserveOption func(*telemetry.Signals)

// WithSignals reports through all three of signals' handles.
func WithSignals(s telemetry.Signals) ObserveOption {
	return func(c *telemetry.Signals) {
		if s.Meter != nil {
			c.Meter = s.Meter
		}
		if s.Tracer != nil {
			c.Tracer = s.Tracer
		}
		if s.Logger != nil {
			c.Logger = s.Logger
		}
	}
}

// WithMeter records the operation metrics through m.
func WithMeter(m telemetry.Meter) ObserveOption {
	return WithSignals(telemetry.Signals{Meter: m})
}

// WithLogger logs every operation's completion through l: debug on success,
// info on a missing record or a conflict, error on a failure.
func WithLogger(l telemetry.Logger) ObserveOption {
	return WithSignals(telemetry.Signals{Logger: l})
}

// WithTracer traces every operation through t.
func WithTracer(t telemetry.Tracer) ObserveOption {
	return WithSignals(telemetry.Signals{Tracer: t})
}

// instrumentDB wraps every half of db.
func instrumentDB(db *DB, o *observer) *DB {
	base := &observedDriver{o: o, next: db.Driver}
	var d Driver = base
	b, hasBatcher := db.Driver.(Batcher)
	w, hasWatcher := db.Driver.(Watcher)
	// A capability the inner driver has must survive the wrapping, and one it
	// lacks must not appear: a Batcher that exists only because something was
	// instrumented would fail at the call rather than at construction.
	switch {
	case hasBatcher && hasWatcher:
		d = &observedBatcherWatcher{observedDriver: base, b: b, w: w}
	case hasBatcher:
		d = &observedBatcher{observedDriver: base, b: b}
	case hasWatcher:
		d = &observedWatcher{observedDriver: base, w: w}
	}

	graph := observedGraph{o: o, next: db.Graph}
	out := *db
	out.Driver = d
	out.Tx = observedTx{o: o, next: db.Tx}
	out.Schema = observedSchema{o: o, next: db.Schema}
	out.Graph = graph
	if gm, ok := db.Graph.(GraphMigrator); ok {
		out.Graph = observedGraphMigrator{observedGraph: graph, m: gm}
	}
	out.Series = observedSeries{o: o, next: db.Series}
	return &out
}

// observer starts every operation of one database.
type observer struct {
	obs     *telemetry.Observer
	backend string
	name    string
}

// run reports fn as one operation named operation on res: written records
// counted when it succeeds, read records as fn reports them.
func (o *observer) run(ctx context.Context, operation string, res *Resource, written int, fn func(ctx context.Context) (read int, err error)) error {
	ctx, op := o.start(ctx, operation, res)
	read, err := fn(ctx)
	if err == nil {
		op.ReadRecords(read)
		op.WrittenRecords(written)
	}
	finish(op, err)
	return err
}

// start begins one operation on res.
func (o *observer) start(ctx context.Context, operation string, res *Resource) (context.Context, *telemetry.Operation) {
	attrs := map[string]any{"db.system": o.backend}
	if o.name != "" {
		attrs["db.namespace"] = o.name
	}
	if res != nil {
		attrs["store.resource"] = res.Name
	}
	return o.obs.Start(ctx, o.backend, operation, telemetry.SpanKindClient, telemetry.WithSpanAttributes(attrs))
}

// finish finishes op with err classified as a store error.
func finish(op *telemetry.Operation, err error) {
	outcome := classify(err)
	if outcome.IsError() {
		op.Span().SetError(err)
	}
	op.Finish(outcome)
}

// classify is the outcome of a store error.
func classify(err error) telemetry.Outcome {
	switch {
	case err == nil:
		return telemetry.OutcomeOK
	case errors.Is(err, ErrNotFound):
		return telemetry.OutcomeNotFound
	case errors.Is(err, ErrAlreadyExists):
		return telemetry.OutcomeConflict
	case errors.Is(err, ErrUnimplemented):
		return telemetry.OutcomeUnsupported
	case errors.Is(err, ErrBadConfig):
		return telemetry.OutcomeInvalid
	default:
		return telemetry.OutcomeOf(err)
	}
}

// --- CRUD and its optional halves ------------------------------------------------

type observedDriver struct {
	o    *observer
	next Driver
}

var _ Driver = (*observedDriver)(nil)

// Unwrap returns the driver underneath, for a caller that needs the
// uninstrumented one.
func (d *observedDriver) Unwrap() Driver { return d.next }

func (d *observedDriver) Create(ctx context.Context, res *Resource, msg proto.Message) (out WriteResult, err error) {
	err = d.o.run(ctx, "create", res, 1, func(ctx context.Context) (int, error) {
		out, err = d.next.Create(ctx, res, msg)
		return 0, err
	})
	return out, err
}

func (d *observedDriver) Get(ctx context.Context, res *Resource, key string) (msg proto.Message, err error) {
	err = d.o.run(ctx, "get", res, 0, func(ctx context.Context) (int, error) {
		msg, err = d.next.Get(ctx, res, key)
		return 1, err
	})
	return msg, err
}

func (d *observedDriver) Update(ctx context.Context, res *Resource, msg proto.Message) (out WriteResult, err error) {
	err = d.o.run(ctx, "update", res, 1, func(ctx context.Context) (int, error) {
		out, err = d.next.Update(ctx, res, msg)
		return 0, err
	})
	return out, err
}

func (d *observedDriver) Delete(ctx context.Context, res *Resource, key string) error {
	return d.o.run(ctx, "delete", res, 1, func(ctx context.Context) (int, error) {
		return 0, d.next.Delete(ctx, res, key)
	})
}

// List also counts the rows it returned: a page taking a second because it
// returned ten thousand rows and one taking a second because the query is bad
// look identical in a duration alone.
func (d *observedDriver) List(ctx context.Context, res *Resource, opts ListOptions) (out ListResult, err error) {
	err = d.o.run(ctx, "list", res, 0, func(ctx context.Context) (int, error) {
		out, err = d.next.List(ctx, res, opts)
		return len(out.Items), err
	})
	return out, err
}

func (d *observedDriver) Count(ctx context.Context, res *Resource, opts ListOptions) (n int64, err error) {
	err = d.o.run(ctx, "count", res, 0, func(ctx context.Context) (int, error) {
		n, err = d.next.Count(ctx, res, opts)
		return 0, err
	})
	return n, err
}

func (d *observedDriver) Exists(ctx context.Context, res *Resource, key string) (ok bool, err error) {
	err = d.o.run(ctx, "exists", res, 0, func(ctx context.Context) (int, error) {
		ok, err = d.next.Exists(ctx, res, key)
		return 0, err
	})
	return ok, err
}

type observedBatcher struct {
	*observedDriver
	b Batcher
}

var _ Batcher = (*observedBatcher)(nil)

func (d *observedBatcher) CreateMany(ctx context.Context, res *Resource, msgs []proto.Message) (out []WriteResult, err error) {
	err = d.o.run(ctx, "create_many", res, len(msgs), func(ctx context.Context) (int, error) {
		out, err = d.b.CreateMany(ctx, res, msgs)
		return 0, err
	})
	return out, err
}

func (d *observedBatcher) GetMany(ctx context.Context, res *Resource, keys []string) (out []proto.Message, err error) {
	err = d.o.run(ctx, "get_many", res, 0, func(ctx context.Context) (int, error) {
		out, err = d.b.GetMany(ctx, res, keys)
		return len(out), err
	})
	return out, err
}

type observedWatcher struct {
	*observedDriver
	w Watcher
}

var _ Watcher = (*observedWatcher)(nil)

// Watch is one operation for the watch's whole life: opening it, and every
// change delivered until the channel closes, which finishes it with the
// changes it read. A watch that never opened finishes with the error.
func (d *observedWatcher) Watch(ctx context.Context, res *Resource, opts WatchOptions) (<-chan Change, error) {
	wctx, op := d.o.start(ctx, "watch", res)
	in, err := d.w.Watch(wctx, res, opts)
	if err != nil {
		finish(op, err)
		return nil, err
	}
	out := make(chan Change)
	go func() {
		defer close(out)
		read := 0
		for c := range in {
			select {
			case out <- c:
				read++
			case <-ctx.Done():
				op.ReadRecords(read)
				op.Finish(telemetry.OutcomeCancelled)
				return
			}
		}
		op.ReadRecords(read)
		if ctx.Err() != nil {
			op.Finish(telemetry.OutcomeCancelled)
			return
		}
		op.Finish(telemetry.OutcomeOK)
	}()
	return out, nil
}

type observedBatcherWatcher struct {
	*observedDriver
	b Batcher
	w Watcher
}

var (
	_ Batcher = (*observedBatcherWatcher)(nil)
	_ Watcher = (*observedBatcherWatcher)(nil)
)

func (d *observedBatcherWatcher) CreateMany(ctx context.Context, res *Resource, msgs []proto.Message) ([]WriteResult, error) {
	return (&observedBatcher{observedDriver: d.observedDriver, b: d.b}).CreateMany(ctx, res, msgs)
}

func (d *observedBatcherWatcher) GetMany(ctx context.Context, res *Resource, keys []string) ([]proto.Message, error) {
	return (&observedBatcher{observedDriver: d.observedDriver, b: d.b}).GetMany(ctx, res, keys)
}

func (d *observedBatcherWatcher) Watch(ctx context.Context, res *Resource, opts WatchOptions) (<-chan Change, error) {
	return (&observedWatcher{observedDriver: d.observedDriver, w: d.w}).Watch(ctx, res, opts)
}

// --- transactions, schema, graph, series ------------------------------------------

type observedTx struct {
	o    *observer
	next Transactional
}

// Run is one operation around the whole transaction, and hands fn an
// instrumented transaction, so each operation inside is reported too.
func (t observedTx) Run(ctx context.Context, fn func(tx *DB) error) error {
	return t.o.run(ctx, "transaction", nil, 0, func(ctx context.Context) (int, error) {
		return 0, t.next.Run(ctx, func(tx *DB) error { return fn(instrumentDB(tx, t.o)) })
	})
}

type observedSchema struct {
	o    *observer
	next Migrator
}

func (s observedSchema) EnsureSchema(ctx context.Context, res *Resource) error {
	return s.o.run(ctx, "ensure_schema", res, 0, func(ctx context.Context) (int, error) {
		return 0, s.next.EnsureSchema(ctx, res)
	})
}

func (s observedSchema) DropSchema(ctx context.Context, res *Resource) error {
	return s.o.run(ctx, "drop_schema", res, 0, func(ctx context.Context) (int, error) {
		return 0, s.next.DropSchema(ctx, res)
	})
}

func (s observedSchema) HasSchema(ctx context.Context, res *Resource) (ok bool, err error) {
	err = s.o.run(ctx, "has_schema", res, 0, func(ctx context.Context) (int, error) {
		ok, err = s.next.HasSchema(ctx, res)
		return 0, err
	})
	return ok, err
}

type observedGraph struct {
	o    *observer
	next Graph
}

func (g observedGraph) Connect(ctx context.Context, edge *Resource, from, to Ref, props proto.Message) (out Edge, err error) {
	err = g.o.run(ctx, "connect", edge, 1, func(ctx context.Context) (int, error) {
		out, err = g.next.Connect(ctx, edge, from, to, props)
		return 0, err
	})
	return out, err
}

func (g observedGraph) Disconnect(ctx context.Context, edge *Resource, key string) error {
	return g.o.run(ctx, "disconnect", edge, 1, func(ctx context.Context) (int, error) {
		return 0, g.next.Disconnect(ctx, edge, key)
	})
}

func (g observedGraph) Neighbors(ctx context.Context, from Ref, opts TraverseOptions) (out []Edge, err error) {
	err = g.o.run(ctx, "neighbors", nil, 0, func(ctx context.Context) (int, error) {
		out, err = g.next.Neighbors(ctx, from, opts)
		return len(out), err
	})
	return out, err
}

func (g observedGraph) Traverse(ctx context.Context, from Ref, opts TraverseOptions) (out []Path, err error) {
	err = g.o.run(ctx, "traverse", nil, 0, func(ctx context.Context) (int, error) {
		out, err = g.next.Traverse(ctx, from, opts)
		return len(out), err
	})
	return out, err
}

type observedGraphMigrator struct {
	observedGraph
	m GraphMigrator
}

var _ GraphMigrator = observedGraphMigrator{}

func (g observedGraphMigrator) EnsureGraph(ctx context.Context, name string, defs []EdgeDefinition) error {
	return g.o.run(ctx, "ensure_graph", nil, 0, func(ctx context.Context) (int, error) {
		return 0, g.m.EnsureGraph(ctx, name, defs)
	})
}

func (g observedGraphMigrator) DropGraph(ctx context.Context, name string) error {
	return g.o.run(ctx, "drop_graph", nil, 0, func(ctx context.Context) (int, error) {
		return 0, g.m.DropGraph(ctx, name)
	})
}

type observedSeries struct {
	o    *observer
	next TimeSeries
}

func (s observedSeries) EnsureHypertable(ctx context.Context, res *Resource, opts HypertableOptions) error {
	return s.o.run(ctx, "ensure_hypertable", res, 0, func(ctx context.Context) (int, error) {
		return 0, s.next.EnsureHypertable(ctx, res, opts)
	})
}

func (s observedSeries) Range(ctx context.Context, res *Resource, opts RangeOptions) (out ListResult, err error) {
	err = s.o.run(ctx, "range", res, 0, func(ctx context.Context) (int, error) {
		out, err = s.next.Range(ctx, res, opts)
		return len(out.Items), err
	})
	return out, err
}

func (s observedSeries) Aggregate(ctx context.Context, res *Resource, opts AggregateOptions) (out []Bucket, err error) {
	err = s.o.run(ctx, "aggregate", res, 0, func(ctx context.Context) (int, error) {
		out, err = s.next.Aggregate(ctx, res, opts)
		return len(out), err
	})
	return out, err
}

var (
	_ Transactional = observedTx{}
	_ Migrator      = observedSchema{}
	_ Graph         = observedGraph{}
	_ TimeSeries    = observedSeries{}
)
