package database

import (
	"context"
	"time"
)

// TimeSeries is implemented by a driver whose backend partitions a resource by
// time and can reduce over it.
//
// It is a capability rather than a contract of its own because a time-series
// store is not an alternative to a record store — TimescaleDB is PostgreSQL, and
// a hypertable is a table. Everything in [Driver] already works against one:
// rows go in through Create, come back through Get and List, and a foreign key
// to a hypertable is an ordinary foreign key.
//
// What does not already work is the part that makes the store worth choosing.
// Partitioning a table by time, reading a window of it, and reducing a window
// into buckets are three things plain SQL either cannot express or expresses so
// badly that everyone writes the same wrong query. Those three are this
// interface, and nothing else is.
//
// # What is deliberately absent
//
// There is no Append. A time-series row is a row and [Driver.Create] writes it;
// a separate method would be the same insert under a name that implied the
// ordinary one did not work here.
//
// There is no downsampling or continuous-aggregate management. Those are real
// and useful and entirely TimescaleDB's own — a second backend would have to
// refuse them, which is the shape this contract avoids. Reach past the contract
// through the driver's own package for those.
type TimeSeries interface {
	// EnsureHypertable partitions the table a resource describes by time, and is
	// safe to call repeatedly.
	//
	// It converts a table that already exists rather than creating one, so
	// [Migrator.EnsureSchema] runs first. That order is not an accident of
	// implementation: the columns come from the descriptor and the partitioning
	// comes from how the data will be read, and the two are decided by different
	// people.
	EnsureHypertable(ctx context.Context, res *Resource, opts HypertableOptions) error

	// Range returns the records whose time column falls inside a window.
	//
	// This is a List with a bound the backend can use. On a partitioned table it
	// is the difference between reading the chunks that overlap the window and
	// reading the table, which at any real volume is the difference between a
	// query and an outage.
	Range(ctx context.Context, res *Resource, opts RangeOptions) (ListResult, error)

	// Aggregate reduces a window into fixed-width buckets.
	//
	// The reduction happens in the backend. Pulling a month of rows across the
	// wire to average them in Go is the thing a time-series store exists to
	// stop, and it is what a caller writes when the contract gives it no way to
	// say what it actually wanted.
	Aggregate(ctx context.Context, res *Resource, opts AggregateOptions) ([]Bucket, error)
}

// HypertableOptions describes how a resource is partitioned.
type HypertableOptions struct {
	// TimeColumn is the column to partition on. Required: there is no sensible
	// default, and guessing one would silently partition by the wrong thing.
	TimeColumn string

	// ChunkInterval is how much time one partition covers. Zero takes the
	// backend's default.
	//
	// The number worth thinking about: a chunk is the unit a query skips and the
	// unit retention drops, so intervals far smaller than the queries produce
	// thousands of partitions to plan across, and far larger ones read data
	// nobody asked for.
	ChunkInterval time.Duration

	// PartitionColumn adds a second dimension, hashed rather than ranged — a
	// device id, a tenant. Empty partitions by time alone.
	//
	// It is worth setting only on a multi-node deployment, where it decides
	// which node a row lands on. On one node it splits chunks without splitting
	// work.
	PartitionColumn string

	// Partitions is how many hash partitions PartitionColumn gets. Zero takes
	// the backend's default, and it is ignored without a PartitionColumn.
	Partitions int

	// Retention drops data older than this. Zero keeps everything.
	//
	// This is the one option here that deletes: a backend applies it as a policy
	// that runs on its own afterwards, so setting it is a standing instruction
	// rather than a one-off, and shortening it later discards what no longer
	// fits.
	Retention time.Duration
}

// RangeOptions bounds a windowed read.
type RangeOptions struct {
	// TimeColumn is the column the window applies to. Required.
	TimeColumn string

	// Start and End bound the window, inclusive of Start and exclusive of End —
	// so consecutive windows tile without overlapping or dropping a row on the
	// boundary. A zero Start or End leaves that side unbounded.
	Start time.Time
	End   time.Time

	// Descending returns the newest first, which is what a caller wants often
	// enough that expressing it through OrderBy every time would be noise.
	Descending bool

	// Filter, PageSize and PageToken behave as they do on [ListOptions].
	Filter    string
	PageSize  int32
	PageToken string
}

// AggregateOptions describes a reduction over a window.
type AggregateOptions struct {
	// TimeColumn is the column bucketed on. Required.
	TimeColumn string

	// Start and End bound the window, as on [RangeOptions].
	Start time.Time
	End   time.Time

	// Every is the bucket width. Required: without it there is one bucket
	// covering everything, which is a different question that Count already
	// answers.
	Every time.Duration

	// Reduce is what to compute per bucket. At least one is required.
	Reduce []Reduction

	// GroupBy splits each bucket by these columns, so a series per device comes
	// back as separate buckets rather than one blended average.
	GroupBy []string

	// Filter narrows the rows considered, as on [ListOptions].
	Filter string

	// Limit caps the buckets returned. Zero takes the backend's default rather
	// than meaning unlimited, for the reason a listing has a page size.
	Limit int
}

// Reducer is one of the reductions every backend here can compute.
//
// A closed set rather than an expression, because an expression would be the
// backend's own SQL and this contract would be passing it through while
// pretending to be portable.
type Reducer string

const (
	// Count counts rows, and is the one reduction that ignores Column.
	Count Reducer = "count"
	Sum   Reducer = "sum"
	Avg   Reducer = "avg"
	Min   Reducer = "min"
	Max   Reducer = "max"
)

// Reduction is one computed value in a bucket.
type Reduction struct {
	// Func is what to compute.
	Func Reducer

	// Column is what to compute it over, and is ignored by [Count].
	Column string

	// As names the result in [Bucket.Values]. Empty takes "func_column", or the
	// function name alone for Count.
	As string
}

// Bucket is one interval of an aggregation.
type Bucket struct {
	// Start is when the bucket begins. Buckets are aligned to the epoch rather
	// than to the window, so the same query over overlapping windows returns the
	// same bucket boundaries — which is what makes two results comparable.
	Start time.Time

	// Group carries the GroupBy column values this bucket covers, and is nil
	// when the aggregation named none.
	Group map[string]string

	// Values are the reductions, keyed by their [Reduction.As] name.
	//
	// Float64 because every reduction here is numeric and one type keeps a
	// caller from switching on which it asked for. A count is exact up to 2^53,
	// which is more rows than a bucket has.
	Values map[string]float64
}
