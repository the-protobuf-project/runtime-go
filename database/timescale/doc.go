// Package timescale implements the time-series half of the database contract
// over TimescaleDB.
//
// It embeds the relational driver rather than reimplementing it. TimescaleDB is
// PostgreSQL with an extension: a hypertable is a table, a row is a row, and
// every method on [database.Driver] already works against one — including
// transactions and migrations. A second CRUD implementation would produce two
// ways for the same INSERT to behave and no benefit.
//
// What this package adds is [database.TimeSeries]: partitioning a table by time,
// reading a window of it, and reducing a window into buckets.
//
//	db, _ := timescale.NewProvider(gormDB).SetDatabase(ctx, "metrics")
//	defer db.Close()
//
//	db.Schema.EnsureSchema(ctx, res)                       // the table
//	db.Series.EnsureHypertable(ctx, res,                     // the partitioning
//	    timescale.PartitionedBy(timescale.Daily, "observed_at"))
//
//	db.Create(ctx, res, reading)                           // ordinary write
//	page, _ := db.Series.Range(ctx, res, database.RangeOptions{…})
//	buckets, _ := db.Series.Aggregate(ctx, res, database.AggregateOptions{…})
//
// # The primary-key rule
//
// TimescaleDB requires every unique index to contain the partitioning column,
// because each chunk is its own table and an index on one cannot see the
// others. A descriptor written for any other backend — an id primary key with a
// time column beside it — violates that.
//
// So a resource destined to be a hypertable either makes the time column its
// primary key, or describes its id column without PrimaryKey: the id stays
// [database.Resource.PKColumn] and remains the lookup key for Get, Update and
// Delete, but the table carries no unique constraint. What that costs is
// duplicate detection on Create, which for append-only measurement data is
// rarely what anyone was relying on.
//
// [Driver.EnsureHypertable] checks this before the server does and explains it,
// because a caller hitting it has a design decision to make rather than a bug to
// fix.
//
// # What is deliberately not here
//
// Continuous aggregates, compression policies and downsampling are real and
// useful and entirely TimescaleDB's own. Putting them in [database.TimeSeries]
// would give every other backend a method to refuse; reach past the contract
// through the embedded relational driver for those.
package timescale
