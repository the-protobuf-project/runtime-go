package timescale

import (
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Chunk intervals worth starting from.
//
// The number matters more than it looks: a chunk is the unit a query skips and
// the unit retention drops, so one much smaller than the queries produces
// thousands of partitions to plan across, and one much larger reads data nobody
// asked for. The usual guidance is that a chunk should hold roughly what a
// typical query reads.
//
// These name the common shapes so a caller starts somewhere defensible rather
// than at zero, which takes the backend's default of seven days whatever the
// data looks like.
var (
	// Hourly suits high-frequency data queried in minutes — device telemetry,
	// request traces.
	Hourly = database.HypertableOptions{ChunkInterval: time.Hour}

	// Daily suits data queried in hours or days, which is most of it.
	Daily = database.HypertableOptions{ChunkInterval: 24 * time.Hour}

	// Weekly suits sparse data queried in weeks or months — daily rollups,
	// billing records.
	Weekly = database.HypertableOptions{ChunkInterval: 7 * 24 * time.Hour}
)

// PartitionedBy returns a copy of opts partitioned on the given column.
//
//	db.Series.EnsureHypertable(ctx, res,
//	    timescale.PartitionedBy(timescale.Daily, "observed_at"))
func PartitionedBy(opts database.HypertableOptions, column string) database.HypertableOptions {
	opts.TimeColumn = column
	return opts
}

// KeptFor returns a copy of opts that drops data older than d.
//
// A standing instruction rather than a one-off: the policy runs on its own
// afterwards, and shortening it later discards what no longer fits.
func KeptFor(opts database.HypertableOptions, d time.Duration) database.HypertableOptions {
	opts.Retention = d
	return opts
}
