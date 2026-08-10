package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database/examples/store/internal/model"
	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/database/timescale"
)

const dsn = "host=localhost port=5433 user=postgres password=postgres dbname=runtime sslmode=disable"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	client, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		TranslateError: true,
		Logger:         logger.Discard,
	})
	if err != nil {
		return err
	}

	schema := "examples_ts"
	if err = client.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schema)).Error; err != nil {
		return err
	}
	defer func() {
		_ = client.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)).Error
	}()

	// SetDatabase checks for the extension here, so a plain PostgreSQL is a
	// startup error naming what is missing rather than a confusing failure
	// later inside EnsureHypertable.
	db, err := timescale.NewProvider(client).SetDatabase(ctx, schema)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	log.Printf("on %s schema %q", db.Backend, db.Name)

	readings := model.ReadingResource()

	// The table first, then the partitioning. That order is not incidental: the
	// columns come from the descriptor and the partitioning comes from how the
	// data will be read, and those are decided by different people.
	if err = db.Schema.EnsureSchema(ctx, readings); err != nil {
		return err
	}
	if err = db.Series.EnsureHypertable(ctx, readings,
		timescale.PartitionedBy(timescale.Daily, "observed_at")); err != nil {
		return err
	}
	log.Print("partitioned by observed_at, one chunk per day")

	// --- write ---
	// Ordinary writes. A time-series row is a row, so there is no separate
	// append path that would imply Create does not work here.
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for i := range 48 {
		sensor := "kitchen"
		if i%2 == 1 {
			sensor = "garage"
		}
		at := base.Add(time.Duration(i) * time.Hour)
		celsius := 18 + float64(i%12)
		if _, err = db.Create(ctx, readings,
			model.Reading(fmt.Sprintf("r-%03d", i), sensor, celsius, at)); err != nil {
			return err
		}
	}

	// --- a window ---
	// Range is a List with a bound the backend can use: on a partitioned table
	// it reads the chunks that overlap the window rather than the table.
	// Inclusive of start, exclusive of end, so consecutive windows tile.
	day2, err := db.Series.Range(ctx, readings, store.RangeOptions{
		TimeColumn: "observed_at",
		Start:      base.Add(24 * time.Hour),
		End:        base.Add(48 * time.Hour),
		PageSize:   100,
	})
	if err != nil {
		return err
	}
	log.Printf("second day: %d readings", day2.Total)

	// --- a reduction ---
	// This is the method the store exists for. Without it a caller pulls every
	// row across the wire to average them here, which is exactly what a
	// time-series database is for avoiding.
	buckets, err := db.Series.Aggregate(ctx, readings, store.AggregateOptions{
		TimeColumn: "observed_at",
		Start:      base,
		End:        base.Add(48 * time.Hour),
		Every:      12 * time.Hour,
		GroupBy:    []string{"sensor"},
		Reduce: []store.Reduction{
			{Func: store.Count},
			{Func: store.Avg, Column: "celsius"},
			{Func: store.Max, Column: "celsius", As: "peak"},
		},
	})
	if err != nil {
		return err
	}
	log.Printf("%d buckets of 12h, split by sensor:", len(buckets))
	for _, b := range buckets {
		log.Printf("  %s %-8s n=%.0f avg=%.1f peak=%.1f",
			b.Start.Format("Jan 2 15:04"), b.Group["sensor"],
			b.Values["count"], b.Values["avg_celsius"], b.Values["peak"])
	}

	// --- what it refuses ---
	// An unbounded reduction is a different question, and averaging text is not
	// a question at all.
	_, err = db.Series.Aggregate(ctx, readings, store.AggregateOptions{
		TimeColumn: "observed_at", Every: time.Hour,
		Reduce: []store.Reduction{{Func: store.Avg, Column: "sensor"}},
	})
	log.Printf("averaging a text column: %v", err)

	return nil
}
