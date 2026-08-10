package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/database/timescale"
)

// Reading is a measurement.
//
// The id is tagged key rather than pk: it stays the lookup key for Get, Update
// and Delete, but declares no constraint. TimescaleDB requires every unique
// index to contain the partitioning column, and this table is partitioned on
// observed_at — so a primary key on id alone is one the server refuses.
type Reading struct {
	ID       string    `db:"id,key"`
	Sensor   string    `db:"sensor"`
	Celsius  float64   `db:"celsius"`
	Observed time.Time `db:"observed_at,notnull"`
}

const dsn = "host=localhost port=5433 user=postgres password=postgres dbname=runtime sslmode=disable"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	client, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		TranslateError: true, Logger: logger.Discard,
	})
	if err != nil {
		return err
	}

	schema := "examples_simple_ts"
	if err = client.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schema)).Error; err != nil {
		return err
	}
	defer func() {
		_ = client.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)).Error
	}()

	db, err := timescale.NewProvider(client).SetDatabase(ctx, schema)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	readings, err := database.Collection[Reading](db, "readings")
	if err != nil {
		return err
	}
	if err = readings.EnsureSchema(ctx); err != nil {
		return err
	}
	log.Printf("on %s, storing %T", db.Backend, Reading{})

	// The table comes from the struct; the partitioning comes from how the data
	// will be read, so it is a separate call taking the derived descriptor.
	if err = db.Series.EnsureHypertable(ctx, readings.Resource(),
		timescale.PartitionedBy(timescale.Daily, "observed_at")); err != nil {
		return err
	}
	log.Print("partitioned by observed_at")

	// --- write, as ordinary Go values ---
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for i := range 48 {
		sensor := "kitchen"
		if i%2 == 1 {
			sensor = "garage"
		}
		if _, cerr := readings.Create(ctx, Reading{
			ID:       fmt.Sprintf("r-%03d", i),
			Sensor:   sensor,
			Celsius:  18 + float64(i%12),
			Observed: base.Add(time.Duration(i) * time.Hour),
		}); cerr != nil {
			return cerr
		}
	}

	// Ordinary reads work exactly as on any other backend.
	one, err := readings.Get(ctx, "r-005")
	if err != nil {
		return err
	}
	log.Printf("read back a %T: %s %.1fC at %s",
		one, one.Sensor, one.Celsius, one.Observed.Format(time.RFC3339))

	// --- the part that is not storage ---
	buckets, err := db.Series.Aggregate(ctx, readings.Resource(), store.AggregateOptions{
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

	return nil
}
