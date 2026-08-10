package timescale_test

// These need a live TimescaleDB and skip without one:
//
//	docker compose -f ../docker/compose.yaml up -d timescaledb
//	go test ./...

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database/store"
	"github.com/the-protobuf-project/runtime-go/database/timescale"
)

const dialTimeout = 2 * time.Second

var seq atomic.Int64

func addr() string {
	host, port := os.Getenv("TIMESCALE_TEST_HOST"), os.Getenv("TIMESCALE_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "5433"
	}
	return net.JoinHostPort(host, port)
}

func openDB(t *testing.T) *gorm.DB {
	t.Helper()
	target := addr()
	ctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		t.Skipf("no TimescaleDB at %s: %v", target, err)
	}
	_ = conn.Close()

	host, port, _ := net.SplitHostPort(target)
	dsn := fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=runtime sslmode=disable", host, port)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		TranslateError: true,
		Logger:         logger.Discard,
	})
	if err != nil {
		t.Skipf("cannot open TimescaleDB: %v", err)
	}
	return db
}

type fixture struct {
	db  *store.DB
	res *store.Resource
	md  protoreflect.MessageDescriptor
}

func setup(t *testing.T) fixture {
	t.Helper()
	gdb := openDB(t)

	schema := fmt.Sprintf("itest_%d_%d", os.Getpid(), seq.Add(1))
	if err := gdb.Exec(fmt.Sprintf("CREATE SCHEMA %q", schema)).Error; err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_ = gdb.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)).Error
	})

	db, err := timescale.NewProvider(gdb).SetDatabase(t.Context(), schema)
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	md := readingMD(t)
	res := readingRes(md)
	if err := db.Schema.EnsureSchema(t.Context(), res); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return fixture{db: db, res: res, md: md}
}

func readingMD(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("itest/v1/timescale_test.proto"),
		Package:    proto.String("itestts.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Reading"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("device"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("celsius"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("observed_at"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".google.protobuf.Timestamp"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func readingRes(md protoreflect.MessageDescriptor) *store.Resource {
	return &store.Resource{
		Name: "Reading", Table: "readings", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			// The lookup key, but deliberately not a SQL PRIMARY KEY: see
			// TestUniqueIndexConflictIsExplained.
			{Name: "id", Field: "id", Kind: store.KindString, SQLType: "TEXT", NotNull: true},
			{Name: "device", Field: "device", Kind: store.KindString, SQLType: "TEXT"},
			{Name: "celsius", Field: "celsius", Kind: store.KindFloat, SQLType: "DOUBLE PRECISION"},
			{Name: "observed_at", Field: "observed_at", Kind: store.KindTimestamp, SQLType: "TIMESTAMPTZ", NotNull: true},
		},
	}
}

var base = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

func newReading(md protoreflect.MessageDescriptor, id, device string, celsius float64, at time.Time) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("device"), protoreflect.ValueOfString(device))
	m.Set(f.ByName("celsius"), protoreflect.ValueOfFloat64(celsius))
	ts := timestamppb.New(at)
	m.Set(f.ByName("observed_at"), protoreflect.ValueOfMessage(ts.ProtoReflect()))
	return msg
}

// seedHourly writes one reading per hour for n hours, alternating two devices.
func seedHourly(t *testing.T, f fixture, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		device := "dev-a"
		if i%2 == 1 {
			device = "dev-b"
		}
		at := base.Add(time.Duration(i) * time.Hour)
		id := fmt.Sprintf("r-%03d", i)
		if _, err := f.db.Create(ctx, f.res, newReading(f.md, id, device, float64(i), at)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

// TimescaleDB is PostgreSQL, so everything the relational driver does already
// works — that is the reason this package embeds it rather than reimplementing.
func TestCRUDStillWorks(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Create(ctx, f.res, newReading(f.md, "r-1", "dev-a", 21.5, base)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := f.db.Get(ctx, f.res, "r-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if c := got.ProtoReflect().Get(f.md.Fields().ByName("celsius")).Float(); c != 21.5 {
		t.Errorf("celsius = %v", c)
	}
	if n, _ := f.db.Count(ctx, f.res, store.ListOptions{}); n != 1 {
		t.Errorf("Count = %d, want 1", n)
	}
	// And the capabilities the relational driver has came across with it.
	if err := f.db.Tx.Run(ctx, func(tx *store.DB) error {
		_, cerr := tx.Create(ctx, f.res, newReading(f.md, "r-2", "dev-a", 22, base.Add(time.Hour)))
		return cerr
	}); err != nil {
		t.Errorf("transactions did not survive the embedding: %v", err)
	}
}

func TestEnsureHypertable(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	opts := store.HypertableOptions{
		TimeColumn:    "observed_at",
		ChunkInterval: 24 * time.Hour,
	}
	if err := f.db.Series.EnsureHypertable(ctx, f.res, opts); err != nil {
		t.Fatalf("EnsureHypertable: %v", err)
	}
	// Safe to call repeatedly, because it runs on startup.
	if err := f.db.Series.EnsureHypertable(ctx, f.res, opts); err != nil {
		t.Fatalf("EnsureHypertable twice: %v", err)
	}

	// And the table really is partitioned now.
	gdb := openDB(t)
	var n int64
	if err := gdb.Raw(
		"SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = ?",
		f.res.Table).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Error("the table was not converted to a hypertable")
	}

	// Rows still go in and come back the ordinary way.
	seedHourly(t, f, 5)
	if got, _ := f.db.Count(ctx, f.res, store.ListOptions{}); got != 5 {
		t.Errorf("Count after partitioning = %d, want 5", got)
	}
}

// Converting a table that already holds rows is the normal case, not the
// exception — someone adds partitioning to something already in production.
func TestEnsureHypertableMigratesExistingRows(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	seedHourly(t, f, 10)
	if err := f.db.Series.EnsureHypertable(ctx, f.res, store.HypertableOptions{
		TimeColumn: "observed_at",
	}); err != nil {
		t.Fatalf("EnsureHypertable over existing rows: %v", err)
	}
	if got, _ := f.db.Count(ctx, f.res, store.ListOptions{}); got != 10 {
		t.Errorf("%d rows survived the conversion, want 10", got)
	}
}

func TestEnsureHypertableChecksTheColumn(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if err := f.db.Series.EnsureHypertable(ctx, f.res, store.HypertableOptions{
		TimeColumn: "nosuch",
	}); err == nil {
		t.Error("a missing time column was accepted")
	}
	// Partitioning on something that is not time would order rows by something
	// that is not time.
	err := f.db.Series.EnsureHypertable(ctx, f.res, store.HypertableOptions{
		TimeColumn: "device",
	})
	if err == nil {
		t.Fatal("a non-timestamp partition column was accepted")
	}
	if !strings.Contains(err.Error(), "timestamp") {
		t.Errorf("the error does not say why: %v", err)
	}
}

func TestRange(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	if err := f.db.Series.EnsureHypertable(ctx, f.res, store.HypertableOptions{
		TimeColumn: "observed_at", ChunkInterval: 6 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	seedHourly(t, f, 24)

	// A window inclusive of start and exclusive of end, so consecutive windows
	// tile without double-counting the boundary row.
	out, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at",
		Start:      base.Add(4 * time.Hour),
		End:        base.Add(8 * time.Hour),
		PageSize:   100,
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(out.Items) != 4 {
		t.Fatalf("window returned %d rows, want 4", len(out.Items))
	}
	if out.Total != 4 {
		t.Errorf("Total = %d, want 4", out.Total)
	}
	first := out.Items[0].ProtoReflect().Get(f.md.Fields().ByName("id")).String()
	if first != "r-004" {
		t.Errorf("first id = %q, want r-004", first)
	}

	// The two halves of a split window tile exactly.
	lower, _ := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at", Start: base, End: base.Add(12 * time.Hour), PageSize: 100,
	})
	upper, _ := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at", Start: base.Add(12 * time.Hour), End: base.Add(24 * time.Hour), PageSize: 100,
	})
	if lower.Total+upper.Total != 24 {
		t.Errorf("split windows covered %d rows, want 24 — they overlap or drop the boundary",
			lower.Total+upper.Total)
	}

	// Newest first.
	desc, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at", Descending: true, PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	newest := desc.Items[0].ProtoReflect().Get(f.md.Fields().ByName("id")).String()
	if newest != "r-023" {
		t.Errorf("newest id = %q, want r-023", newest)
	}

	// And the filter narrows within the window.
	filtered, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at", Filter: "device = dev-a", PageSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Total != 12 {
		t.Errorf("filtered window returned %d rows, want 12", filtered.Total)
	}
}

func TestRangePages(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	seedHourly(t, f, 25)

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > 30 {
			t.Fatal("paging did not terminate")
		}
		out, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{
			TimeColumn: "observed_at", PageSize: 10, PageToken: token,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Items {
			seen = append(seen, m.ProtoReflect().Get(f.md.Fields().ByName("id")).String())
		}
		if out.NextPageToken == "" {
			break
		}
		token = out.NextPageToken
	}
	if len(seen) != 25 {
		t.Fatalf("paged over %d rows, want 25", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] >= seen[i] {
			t.Fatalf("out of order at %d: %v", i, seen[i-1:i+1])
		}
	}
}

// The reduction runs in the database. Pulling a month of rows across the wire to
// average them in Go is the thing this store exists to stop.
func TestAggregate(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	if err := f.db.Series.EnsureHypertable(ctx, f.res, store.HypertableOptions{
		TimeColumn: "observed_at", ChunkInterval: 6 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	seedHourly(t, f, 24) // celsius = 0..23, one per hour

	buckets, err := f.db.Series.Aggregate(ctx, f.res, store.AggregateOptions{
		TimeColumn: "observed_at",
		Start:      base,
		End:        base.Add(24 * time.Hour),
		Every:      6 * time.Hour,
		Reduce: []store.Reduction{
			{Func: store.Count},
			{Func: store.Avg, Column: "celsius"},
			{Func: store.Min, Column: "celsius", As: "coldest"},
			{Func: store.Max, Column: "celsius", As: "hottest"},
		},
	})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(buckets) != 4 {
		t.Fatalf("got %d buckets, want 4", len(buckets))
	}

	first := buckets[0]
	if first.Values["count"] != 6 {
		t.Errorf("first bucket count = %v, want 6", first.Values["count"])
	}
	if first.Values["avg_celsius"] != 2.5 { // mean of 0..5
		t.Errorf("first bucket avg = %v, want 2.5", first.Values["avg_celsius"])
	}
	if first.Values["coldest"] != 0 || first.Values["hottest"] != 5 {
		t.Errorf("first bucket min/max = %v/%v, want 0/5", first.Values["coldest"], first.Values["hottest"])
	}
	if !first.Start.Equal(base) {
		t.Errorf("first bucket starts at %v, want %v", first.Start, base)
	}

	last := buckets[3]
	if last.Values["hottest"] != 23 {
		t.Errorf("last bucket max = %v, want 23", last.Values["hottest"])
	}
}

// A series per device has to come back as separate buckets, not one blended
// average.
func TestAggregateGroups(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	seedHourly(t, f, 12)

	buckets, err := f.db.Series.Aggregate(ctx, f.res, store.AggregateOptions{
		TimeColumn: "observed_at",
		Every:      12 * time.Hour,
		GroupBy:    []string{"device"},
		Reduce:     []store.Reduction{{Func: store.Count}},
	})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("got %d buckets, want 2 (one per device)", len(buckets))
	}
	byDevice := map[string]float64{}
	for _, b := range buckets {
		if b.Group == nil {
			t.Fatal("a grouped bucket carries no Group")
		}
		byDevice[b.Group["device"]] = b.Values["count"]
	}
	if byDevice["dev-a"] != 6 || byDevice["dev-b"] != 6 {
		t.Errorf("counts by device = %v, want 6 each", byDevice)
	}
}

func TestAggregateRefusesWhatItCannotMean(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Series.Aggregate(ctx, f.res, store.AggregateOptions{
		TimeColumn: "observed_at",
		Reduce:     []store.Reduction{{Func: store.Count}},
	}); err == nil {
		t.Error("an aggregation with no bucket width was accepted")
	}
	if _, err := f.db.Series.Aggregate(ctx, f.res, store.AggregateOptions{
		TimeColumn: "observed_at", Every: time.Hour,
	}); err == nil {
		t.Error("an aggregation with no reduction was accepted")
	}
	// Averaging text is not meaningful, and saying so beats a database error.
	_, err := f.db.Series.Aggregate(ctx, f.res, store.AggregateOptions{
		TimeColumn: "observed_at", Every: time.Hour,
		Reduce: []store.Reduction{{Func: store.Avg, Column: "device"}},
	})
	if err == nil {
		t.Fatal("averaging a text column was accepted")
	}
	if !strings.Contains(err.Error(), "not a number") {
		t.Errorf("the error does not say why: %v", err)
	}
	// A reduction outside the closed set.
	if _, err := f.db.Series.Aggregate(ctx, f.res, store.AggregateOptions{
		TimeColumn: "observed_at", Every: time.Hour,
		Reduce: []store.Reduction{{Func: "median", Column: "celsius"}},
	}); err == nil {
		t.Error("a reduction this contract does not define was accepted")
	}
}

// A filter and a window arrive from a request on exactly this kind of endpoint.
func TestFilterIsBoundNotInterpolated(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	seedHourly(t, f, 4)

	// A value that would end the statement if it were pasted in.
	out, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at",
		Filter:     "device = '; DROP TABLE readings; --",
		PageSize:   10,
	})
	if err != nil {
		t.Fatalf("Range with an injected value: %v", err)
	}
	if out.Total != 0 {
		t.Errorf("the injected value matched %d rows", out.Total)
	}
	// The table is still there.
	if n, cerr := f.db.Count(ctx, f.res, store.ListOptions{}); cerr != nil || n != 4 {
		t.Errorf("Count after the injected filter = %d, %v; want 4", n, cerr)
	}

	// A column nobody declared is refused rather than reaching the query.
	if _, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{
		TimeColumn: "observed_at", Filter: "nosuch = 1",
	}); err == nil {
		t.Error("a filter naming an unknown column was accepted")
	}
}

func TestTimeColumnIsRequired(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{}); err == nil {
		t.Error("a range with no time column was accepted")
	}
	if _, err := f.db.Series.Range(ctx, f.res, store.RangeOptions{TimeColumn: "nosuch"}); err == nil {
		t.Error("a range on an unknown column was accepted")
	}
}

// A PostgreSQL without the extension is a startup error naming what is missing,
// not a confusing failure inside EnsureHypertable.
func TestExtensionIsCheckedAtSelection(t *testing.T) {
	gdb := openDB(t)
	ctx := context.Background()

	// The real server has it, so this proves the check passes rather than
	// always failing.
	if _, err := timescale.NewProvider(gdb).SetDatabase(ctx, "public"); err != nil {
		t.Fatalf("SetDatabase against a real TimescaleDB: %v", err)
	}
}

func TestSeriesRefusedOnABackendWithoutIt(t *testing.T) {
	db := store.Build(bare{}, "sqlite", "", nil)
	err := db.Series.EnsureHypertable(context.Background(), nil, store.HypertableOptions{})
	if !errors.Is(err, store.ErrUnimplemented) {
		t.Fatalf("EnsureHypertable = %v, want ErrUnimplemented", err)
	}
	if !strings.Contains(err.Error(), "sqlite") {
		t.Errorf("the refusal does not name the backend: %v", err)
	}
}

type bare struct{ store.Driver }

// A descriptor written for any other backend has an id primary key and a time
// column beside it, which is exactly what TimescaleDB cannot partition. The
// refusal has to explain the rule rather than pass through TS103.
func TestUniqueIndexConflictIsExplained(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	// A second resource, this time with a real primary key on the id.
	keyed := readingRes(f.md)
	keyed.Table = "keyed_readings"
	for i := range keyed.Columns {
		if keyed.Columns[i].Name == "id" {
			keyed.Columns[i].PrimaryKey = true
		}
	}
	if err := f.db.Schema.EnsureSchema(ctx, keyed); err != nil {
		t.Fatal(err)
	}

	err := f.db.Series.EnsureHypertable(ctx, keyed, store.HypertableOptions{
		TimeColumn: "observed_at",
	})
	if err == nil {
		t.Fatal("a table with a conflicting unique index was partitioned")
	}
	for _, want := range []string{"observed_at", "unique", "partitioning column"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}
