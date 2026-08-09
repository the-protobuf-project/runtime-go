package timescale

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/core"
)

// defaultBucketLimit bounds an aggregation that names none, for the reason a
// listing has a page size: a caller who forgot is better served by a page than
// by a year of minutes.
const defaultBucketLimit = 1000

// EnsureHypertable partitions the table a resource describes by time.
//
// It converts an existing table, so [database.Migrator.EnsureSchema] runs first.
// TimescaleDB requires that order too — create_hypertable takes a table — and
// the contract says so rather than hiding a CREATE TABLE in here where a caller
// looking for one would not find it.
//
// migrate_data is deliberately on. Converting a table that already holds rows
// otherwise fails, and a caller adding partitioning to something already in
// production is the normal case rather than the exception.
func (d *Driver) EnsureHypertable(ctx context.Context, res *database.Resource, opts database.HypertableOptions) error {
	if res == nil {
		return fmt.Errorf("timescale: EnsureHypertable needs a resource")
	}
	col, ok := res.LookupColumn(opts.TimeColumn)
	if !ok {
		return fmt.Errorf("timescale: resource %q has no column %q to partition on", res.Name, opts.TimeColumn)
	}
	if col.Kind != database.KindTimestamp {
		return fmt.Errorf(
			"timescale: column %q is %v, not a timestamp; partitioning on it would order rows by something that is not time",
			opts.TimeColumn, col.Kind)
	}

	if cerr := d.checkUniqueIndexes(ctx, res, col.Name); cerr != nil {
		return cerr
	}

	// Every argument is cast. create_hypertable is polymorphic, and a bare
	// placeholder arrives as "unknown" — PostgreSQL cannot pick an overload from
	// that and fails with a message about polymorphic types that says nothing
	// about the actual problem.
	args := []any{d.table(res), col.Name}
	sql := "SELECT create_hypertable(?::regclass, ?::name, if_not_exists => TRUE, migrate_data => TRUE"
	if opts.ChunkInterval > 0 {
		sql += ", chunk_time_interval => ?::interval"
		args = append(args, interval(opts.ChunkInterval))
	}
	if opts.PartitionColumn != "" {
		pc, pok := res.LookupColumn(opts.PartitionColumn)
		if !pok {
			return fmt.Errorf("timescale: resource %q has no column %q to partition on", res.Name, opts.PartitionColumn)
		}
		partitions := opts.Partitions
		if partitions <= 0 {
			partitions = 4
		}
		sql += ", partitioning_column => ?::name, number_partitions => ?::integer"
		args = append(args, pc.Name, partitions)
	}
	sql += ")"

	if err := d.db.WithContext(ctx).Exec(sql, args...).Error; err != nil {
		return fmt.Errorf("timescale: cannot make %s a hypertable: %w", d.table(res), err)
	}

	if opts.Retention > 0 {
		// A standing instruction rather than a one-off: the policy runs on its
		// own afterwards and drops chunks that fall out of the window.
		if err := d.db.WithContext(ctx).Exec(
			"SELECT add_retention_policy(?::regclass, ?::interval, if_not_exists => TRUE)",
			d.table(res), interval(opts.Retention)).Error; err != nil {
			return fmt.Errorf("timescale: cannot set retention on %s: %w", d.table(res), err)
		}
	}
	return nil
}

// Range returns the records whose time column falls inside a window.
func (d *Driver) Range(ctx context.Context, res *database.Resource, opts database.RangeOptions) (database.ListResult, error) {
	if res == nil {
		return database.ListResult{}, fmt.Errorf("timescale: Range needs a resource")
	}
	timeCol, err := d.timeColumn(res, opts.TimeColumn)
	if err != nil {
		return database.ListResult{}, err
	}

	where, args, err := buildWhere(res, opts.Filter, timeCol, opts.Start, opts.End)
	if err != nil {
		return database.ListResult{}, err
	}

	var total int64
	countSQL := fmt.Sprintf("SELECT count(*) FROM %s %s", d.table(res), where)
	if cerr := d.db.WithContext(ctx).Raw(countSQL, args...).Scan(&total).Error; cerr != nil {
		return database.ListResult{}, fmt.Errorf("timescale: cannot count %s: %w", res.Table, cerr)
	}

	direction := "ASC"
	if opts.Descending {
		direction = "DESC"
	}
	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)

	sql := fmt.Sprintf("SELECT * FROM %s %s ORDER BY %s %s LIMIT ? OFFSET ?",
		d.table(res), where, quote(timeCol), direction)
	rowArgs := append(append([]any{}, args...), limit, offset)

	var rows []map[string]any
	if serr := d.db.WithContext(ctx).Raw(sql, rowArgs...).Scan(&rows).Error; serr != nil {
		return database.ListResult{}, fmt.Errorf("timescale: cannot read %s: %w", res.Table, serr)
	}

	items, err := core.RowsToMessages(res, rows)
	if err != nil {
		return database.ListResult{}, err
	}
	return database.ListResult{
		Items:         items,
		NextPageToken: core.EncodeToken(offset, int64(len(rows)), total),
		Total:         total,
	}, nil
}

// Aggregate reduces a window into fixed-width buckets.
//
// The reduction runs in the database. That is the whole point of the method: a
// caller without it pulls every row across the wire to average them in Go, which
// is what a time-series store exists to stop.
func (d *Driver) Aggregate(ctx context.Context, res *database.Resource, opts database.AggregateOptions) ([]database.Bucket, error) {
	if res == nil {
		return nil, fmt.Errorf("timescale: Aggregate needs a resource")
	}
	if opts.Every <= 0 {
		return nil, fmt.Errorf("timescale: Aggregate needs a bucket width; without one there is one bucket, which Count already answers")
	}
	if len(opts.Reduce) == 0 {
		return nil, fmt.Errorf("timescale: Aggregate needs at least one reduction")
	}
	timeCol, err := d.timeColumn(res, opts.TimeColumn)
	if err != nil {
		return nil, err
	}

	selects := []string{fmt.Sprintf("time_bucket(?::interval, %s) AS bucket", quote(timeCol))}
	args := make([]any, 0, 2+len(opts.Reduce))
	args = append(args, interval(opts.Every))

	groups, err := columnNames(res, opts.GroupBy)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		selects = append(selects, quote(g))
	}

	names := make([]string, 0, len(opts.Reduce))
	for _, r := range opts.Reduce {
		expr, alias, rerr := reduction(res, r)
		if rerr != nil {
			return nil, rerr
		}
		selects = append(selects, expr+" AS "+quote(alias))
		names = append(names, alias)
	}

	where, whereArgs, err := buildWhere(res, opts.Filter, timeCol, opts.Start, opts.End)
	if err != nil {
		return nil, err
	}
	args = append(args, whereArgs...)

	groupBy := []string{"bucket"}
	for _, g := range groups {
		groupBy = append(groupBy, quote(g))
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultBucketLimit
	}
	sql := fmt.Sprintf("SELECT %s FROM %s %s GROUP BY %s ORDER BY bucket ASC LIMIT ?",
		strings.Join(selects, ", "), d.table(res), where, strings.Join(groupBy, ", "))
	args = append(args, limit)

	var rows []map[string]any
	if serr := d.db.WithContext(ctx).Raw(sql, args...).Scan(&rows).Error; serr != nil {
		return nil, fmt.Errorf("timescale: cannot aggregate %s: %w", res.Table, serr)
	}

	out := make([]database.Bucket, 0, len(rows))
	for _, row := range rows {
		b := database.Bucket{Values: make(map[string]float64, len(names))}
		if t, ok := row["bucket"].(time.Time); ok {
			b.Start = t.UTC()
		}
		if len(groups) > 0 {
			b.Group = make(map[string]string, len(groups))
			for _, g := range groups {
				b.Group[g] = fmt.Sprint(row[g])
			}
		}
		for _, name := range names {
			b.Values[name] = toFloat(row[name])
		}
		out = append(out, b)
	}
	return out, nil
}

// timeColumn resolves and validates the column a window applies to.
func (d *Driver) timeColumn(res *database.Resource, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("timescale: a time column is required; there is no sensible default")
	}
	col, ok := res.LookupColumn(name)
	if !ok {
		return "", fmt.Errorf("timescale: resource %q has no column %q", res.Name, name)
	}
	return col.Name, nil
}

// table returns the schema-qualified table a resource lives in.
func (d *Driver) table(res *database.Resource) string {
	schema := d.schema
	if schema == "" {
		schema = res.Schema
	}
	if schema != "" {
		return quote(schema) + "." + quote(res.Table)
	}
	return quote(res.Table)
}

// interval renders a Go duration as a PostgreSQL interval literal.
func interval(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Microsecond), 10) + " microseconds"
}

// quote wraps an identifier, doubling any quote inside it.
//
// Identifiers here come from a descriptor rather than a request, and are checked
// against it before reaching this — so this is the second line rather than the
// first. A descriptor is still data, and data that reaches an identifier
// position gets quoted.
func quote(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// toFloat narrows whatever the driver returned for a reduction.
func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int:
		return float64(x)
	case []byte:
		f, _ := strconv.ParseFloat(string(x), 64)
		return f
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

// checkUniqueIndexes reports a unique constraint that would stop the table being
// partitioned.
//
// TimescaleDB requires every unique index to contain the partitioning column,
// because uniqueness across chunks cannot be enforced without it — each chunk is
// its own table and an index on one knows nothing about the others. A resource
// whose primary key is an id and whose time column is something else violates
// that, which is the ordinary shape of a descriptor written for any other
// backend.
//
// The server refuses with TS103 and a message about creating an index, which
// reads as an implementation detail. This catches it first and says what the
// rule is and what the two ways out are, because a caller hitting this has a
// design decision to make rather than a bug to fix.
func (d *Driver) checkUniqueIndexes(ctx context.Context, res *database.Resource, timeColumn string) error {
	type indexRow struct {
		Name string
		Cols string
	}
	var rows []indexRow
	const q = `SELECT ix.relname AS name, string_agg(a.attname, ',') AS cols
		FROM pg_index i
		JOIN pg_class ix ON ix.oid = i.indexrelid
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = ?::regclass AND i.indisunique
		GROUP BY ix.relname`
	if err := d.db.WithContext(ctx).Raw(q, d.table(res)).Scan(&rows).Error; err != nil {
		return fmt.Errorf("timescale: cannot inspect the indexes on %s: %w", d.table(res), err)
	}

	for _, row := range rows {
		if slices.Contains(strings.Split(row.Cols, ","), timeColumn) {
			continue
		}
		return fmt.Errorf(
			"timescale: %s cannot be partitioned by %q while index %q is unique over (%s): "+
				"TimescaleDB requires every unique index to include the partitioning column, "+
				"because each chunk is its own table and an index on one cannot see the others. "+
				"Either make %q the resource's primary key, or describe the id column without "+
				"PrimaryKey so the table carries no unique constraint and the id stays the lookup key",
			d.table(res), timeColumn, row.Name, row.Cols, timeColumn)
	}
	return nil
}
