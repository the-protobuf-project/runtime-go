package events

import (
	"context"
	"crypto/md5" //nolint:gosec // the shard function of the outbox contract, not a security use
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// postgres.go claims and writes the <table>_outbox tables protoc-gen-store
// generates, over database/sql with any PostgreSQL driver. Every statement
// here is part of the outbox contract runtime-rs ports byte for byte; see
// docs/design/outbox-and-streams.md.

// lockNamespace starts every outbox lock key, so the keys cannot meet another
// program's advisory locks by accident.
const lockNamespace = "the-protobuf-project/events/outbox"

// identPart is one part of a table name, quoted as it stands.
var identPart = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// quoteTable quotes "schema.table" (or "table") as "schema"."table", refusing
// anything but plain identifiers: the name is spliced into SQL.
func quoteTable(table string) (string, error) {
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("events: outbox table %q is not schema.table", table)
	}
	for i, p := range parts {
		if !identPart.MatchString(p) {
			return "", fmt.Errorf("events: outbox table %q: %q is not a plain identifier", table, p)
		}
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, "."), nil
}

// LockKey is the advisory lock a server holds while it drains shard (of
// shards) of table: the first 8 bytes, big-endian, of SHA-256 over the lock
// namespace, the table as given, the shard and the shard count, each followed
// by a zero byte but the last.
func LockKey(table string, shard, shards int) int64 {
	sum := sha256.Sum256([]byte(lockNamespace + "\x00" + table + "\x00" +
		strconv.Itoa(shard) + "\x00" + strconv.Itoa(shards)))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// ShardOf is the shard (of shards) an aggregate's rows are in: the first 4
// bytes of MD5 over its id, big-endian, top bit cleared, modulo shards. It is
// what the claim's SQL computes, so a test can say which shard a row is in.
func ShardOf(aggregateID string, shards int) int {
	if shards <= 1 {
		return 0
	}
	sum := md5.Sum([]byte(aggregateID)) //nolint:gosec // see the import
	return int(binary.BigEndian.Uint32(sum[:4])&0x7fffffff) % shards
}

// PostgresOutbox is the OutboxClaimer for an outbox table protoc-gen-store
// generated, e.g. "shop.orders_outbox", over db.
func PostgresOutbox(db *sql.DB, table string) (OutboxClaimer, error) {
	quoted, err := quoteTable(table)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.New("events: PostgresOutbox needs a database")
	}
	return &pgOutbox{db: db, table: table, quoted: quoted}, nil
}

type pgOutbox struct {
	db     *sql.DB
	table  string
	quoted string
}

// Claim takes the shard's advisory lock, transaction-scoped, then locks its
// oldest outstanding rows. The claim's transaction is bounded by ctx's
// deadline twice over: statement_timeout for a statement that hangs, and
// idle_in_transaction_session_timeout for a server that stalls between them,
// which ends the session and so frees the lock.
func (o *pgOutbox) Claim(ctx context.Context, shard, shards, limit int, upTo time.Time) (OutboxClaim, bool, error) {
	tx, err := o.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, false, err
	}
	if dl, ok := ctx.Deadline(); ok {
		ms := max(time.Until(dl).Milliseconds(), 1)
		for _, setting := range []string{"statement_timeout", "idle_in_transaction_session_timeout"} {
			if _, serr := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL %s = '%dms'", setting, ms)); serr != nil {
				_ = tx.Rollback()
				return nil, false, serr
			}
		}
	}
	var locked bool
	if lerr := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", LockKey(o.table, shard, shards)).Scan(&locked); lerr != nil {
		_ = tx.Rollback()
		return nil, false, lerr
	}
	if !locked {
		_ = tx.Rollback()
		return nil, false, nil
	}
	query, args := o.claimQuery(shard, shards, limit, upTo)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	c := &pgClaim{tx: tx, quoted: o.quoted}
	for rows.Next() {
		var (
			r       OutboxRow
			payload string
		)
		if err := rows.Scan(&r.ID, &r.AggregateID, &r.EventType, &payload, &r.CreateTime); err != nil {
			_ = tx.Rollback()
			return nil, false, err
		}
		r.ID = strings.TrimRight(r.ID, " ") // CHAR(26) pads a shorter id
		r.Payload = []byte(payload)
		c.rows = append(c.rows, r)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	return c, true, nil
}

// claimQuery is the claim's SELECT and its arguments.
func (o *pgOutbox) claimQuery(shard, shards, limit int, upTo time.Time) (string, []any) {
	q := "SELECT id, aggregate_id::text, event_type, payload::text, created_at FROM " + o.quoted +
		" WHERE published_at IS NULL AND created_at <= $1"
	args := []any{upTo, limit}
	if shards > 1 {
		q += " AND (('x' || substr(md5(aggregate_id::text), 1, 8))::bit(32)::int & 2147483647) % " +
			strconv.Itoa(shards) + " = $3"
		args = append(args, shard)
	}
	return q + " ORDER BY created_at, id LIMIT $2 FOR UPDATE", args
}

// Prune deletes up to limit rows published before before, oldest first.
func (o *pgOutbox) Prune(ctx context.Context, before time.Time, limit int) error {
	_, err := o.db.ExecContext(ctx, "DELETE FROM "+o.quoted+" WHERE id IN (SELECT id FROM "+o.quoted+ //nolint:gosec // the table is a validated, quoted identifier
		" WHERE published_at < $1 ORDER BY published_at LIMIT $2)", before, limit)
	return err
}

type pgClaim struct {
	tx     *sql.Tx
	quoted string
	rows   []OutboxRow
}

func (c *pgClaim) Rows() []OutboxRow { return c.rows }

// Commit marks the rows published, in the claim's transaction, and commits it.
func (c *pgClaim) Commit(ctx context.Context, published []string) error {
	if len(published) > 0 {
		if _, err := c.tx.ExecContext(ctx, "UPDATE "+c.quoted+ //nolint:gosec // the table is a validated, quoted identifier
			" SET published_at = clock_timestamp() WHERE id = ANY(string_to_array($1, ',')::char(26)[])",
			strings.Join(published, ",")); err != nil {
			_ = c.tx.Rollback()
			return err
		}
	}
	return c.tx.Commit()
}

func (c *pgClaim) Release() error {
	err := c.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}
