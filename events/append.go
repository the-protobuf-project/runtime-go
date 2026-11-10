package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/ulid"
)

// Execer is what AppendOutbox writes through: *sql.Tx, *sql.Conn, *sql.DB, or
// gorm's tx.Statement.ConnPool inside a gorm transaction.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// OutboxEntry is one event to append to an outbox.
type OutboxEntry struct {
	// ID is the event's id. Empty mints a ULID, which is what the table's id
	// column holds.
	ID string
	// AggregateID is the owner's primary key, typed as its column is.
	AggregateID any
	// Type is the CloudEvent type, e.g. "shop.order.created".
	Type string
	// Payload is the event body, JSON: usually the resource as protojson
	// renders it, so the event's subject is its "name".
	Payload []byte
}

// AppendOutbox inserts e into table inside the caller's transaction and
// returns its id. A rolled-back transaction takes the event with it, and a
// committed one guarantees it is sent.
//
// Call it after writing the aggregate's own row, in the same transaction: the
// row's created_at is the time of this insert, and writers of one aggregate
// are serialized by that row's lock, so an aggregate's events are drained in
// the order they committed.
//
//	err := db.Transaction(func(tx *gorm.DB) error {
//	    if err := tx.Create(&order).Error; err != nil {
//	        return err
//	    }
//	    _, err := events.AppendOutbox(ctx, tx.Statement.ConnPool, "shop.orders_outbox", events.OutboxEntry{
//	        AggregateID: order.ID, Type: "shop.order.created", Payload: payload,
//	    })
//	    return err
//	})
func AppendOutbox(ctx context.Context, tx Execer, table string, e OutboxEntry) (string, error) {
	quoted, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	switch {
	case tx == nil:
		return "", errors.New("events: AppendOutbox needs the writer's transaction")
	case e.Type == "":
		return "", errors.New("events: an outbox entry needs a type")
	case e.AggregateID == nil:
		return "", errors.New("events: an outbox entry needs its aggregate's id")
	case !json.Valid(e.Payload):
		return "", fmt.Errorf("events: the payload of a %s entry is not JSON", e.Type)
	}
	if e.ID == "" {
		e.ID = ulid.Generate().String()
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+quoted+
		" (id, aggregate_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4::jsonb, clock_timestamp())",
		e.ID, e.AggregateID, e.Type, string(e.Payload))
	if err != nil {
		return "", err
	}
	return e.ID, nil
}
