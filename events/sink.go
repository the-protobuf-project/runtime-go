package events

import (
	"context"
	"errors"
	"time"
)

// ErrUndeliverable marks a failure no retry can fix: a broker that refuses the
// event outright, a subject the stream does not declare. A Sink wraps such a
// failure in it so the dispatcher stalls the event loudly rather than retry it
// as if the broker were only down.
var ErrUndeliverable = errors.New("events: undeliverable")

// Event is one change ready for a sink: the rendered CloudEvent and what a
// broker needs to route it.
type Event struct {
	// ID is the CloudEvent id: an outbox row's id, or EventID(name, etag) for
	// a change a feed read.
	ID string
	// Type and Subject are the CloudEvent's type and subject (the resource's
	// name).
	Type    string
	Subject string
	// Time is the CloudEvent's time.
	Time time.Time
	// Scope is what the change belongs to, as [Change.Scope].
	Scope string
	// PartitionKey orders the event with every other carrying the same key:
	// the aggregate's id from an outbox, the record's name from a feed.
	PartitionKey string
	// Body is the CloudEvent as structured-mode JSON, the same bytes a webhook
	// is POSTed.
	Body []byte
}

// Sink is where the dispatcher sends events besides webhooks: a broker, through
// runtime-go's events/stream, or anything else that acknowledges what it
// accepted.
type Sink interface {
	// Deliver sends batch, in order per partition key, and reports which
	// entries the sink accepted: sent[i] for batch[i]. An entry after a failed
	// one of the same partition key is not attempted, so a key's events never
	// overtake each other. err joins the failures; one no retry can fix wraps
	// [ErrUndeliverable].
	Deliver(ctx context.Context, batch []Event) (sent []bool, err error)
}
