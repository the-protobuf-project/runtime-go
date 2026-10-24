package events

import (
	"context"
	"errors"
	"strings"
	"time"
)

// Errors a [Store] reports, compared with errors.Is. An implementation wraps
// its backend's own errors in these (fmt.Errorf("%w: %w", ErrNotFound, err)).
var (
	// ErrNotFound: the named webhook or cursor does not exist.
	ErrNotFound = errors.New("events: not found")
	// ErrAlreadyExists: a delivery or cursor with that name exists already.
	ErrAlreadyExists = errors.New("events: already exists")
	// ErrConflict: the record's etag no longer matches; another server wrote
	// it first.
	ErrConflict = errors.New("events: conflict")
)

// Webhook is an endpoint subscribed to changes.
type Webhook struct {
	// Name identifies the webhook, e.g. "webhooks/w". Its deliveries are
	// named under it.
	Name string
	// URI is the endpoint POSTed to.
	URI string
	// EventTypes are the types it is sent; empty is every type.
	EventTypes []string
	// Scope limits it to the changes of one scope (see [Change.Scope]); empty
	// is every scope.
	Scope string
	// Paused webhooks are sent nothing; their deliveries wait.
	Paused bool
	// Secret is the signing secret as stored: sealed by a [Sealer], or in the
	// clear.
	Secret string
	// PreviousSecret is the secret Secret replaced at RotateTime; it signs
	// too, for [Options.RotationGrace].
	PreviousSecret string
	RotateTime     time.Time
	// CreateTime: a webhook is sent only the changes after it.
	CreateTime time.Time
}

// DeliveryState is where a delivery stands.
type DeliveryState int

const (
	// DeliveryPending is waiting for its next attempt.
	DeliveryPending DeliveryState = iota + 1
	// DeliverySucceeded was accepted by its endpoint.
	DeliverySucceeded
	// DeliveryFailed ran out of attempts, or its webhook was deleted.
	DeliveryFailed
)

func (s DeliveryState) String() string {
	switch s {
	case DeliveryPending:
		return "PENDING"
	case DeliverySucceeded:
		return "SUCCEEDED"
	case DeliveryFailed:
		return "FAILED"
	}
	return "UNSPECIFIED"
}

// Delivery is one change recorded for one webhook.
type Delivery struct {
	// Name is "<webhook>/deliveries/<id>", the id derived from the webhook and
	// the change (see [DeliveryName]).
	Name string
	// Webhook is the webhook it belongs to: Name's parent.
	Webhook string
	// EventID, EventType and Subject are the CloudEvent's id, type and
	// subject (the record's name).
	EventID   string
	EventType string
	Subject   string
	// Payload is the request body: a CloudEvents 1.0 structured-mode JSON
	// document.
	Payload string
	State   DeliveryState
	// AttemptCount, ResponseCode and ErrorMessage describe the attempts so
	// far: how many, the last HTTP status (0 for none), and the last failure.
	AttemptCount int32
	ResponseCode int32
	ErrorMessage string
	// NextAttemptTime is when a PENDING delivery is next due; zero otherwise.
	NextAttemptTime time.Time
	// DeliverTime is when the endpoint accepted it.
	DeliverTime time.Time
	// CreateTime is set by the store; it measures delivery lag.
	CreateTime time.Time
	// Etag guards UpdateDelivery.
	Etag string
}

// The paths UpdateDelivery is asked to write.
const (
	PathState           = "state"
	PathAttemptCount    = "attempt_count"
	PathResponseCode    = "response_code"
	PathErrorMessage    = "error_message"
	PathNextAttemptTime = "next_attempt_time"
	PathDeliverTime     = "deliver_time"
)

// Cursor is how far a feed has been read.
type Cursor struct {
	// Feed is the feed's id.
	Feed string
	// Position is the latest update time read.
	Position time.Time
	// Etag guards UpdateCursor.
	Etag string
}

// Store keeps webhooks, deliveries and cursors. Methods report the errors
// above where they say so; any other error ends the round.
type Store interface {
	// ListWebhooks lists a page of every webhook, paused ones included.
	ListWebhooks(ctx context.Context, pageToken string) (webhooks []Webhook, next string, err error)
	// GetWebhook reads one webhook: ErrNotFound when it was deleted.
	GetWebhook(ctx context.Context, name string) (Webhook, error)

	// GetCursor reads a feed's cursor: ErrNotFound for a feed never read.
	GetCursor(ctx context.Context, feed string) (Cursor, error)
	// CreateCursor starts a feed's cursor: ErrAlreadyExists when another
	// server started it first.
	CreateCursor(ctx context.Context, c Cursor) (Cursor, error)
	// UpdateCursor moves a cursor to c.Position if its etag is still c.Etag:
	// ErrConflict otherwise.
	UpdateCursor(ctx context.Context, c Cursor) error

	// CreateDelivery records a delivery: ErrAlreadyExists when one of that
	// name exists, which the dispatcher ignores.
	CreateDelivery(ctx context.Context, d Delivery) error
	// DueDeliveries lists at most limit PENDING deliveries whose next attempt
	// is at or before now, earliest first, across every webhook.
	DueDeliveries(ctx context.Context, now time.Time, limit int) ([]Delivery, error)
	// UpdateDelivery writes the named paths of d if its etag is still d.Etag,
	// and returns it as stored (with its new etag): ErrConflict otherwise.
	UpdateDelivery(ctx context.Context, d Delivery, paths []string) (Delivery, error)
	// PruneDeliveries deletes at most limit SUCCEEDED or FAILED deliveries
	// last changed before before.
	PruneDeliveries(ctx context.Context, before time.Time, limit int) error
}

// WebhookOf is the webhook a delivery name belongs to:
// "webhooks/w/deliveries/d" -> "webhooks/w".
func WebhookOf(delivery string) string {
	if i := strings.Index(delivery, "/deliveries/"); i >= 0 {
		return delivery[:i]
	}
	return delivery
}
