package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// outbox.go drains the <table>_outbox tables protoc-gen-store generates.
//
// An outbox row is written in the same transaction as the change it describes,
// so it exists exactly when the change committed; it never changes after, and
// published_at says whether it was sent. None of a feed's machinery -- a time
// cursor, the Lag reread, the seen-set, refetching by etag -- applies to it:
// a row is outstanding until it is marked, whatever the clocks say.
//
// A round claims each shard of each outbox (one server at a time per shard),
// renders every claimed row once as a CloudEvent, records it for the webhooks
// that want it, hands the batch to the Sink, and marks published what every
// one of them has. A row the sink did not take stays outstanding, and so does
// everything after it with the same aggregate, so an aggregate's events go out
// in the order they were written.

// OutboxRow is one row of a <table>_outbox.
type OutboxRow struct {
	// ID is the row's ULID, and the event's id everywhere it goes.
	ID string
	// AggregateID is the owner's primary key, as text.
	AggregateID string
	// EventType is the CloudEvent type, e.g. "shop.order.created".
	EventType string
	// Payload is the event body, JSON.
	Payload []byte
	// CreateTime is when the row was written.
	CreateTime time.Time
}

// OutboxClaimer claims outstanding rows of one outbox table. [PostgresOutbox]
// is the implementation for the tables protoc-gen-store generates.
type OutboxClaimer interface {
	// Claim locks shard (of shards) and up to limit of its outstanding rows
	// created at or before upTo, oldest first. ok is false when another server
	// holds the shard; nothing is locked then.
	Claim(ctx context.Context, shard, shards, limit int, upTo time.Time) (claim OutboxClaim, ok bool, err error)
	// Prune deletes up to limit rows published before before.
	Prune(ctx context.Context, before time.Time, limit int) error
}

// OutboxClaim is one claimed batch, open until Commit or Release.
type OutboxClaim interface {
	// Rows are the claimed rows, oldest first.
	Rows() []OutboxRow
	// Commit marks the named rows published and ends the claim.
	Commit(ctx context.Context, published []string) error
	// Release ends the claim without marking anything.
	Release() error
}

// OutboxSpec describes how one outbox table is drained. The zero value of each
// field is its default.
type OutboxSpec struct {
	// Name labels the outbox's metrics and errors, e.g. "shop.orders".
	Name string
	// Subject is an event's CloudEvent subject. Default: the payload's
	// top-level "name" string (an AIP resource name), else the aggregate id.
	Subject func(r OutboxRow) string
	// Scope is an event's scope for webhooks ([Webhook.Scope]). Default "".
	Scope func(r OutboxRow, subject string) string
	// Shards splits the table by aggregate so several servers drain it at
	// once, each aggregate always in one shard. Default 1.
	Shards int
	// BatchSize bounds one claim. Default 500.
	BatchSize int
	// ClaimTimeout bounds one claim, its publishing included. Default 30
	// seconds.
	ClaimTimeout time.Duration
	// Retention is how long published rows are kept. Default 7 days; negative
	// keeps them.
	Retention time.Duration
	// MetricPrefix names the metrics: <prefix>.attempts, <prefix>.delivery_lag
	// and <prefix>.claims. Default "outbox".
	MetricPrefix string
}

// Outbox is one outbox table the dispatcher drains. Build one with NewOutbox.
type Outbox struct {
	c    OutboxClaimer
	spec OutboxSpec
}

// NewOutbox drains c as spec describes.
func NewOutbox(c OutboxClaimer, spec OutboxSpec) *Outbox {
	if spec.Subject == nil {
		spec.Subject = DefaultSubject
	}
	if spec.Scope == nil {
		spec.Scope = func(OutboxRow, string) string { return "" }
	}
	if spec.Shards <= 0 {
		spec.Shards = 1
	}
	if spec.BatchSize <= 0 {
		spec.BatchSize = 500
	}
	if spec.ClaimTimeout <= 0 {
		spec.ClaimTimeout = 30 * time.Second
	}
	if spec.Retention == 0 {
		spec.Retention = 7 * 24 * time.Hour
	}
	if spec.MetricPrefix == "" {
		spec.MetricPrefix = "outbox"
	}
	return &Outbox{c: c, spec: spec}
}

// Name is the outbox's name.
func (o *Outbox) Name() string { return o.spec.Name }

// DefaultSubject is an outbox event's subject when OutboxSpec.Subject is not
// set: the payload's top-level "name" when it is a JSON string -- the AIP
// resource name of a protojson-rendered resource -- else the aggregate id.
func DefaultSubject(r OutboxRow) string {
	var top map[string]json.RawMessage
	if json.Unmarshal(r.Payload, &top) == nil {
		var name string
		if raw, ok := top["name"]; ok && json.Unmarshal(raw, &name) == nil && name != "" {
			return name
		}
	}
	return r.AggregateID
}

// OutboxBackoff is how long a shard waits after its kth consecutive failed
// round: 1 second, doubling, at most a minute. There is no last attempt: an
// outbox event is never given up on.
func OutboxBackoff(k int) time.Duration {
	wait := time.Second
	for i := 1; i < k && wait < time.Minute; i++ {
		wait *= 2
	}
	return min(wait, time.Minute)
}

// Outcomes a claim of a shard can have, the claims counter's "result" label.
const (
	ClaimClaimed   = "claimed"
	ClaimContended = "contended"
	ClaimEmpty     = "empty"
)

// outboxMetrics is what draining reports: every row by how its round ended
// (<prefix>.attempts{outcome}), how long after it was written each published
// row was (<prefix>.delivery_lag), and every claim of a shard
// (<prefix>.claims{result}).
type outboxMetrics struct {
	attempts telemetry.Counter
	lag      telemetry.Histogram
	claims   telemetry.Counter
	name     string
}

func newOutboxMetrics(m telemetry.Meter, prefix, name string) outboxMetrics {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return outboxMetrics{
		attempts: m.Counter(prefix+".attempts", telemetry.WithUnit("1"),
			telemetry.WithDescription("Outbox rows a round handled, by outcome: succeeded (published), retrying (left outstanding) or failed (undeliverable).")),
		lag: m.Histogram(prefix+".delivery_lag", telemetry.WithUnit("s"),
			telemetry.WithDescription("From an outbox row being written to its being marked published."),
			telemetry.WithBuckets(0.5, 1, 2, 5, 10, 30, 60, 300, 1800, 3600, 21600, 86400)),
		claims: m.Counter(prefix+".claims", telemetry.WithUnit("1"),
			telemetry.WithDescription("Claims of an outbox shard, by result: claimed, contended (another server holds it) or empty.")),
		name: name,
	}
}

func (m outboxMetrics) attempt(ctx context.Context, outcome string, n int) {
	if n > 0 {
		m.attempts.Add(ctx, float64(n), telemetry.Labels{"outcome": outcome, "outbox": m.name})
	}
}

func (m outboxMetrics) claim(ctx context.Context, result string) {
	m.claims.Add(ctx, 1, telemetry.Labels{"result": result, "outbox": m.name})
}

func (m outboxMetrics) published(ctx context.Context, lag time.Duration) {
	m.lag.Record(ctx, lag.Seconds(), telemetry.Labels{"outbox": m.name})
}

// shardRetry is a shard's run of failed rounds, and when it may next be tried.
type shardRetry struct {
	failures int
	next     time.Time
}

// drain drains every shard of every outbox that is not backing off.
func (d *Dispatcher) drain(ctx context.Context, hooks []Webhook, upTo time.Time) error {
	var errs []error
	for _, o := range d.opts.Outboxes {
		for shard := range o.spec.Shards {
			key := o.spec.Name + "/" + strconv.Itoa(shard)
			if d.backingOff(key) {
				continue
			}
			err := d.drainShard(ctx, o, shard, hooks, upTo)
			d.retried(key, err)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s shard %d: %w", o.spec.Name, shard, err))
			}
		}
	}
	return errors.Join(errs...)
}

// drainShard claims one shard and sends what it holds.
func (d *Dispatcher) drainShard(ctx context.Context, o *Outbox, shard int, hooks []Webhook, upTo time.Time) error {
	m := d.outboxMetrics[o]
	ctx, cancel := context.WithTimeout(ctx, o.spec.ClaimTimeout)
	defer cancel()
	claim, ok, err := o.c.Claim(ctx, shard, o.spec.Shards, o.spec.BatchSize, upTo)
	switch {
	case err != nil:
		return err
	case !ok:
		m.claim(ctx, ClaimContended)
		return nil
	}
	rows := claim.Rows()
	if len(rows) == 0 {
		m.claim(ctx, ClaimEmpty)
		return claim.Release()
	}
	m.claim(ctx, ClaimClaimed)

	batch, err := d.render(o, rows)
	if err != nil {
		_ = claim.Release()
		return err
	}
	for i := range batch {
		if err := d.recordEvent(ctx, &batch[i], hooks); err != nil {
			_ = claim.Release()
			return err
		}
	}
	sent, sendErr := d.deliver(ctx, batch)

	published := make([]string, 0, len(rows))
	for i := range rows {
		if sent[i] {
			published = append(published, rows[i].ID)
		}
	}
	if len(published) == 0 {
		_ = claim.Release()
	} else if err := claim.Commit(ctx, published); err != nil {
		return errors.Join(sendErr, err)
	}
	now := d.opts.Now()
	for i := range rows {
		if sent[i] {
			m.published(ctx, now.Sub(rows[i].CreateTime))
		}
	}
	m.attempt(ctx, OutcomeSucceeded, len(published))
	outcome := OutcomeRetrying
	if errors.Is(sendErr, ErrUndeliverable) {
		outcome = OutcomeFailed
	}
	m.attempt(ctx, outcome, len(rows)-len(published))
	return sendErr
}

// render turns claimed rows into events, each rendered once: the body a
// webhook is POSTed is the body the sink publishes.
func (d *Dispatcher) render(o *Outbox, rows []OutboxRow) ([]Event, error) {
	out := make([]Event, len(rows))
	for i, r := range rows {
		subject := o.spec.Subject(r)
		body, err := CloudEvent(r.ID, d.opts.Source, r.EventType, subject, r.CreateTime, r.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: row %s: %w", ErrUndeliverable, r.ID, err)
		}
		out[i] = Event{
			ID: r.ID, Type: r.EventType, Subject: subject, Time: r.CreateTime,
			Scope: o.spec.Scope(r, subject), PartitionKey: r.AggregateID, Body: []byte(body),
		}
	}
	return out, nil
}

// recordEvent writes e as a PENDING delivery for every webhook that wants it.
// A delivery recorded already -- by another server, or by this one before a
// claim it could not commit -- collides on its derived name and is left as it
// is, so a republished event is never POSTed twice.
func (d *Dispatcher) recordEvent(ctx context.Context, e *Event, hooks []Webhook) error {
	c := &Change{Name: e.Subject, Type: e.Type, Time: e.Time, Scope: e.Scope}
	for i := range hooks {
		w := &hooks[i]
		if !Wants(w, c) {
			continue
		}
		err := d.store.CreateDelivery(ctx, Delivery{
			Name:            DeliveryName(w.Name, e.ID),
			Webhook:         w.Name,
			EventID:         e.ID,
			EventType:       e.Type,
			Subject:         e.Subject,
			Payload:         string(e.Body),
			State:           DeliveryPending,
			NextAttemptTime: d.opts.Now(),
		})
		if err != nil && !errors.Is(err, ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// deliver hands batch to the sink; with no sink, webhooks are the only place
// an event goes, and recording it was the hand-off.
func (d *Dispatcher) deliver(ctx context.Context, batch []Event) ([]bool, error) {
	if d.opts.Sink == nil {
		sent := make([]bool, len(batch))
		for i := range sent {
			sent[i] = true
		}
		return sent, nil
	}
	sent, err := d.opts.Sink.Deliver(ctx, batch)
	if len(sent) != len(batch) {
		return make([]bool, len(batch)), errors.Join(err,
			fmt.Errorf("events: the sink reported %d results for %d events", len(sent), len(batch)))
	}
	return sent, err
}

// backingOff reports whether the shard under key failed lately and is not due
// again yet.
func (d *Dispatcher) backingOff(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, ok := d.shardRetries[key]
	return ok && d.opts.Now().Before(r.next)
}

// retried records how a shard's round went: a failure pushes its next try out
// by OutboxBackoff, a success clears its run.
func (d *Dispatcher) retried(key string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err == nil {
		delete(d.shardRetries, key)
		return
	}
	r := d.shardRetries[key]
	r.failures++
	r.next = d.opts.Now().Add(OutboxBackoff(r.failures))
	d.shardRetries[key] = r
}

// pruneOutboxes deletes published rows past each outbox's retention.
func (d *Dispatcher) pruneOutboxes(ctx context.Context) error {
	for _, o := range d.opts.Outboxes {
		if o.spec.Retention < 0 {
			continue
		}
		if err := o.c.Prune(ctx, d.opts.Now().Add(-o.spec.Retention), prunePage); err != nil {
			return fmt.Errorf("%s: %w", o.spec.Name, err)
		}
	}
	return nil
}
