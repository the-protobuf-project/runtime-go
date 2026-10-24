package events

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

const (
	// feedPage and sendBatch bound one read of a feed and of due deliveries.
	feedPage  = 500
	sendBatch = 100
	// sendParallel bounds concurrent POSTs, so one slow endpoint does not
	// hold up the rest.
	sendParallel = 8
	// claimFor is how long a claimed delivery is left to its sender before
	// another server may attempt it: longer than any one attempt takes.
	claimFor = time.Minute
	// prunePage bounds the finished deliveries one round deletes.
	prunePage = 500
)

// Options tune a Dispatcher. The zero value of each field is its default.
type Options struct {
	// Source is the CloudEvents source of every event, e.g.
	// "//shop.example.com".
	Source string
	// UserAgent is sent with every delivery. Default "runtime-go-events/1".
	UserAgent string
	// MetricPrefix names the metrics: <prefix>.attempts and
	// <prefix>.delivery_lag. Default "webhook".
	MetricPrefix string
	// AllowPrivateNetworks lets the default client reach loopback, private
	// and link-local addresses. Ignored when Client is set.
	AllowPrivateNetworks bool
	// Retention is how long finished deliveries are kept. Default 30 days.
	Retention time.Duration
	// MaxAttempts before a delivery is FAILED. Default 8: with Backoff, about
	// a day and a half of trying.
	MaxAttempts int
	// Lag is how far back each round rereads, so a write that committed after
	// a later one was read is still seen. Default 30 seconds. Every round
	// rereads everything changed within Lag, so a longer one only slows
	// delivery after a burst of changes.
	Lag time.Duration
	// HookSettle is how far behind its read of the webhooks a round stops
	// taking changes: time for a webhook being created as the round starts to
	// commit. Default 1 second.
	HookSettle time.Duration
	// RotationGrace is how long a replaced secret goes on signing. Default
	// 24 hours.
	RotationGrace time.Duration
	// Client sends deliveries. Default NewClient(AllowPrivateNetworks).
	Client *http.Client
	// Now is the clock. Default time.Now.
	Now func() time.Time
	// Sealer opens sealed signing secrets. Nil reads them as they are.
	Sealer *Sealer
	// Meter receives the metrics. Nil drops them.
	Meter telemetry.Meter
}

// Dispatcher records every new change for the webhooks that want it, and
// sends every delivery that is due. Run it with Start, or call Round.
type Dispatcher struct {
	store Store
	feeds []Feed
	opts  Options

	// recorded remembers the changes this server has recorded lately, so a
	// round's reread of the last Lag does not write them all again.
	mu       sync.Mutex
	recorded map[string]time.Time
	// settled is, per feed, a cursor position everything at or before which
	// has been read after it could have committed (see dispatchFeed): rounds
	// at that position read only what is newer.
	settled map[string]time.Time

	metrics deliveryMetrics
}

// NewDispatcher returns a Dispatcher over store and feeds.
func NewDispatcher(store Store, feeds []Feed, opts Options) *Dispatcher {
	if opts.UserAgent == "" {
		opts.UserAgent = "runtime-go-events/1"
	}
	if opts.MetricPrefix == "" {
		opts.MetricPrefix = "webhook"
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 8
	}
	if opts.HookSettle <= 0 {
		opts.HookSettle = time.Second
	}
	if opts.Lag <= 0 {
		opts.Lag = 30 * time.Second
	}
	if opts.Retention <= 0 {
		opts.Retention = 30 * 24 * time.Hour
	}
	if opts.RotationGrace <= 0 {
		opts.RotationGrace = 24 * time.Hour
	}
	if opts.Client == nil {
		opts.Client = NewClient(opts.AllowPrivateNetworks)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Dispatcher{
		store: store, feeds: feeds, opts: opts,
		recorded: map[string]time.Time{}, settled: map[string]time.Time{},
		metrics: newDeliveryMetrics(opts.Meter, opts.MetricPrefix),
	}
}

// Start runs Round every interval until ctx ends, passing each failure to
// logErr.
func (d *Dispatcher) Start(ctx context.Context, interval time.Duration, logErr func(error)) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			if err := d.Round(ctx); err != nil && logErr != nil && ctx.Err() == nil {
				logErr(err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// Round records every new change for the webhooks that want it, sends every
// delivery that is due, and prunes old finished ones. Safe to run on several
// servers at once: a change recorded twice collides on its delivery's derived
// name, and a delivery is claimed before it is sent.
func (d *Dispatcher) Round(ctx context.Context) error {
	if err := d.dispatch(ctx); err != nil {
		return fmt.Errorf("webhooks: dispatch: %w", err)
	}
	if err := d.send(ctx); err != nil {
		return fmt.Errorf("webhooks: send: %w", err)
	}
	if err := d.store.PruneDeliveries(ctx, d.opts.Now().Add(-d.opts.Retention), prunePage); err != nil {
		return fmt.Errorf("webhooks: prune: %w", err)
	}
	return nil
}

// Wants reports whether w is sent c: it asks for c's type (or every type)
// and c's scope (or every scope), and existed when c happened.
func Wants(w *Webhook, c *Change) bool {
	if len(w.EventTypes) > 0 && !slices.Contains(w.EventTypes, c.Type) {
		return false
	}
	if w.Scope != "" && c.Scope != w.Scope {
		return false
	}
	return !c.Time.Before(w.CreateTime)
}

// dispatch reads each feed from its cursor, less Lag, and records what it
// finds for the active webhooks that want it.
func (d *Dispatcher) dispatch(ctx context.Context) error {
	// The webhooks are read once a round, so a change after that read could
	// meet a webhook the list does not hold yet, be recorded for nobody and
	// never be sent. The round takes only the changes up to the read, less
	// HookSettle for a webhook still committing; the next round takes the rest.
	upTo := d.opts.Now().Add(-d.opts.HookSettle)
	var hooks []Webhook
	for token := ""; ; {
		page, next, err := d.store.ListWebhooks(ctx, token)
		if err != nil {
			return err
		}
		for i := range page {
			if !page[i].Paused {
				hooks = append(hooks, page[i])
			}
		}
		if token = next; token == "" {
			break
		}
	}
	for _, f := range d.feeds {
		if err := d.dispatchFeed(ctx, f, hooks, upTo); err != nil {
			return fmt.Errorf("%s: %w", f.id, err)
		}
	}
	return nil
}

func wantedBy(hooks []Webhook, c *Change) bool {
	for i := range hooks {
		if Wants(&hooks[i], c) {
			return true
		}
	}
	return false
}

func (d *Dispatcher) dispatchFeed(ctx context.Context, f Feed, hooks []Webhook, upTo time.Time) error {
	cur, err := d.cursor(ctx, f.id)
	if err != nil {
		return err
	}
	pos := cur.Position
	latest := pos
	// Each round rereads the Lag before the cursor, for writes that commit
	// after a later one was read. Once a round has read up to Lag past the
	// cursor, everything at or before it has committed and been read, so
	// while the cursor stays put the rounds after read only what is newer --
	// an idle feed would otherwise reread the same Lag every round.
	since, inclusive := pos.Add(-d.opts.Lag), true
	if d.isSettled(f.id, pos) {
		since, inclusive = pos, false
	}
	for token := ""; ; {
		page, next, rerr := f.read(ctx, Query{
			PageSize: feedPage, PageToken: token, OrderByUpdateTime: true,
			Since: since, SinceInclusive: inclusive,
			// Later changes are the next round's (see dispatch).
			Until: upTo,
		})
		if rerr != nil {
			return rerr
		}
		var sent []Change
		for i := range page {
			c := &page[i]
			// A change sent to nobody still moves the cursor past it, or the
			// next round would read it again, and the one after.
			if c.Time.After(latest) {
				latest = c.Time
			}
			if c.Type == "" || d.seen(c) {
				continue
			}
			if !wantedBy(hooks, c) {
				d.remember(c) // recorded for nobody, as before
				continue
			}
			sent = append(sent, *c)
		}
		if rerr := d.recordAll(ctx, f, sent, hooks); rerr != nil {
			return rerr
		}
		if token = next; token == "" {
			break
		}
	}
	d.forget(latest.Add(-d.opts.Lag))
	if !latest.After(pos) {
		if !upTo.Before(pos.Add(d.opts.Lag)) {
			d.settle(f.id, pos)
		}
		return nil
	}
	cur.Position = latest
	err = d.store.UpdateCursor(ctx, cur)
	if errors.Is(err, ErrConflict) {
		return nil // another server moved it; its position is as good
	}
	return err
}

// cursor returns a feed's cursor, starting one at now: a feed nobody has read
// begins with the changes after its first read, not with all of history.
func (d *Dispatcher) cursor(ctx context.Context, id string) (Cursor, error) {
	cur, err := d.store.GetCursor(ctx, id)
	if !errors.Is(err, ErrNotFound) {
		return cur, err
	}
	cur, err = d.store.CreateCursor(ctx, Cursor{Feed: id, Position: d.opts.Now()})
	if errors.Is(err, ErrAlreadyExists) {
		return d.store.GetCursor(ctx, id)
	}
	return cur, err
}

// recordAll fetches the records of changes, a page of a feed's changes that
// some webhook wants, and records each, in order. A record that has moved on
// since the page was read (its etag differs) or is gone is not recorded: its
// version can no longer be read, and the write that replaced it is a change
// of its own, which a later page or round reads and sends.
func (d *Dispatcher) recordAll(ctx context.Context, f Feed, changes []Change, hooks []Webhook) error {
	if len(changes) == 0 {
		return nil
	}
	names := make([]string, 0, len(changes))
	for _, c := range changes {
		names = append(names, c.Name)
	}
	slices.Sort(names)
	records, err := f.fetch(ctx, slices.Compact(names))
	if err != nil {
		return err
	}
	for i := range changes {
		c := &changes[i]
		r, ok := records[c.Name]
		if !ok || r.etag != c.Etag {
			continue
		}
		if err := d.record(ctx, c, r, hooks); err != nil {
			return err
		}
	}
	return nil
}

// record writes c as a PENDING delivery for every webhook that wants it.
func (d *Dispatcher) record(ctx context.Context, c *Change, r fetched, hooks []Webhook) error {
	id := EventID(c.Name, c.Etag)
	var payload string
	for i := range hooks {
		w := &hooks[i]
		if !Wants(w, c) {
			continue
		}
		if payload == "" {
			data, err := r.data()
			if err != nil {
				return err
			}
			if payload, err = CloudEvent(id, d.opts.Source, c.Type, c.Name, c.Time, data); err != nil {
				return err
			}
		}
		err := d.store.CreateDelivery(ctx, Delivery{
			Name:            DeliveryName(w.Name, id),
			Webhook:         w.Name,
			EventID:         id,
			EventType:       c.Type,
			Subject:         c.Name,
			Payload:         payload,
			State:           DeliveryPending,
			NextAttemptTime: d.opts.Now(),
		})
		if err != nil && !errors.Is(err, ErrAlreadyExists) {
			return err
		}
	}
	d.remember(c)
	return nil
}

// remember marks c recorded, so a reread of it within Lag skips it.
func (d *Dispatcher) remember(c *Change) {
	d.mu.Lock()
	d.recorded[EventID(c.Name, c.Etag)] = c.Time
	d.mu.Unlock()
}

// isSettled reports whether feed's reads at cursor position pos may skip
// the Lag before it (see dispatchFeed).
func (d *Dispatcher) isSettled(feed string, pos time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	at, ok := d.settled[feed]
	return ok && at.Equal(pos)
}

// settle marks feed's cursor position pos settled.
func (d *Dispatcher) settle(feed string, pos time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.settled[feed] = pos
}

// seen reports whether this server recorded c already.
func (d *Dispatcher) seen(c *Change) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.recorded[EventID(c.Name, c.Etag)]
	return ok
}

// forget drops what was recorded before cutoff, which no reread reaches.
func (d *Dispatcher) forget(cutoff time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, at := range d.recorded {
		if at.Before(cutoff) {
			delete(d.recorded, id)
		}
	}
}

// send attempts every PENDING delivery that is due.
func (d *Dispatcher) send(ctx context.Context) error {
	due, err := d.store.DueDeliveries(ctx, d.opts.Now(), sendBatch)
	if err != nil || len(due) == 0 {
		return err
	}
	// A nil entry is a deleted webhook.
	hooks := map[string]*Webhook{}
	for i := range due {
		name := WebhookOf(due[i].Name)
		if _, ok := hooks[name]; ok {
			continue
		}
		w, err := d.store.GetWebhook(ctx, name)
		switch {
		case errors.Is(err, ErrNotFound):
			hooks[name] = nil
		case err != nil:
			return err
		default:
			hooks[name] = &w
		}
	}

	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, sendParallel)
		mu    sync.Mutex
		errs  []error
	)
	for i := range due {
		dl := &due[i]
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer func() { <-slots; wg.Done() }()
			if err := d.attempt(ctx, *dl, hooks[WebhookOf(dl.Name)]); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// attempt makes one attempt at dl. It claims dl first -- pushing its next
// attempt out, guarded by its etag -- so of several servers reading it due,
// one sends it. A nil w is a deleted webhook: the delivery fails.
func (d *Dispatcher) attempt(ctx context.Context, dl Delivery, w *Webhook) error {
	now := d.opts.Now()
	if w != nil && w.Paused {
		return nil // paused: it waits
	}
	dl.NextAttemptTime = now.Add(claimFor)
	dl, err := d.store.UpdateDelivery(ctx, dl, []string{PathNextAttemptTime})
	if errors.Is(err, ErrConflict) {
		return nil // another server claimed it
	}
	if err != nil {
		return err
	}

	code, sendErr := 0, errors.New("the webhook was deleted")
	if w != nil {
		code, sendErr = d.post(ctx, w, &dl)
	}
	dl.AttemptCount++
	dl.ResponseCode = int32(code)
	paths := []string{PathState, PathAttemptCount, PathResponseCode, PathErrorMessage, PathNextAttemptTime}
	switch {
	case sendErr == nil:
		dl.State, dl.ErrorMessage, dl.NextAttemptTime = DeliverySucceeded, "", time.Time{}
		dl.DeliverTime = d.opts.Now()
		paths = append(paths, PathDeliverTime)
		d.metrics.attempt(ctx, OutcomeSucceeded)
		if !dl.CreateTime.IsZero() {
			d.metrics.delivered(ctx, dl.DeliverTime.Sub(dl.CreateTime))
		}
	case w == nil || int(dl.AttemptCount) >= d.opts.MaxAttempts:
		dl.State, dl.ErrorMessage, dl.NextAttemptTime = DeliveryFailed, sendErr.Error(), time.Time{}
		d.metrics.attempt(ctx, OutcomeFailed)
	default:
		dl.State, dl.ErrorMessage = DeliveryPending, sendErr.Error()
		dl.NextAttemptTime = d.opts.Now().Add(Backoff(int(dl.AttemptCount)))
		d.metrics.attempt(ctx, OutcomeRetrying)
	}
	_, err = d.store.UpdateDelivery(ctx, dl, paths)
	return err
}

// Backoff is the wait after the nth failed attempt: 30 seconds, doubling, at
// most 6 hours.
func Backoff(n int) time.Duration {
	wait := 30 * time.Second
	for i := 1; i < n && wait < 6*time.Hour; i++ {
		wait *= 2
	}
	return min(wait, 6*time.Hour)
}

// post sends dl to w's endpoint, signed per the Standard Webhooks
// specification under the webhook's secret -- and, for RotationGrace after a
// rotation, a second signature under the secret it replaced, space-separated.
// Any 2xx is acceptance; a redirect is not.
func (d *Dispatcher) post(ctx context.Context, w *Webhook, dl *Delivery) (int, error) {
	body := []byte(dl.Payload)
	now := d.opts.Now()
	secrets := []string{w.Secret}
	if w.PreviousSecret != "" && now.Before(w.RotateTime.Add(d.opts.RotationGrace)) {
		secrets = append(secrets, w.PreviousSecret)
	}
	sigs := make([]string, 0, len(secrets))
	for _, stored := range secrets {
		secret, err := d.opts.Sealer.Open(stored)
		if err != nil {
			return 0, err
		}
		sig, err := Sign(secret, dl.EventID, now, body)
		if err != nil {
			return 0, err
		}
		sigs = append(sigs, sig)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URI, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", ContentType)
	req.Header.Set("User-Agent", d.opts.UserAgent)
	req.Header.Set(HeaderID, dl.EventID)
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(now.Unix(), 10))
	req.Header.Set(HeaderSignature, strings.Join(sigs, " "))
	resp, err := d.opts.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("the endpoint answered %s", resp.Status)
	}
	return resp.StatusCode, nil
}
