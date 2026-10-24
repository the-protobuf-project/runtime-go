package events

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// memStore is a Store in memory, etag-guarded as a real one is.
type memStore struct {
	mu         sync.Mutex
	webhooks   map[string]Webhook
	deliveries map[string]Delivery
	updated    map[string]time.Time // a delivery's last write, for pruning
	cursors    map[string]Cursor
	etag       int
	now        func() time.Time
	// claimRace makes the next claim lose to another server.
	claimRace bool
}

func newMemStore() *memStore {
	return &memStore{webhooks: map[string]Webhook{}, deliveries: map[string]Delivery{}, updated: map[string]time.Time{},
		cursors: map[string]Cursor{}, now: time.Now}
}

func (m *memStore) nextEtag() string { m.etag++; return strconv.Itoa(m.etag) }

func (m *memStore) ListWebhooks(_ context.Context, token string) ([]Webhook, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Webhook, 0, len(m.webhooks))
	for name := range m.webhooks {
		out = append(out, m.webhooks[name])
	}
	slices.SortFunc(out, func(a, b Webhook) int { return strings.Compare(a.Name, b.Name) })
	// One webhook a page, to exercise paging.
	i := 0
	if token != "" {
		i, _ = strconv.Atoi(token)
	}
	if i >= len(out) {
		return nil, "", nil
	}
	next := ""
	if i+1 < len(out) {
		next = strconv.Itoa(i + 1)
	}
	return out[i : i+1], next, nil
}

func (m *memStore) GetWebhook(_ context.Context, name string) (Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.webhooks[name]
	if !ok {
		return Webhook{}, ErrNotFound
	}
	return w, nil
}

func (m *memStore) GetCursor(_ context.Context, feed string) (Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cursors[feed]
	if !ok {
		return Cursor{}, ErrNotFound
	}
	return c, nil
}

func (m *memStore) CreateCursor(_ context.Context, c Cursor) (Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.cursors[c.Feed]; ok {
		return Cursor{}, ErrAlreadyExists
	}
	c.Etag = m.nextEtag()
	m.cursors[c.Feed] = c
	return c, nil
}

func (m *memStore) UpdateCursor(_ context.Context, c Cursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cursors[c.Feed].Etag != c.Etag {
		return ErrConflict
	}
	c.Etag = m.nextEtag()
	m.cursors[c.Feed] = c
	return nil
}

func (m *memStore) CreateDelivery(_ context.Context, d Delivery) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.deliveries[d.Name]; ok {
		return ErrAlreadyExists
	}
	d.CreateTime, d.Etag = m.now(), m.nextEtag()
	m.deliveries[d.Name] = d
	m.updated[d.Name] = m.now()
	return nil
}

func (m *memStore) DueDeliveries(_ context.Context, now time.Time, limit int) ([]Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Delivery
	for name := range m.deliveries {
		if d := m.deliveries[name]; d.State == DeliveryPending && !d.NextAttemptTime.After(now) {
			out = append(out, d)
		}
	}
	slices.SortFunc(out, func(a, b Delivery) int { return a.NextAttemptTime.Compare(b.NextAttemptTime) })
	return out[:min(limit, len(out))], nil
}

func (m *memStore) UpdateDelivery(_ context.Context, d Delivery, paths []string) (Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.deliveries[d.Name]
	if !ok {
		return Delivery{}, ErrNotFound
	}
	if m.claimRace {
		m.claimRace = false
		cur.Etag = m.nextEtag()
		m.deliveries[d.Name] = cur
	}
	if cur.Etag != d.Etag {
		return Delivery{}, ErrConflict
	}
	for _, p := range paths {
		switch p {
		case PathState:
			cur.State = d.State
		case PathAttemptCount:
			cur.AttemptCount = d.AttemptCount
		case PathResponseCode:
			cur.ResponseCode = d.ResponseCode
		case PathErrorMessage:
			cur.ErrorMessage = d.ErrorMessage
		case PathNextAttemptTime:
			cur.NextAttemptTime = d.NextAttemptTime
		case PathDeliverTime:
			cur.DeliverTime = d.DeliverTime
		default:
			return Delivery{}, fmt.Errorf("unknown path %q", p)
		}
	}
	cur.Etag = m.nextEtag()
	m.deliveries[d.Name] = cur
	m.updated[d.Name] = m.now()
	return cur, nil
}

func (m *memStore) PruneDeliveries(_ context.Context, before time.Time, limit int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name := range m.deliveries {
		if limit == 0 {
			break
		}
		if d := m.deliveries[name]; (d.State == DeliverySucceeded || d.State == DeliveryFailed) && m.updated[name].Before(before) {
			delete(m.deliveries, name)
			limit--
		}
	}
	return nil
}

func (m *memStore) all() []Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Delivery, 0, len(m.deliveries))
	for name := range m.deliveries {
		out = append(out, m.deliveries[name])
	}
	slices.SortFunc(out, func(a, b Delivery) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// order is a record a feed reads.
type order struct {
	Name, Etag, State string
	Scope             string
	Create, Update    time.Time
	Delete            time.Time
}

func (o *order) GetName() string { return o.Name }
func (o *order) GetEtag() string { return o.Etag }

// orders is a table of orders and the queries made of it.
type orders struct {
	mu      sync.Mutex
	rows    map[string]order
	queries []Query
}

func (t *orders) put(o order) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rows == nil {
		t.rows = map[string]order{}
	}
	t.rows[o.Name] = o
}

func (t *orders) list(_ context.Context, q Query) ([]*order, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queries = append(t.queries, q)
	var out []*order
	for name := range t.rows {
		o := t.rows[name]
		switch {
		case len(q.Names) > 0 && !slices.Contains(q.Names, o.Name):
		case !q.Since.IsZero() && (o.Update.Before(q.Since) || (!q.SinceInclusive && o.Update.Equal(q.Since))):
		case !q.Until.IsZero() && o.Update.After(q.Until):
		case !o.Delete.IsZero() && !q.ShowDeleted:
		default:
			out = append(out, &o)
		}
	}
	slices.SortFunc(out, func(a, b *order) int { return a.Update.Compare(b.Update) })
	return out, "", nil
}

func (t *orders) feed() Feed {
	return NewFeed(FeedSpec[*order]{
		ID:       "orders",
		ReadMask: []string{"name", "etag", "update_time", "state"},
		List: func(ctx context.Context, q Query) ([]*order, string, error) {
			q.ShowDeleted = true
			return t.list(ctx, q)
		},
		Change: func(o *order) Change {
			typ := o.State
			if typ == "" {
				typ = Lifecycle(o.Create, o.Update, o.Delete)
			}
			if typ != "" {
				typ = "shop.order." + typ
			}
			return Change{Type: typ, Time: o.Update, Scope: o.Scope}
		},
		Data: func(o *order) ([]byte, error) {
			return json.Marshal(map[string]string{"name": o.Name, "state": o.State})
		},
	})
}

// receiver is an endpoint that records what it is sent and answers status.
type receiver struct {
	mu     sync.Mutex
	status int
	got    []*http.Request
	bodies []string
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, req)
	r.bodies = append(r.bodies, string(body))
	w.WriteHeader(r.status)
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

// clock is a settable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fixture struct {
	store *memStore
	table *orders
	recv  *receiver
	srv   *httptest.Server
	clock *clock
	d     *Dispatcher
	ctx   context.Context
}

func newFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	f := &fixture{store: newMemStore(), table: &orders{}, recv: &receiver{status: http.StatusOK},
		clock: &clock{t: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)}, ctx: context.Background()}
	f.srv = httptest.NewServer(f.recv)
	t.Cleanup(f.srv.Close)
	f.store.now = f.clock.now
	opts.Source, opts.Now, opts.Client = "//shop.example.com", f.clock.now, f.srv.Client()
	if opts.HookSettle == 0 {
		opts.HookSettle = time.Millisecond
	}
	f.d = NewDispatcher(f.store, []Feed{f.table.feed()}, opts)
	return f
}

func (f *fixture) hook(t *testing.T, name string, mod func(*Webhook)) string {
	t.Helper()
	secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	w := Webhook{Name: name, URI: f.srv.URL, Secret: secret, CreateTime: f.clock.now().Add(-time.Hour)}
	if mod != nil {
		mod(&w)
	}
	f.store.mu.Lock()
	f.store.webhooks[name] = w
	f.store.mu.Unlock()
	return w.Secret
}

func (f *fixture) round(t *testing.T) {
	t.Helper()
	if err := f.d.Round(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRoundDeliversSignedCloudEvents(t *testing.T) {
	f := newFixture(t, Options{UserAgent: "shop-webhooks/1.0"})
	secret := f.hook(t, "webhooks/a", nil)
	f.hook(t, "webhooks/b", func(w *Webhook) { w.EventTypes = []string{"shop.order.cancelled"} })
	f.round(t) // starts the cursor at now

	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)

	if f.recv.count() != 1 {
		t.Fatalf("%d POSTs, want 1 (webhooks/b does not want paid)", f.recv.count())
	}
	req, body := f.recv.got[0], f.recv.bodies[0]
	if req.Header.Get("Content-Type") != ContentType || req.Header.Get("User-Agent") != "shop-webhooks/1.0" {
		t.Fatalf("headers %v", req.Header)
	}
	id := EventID("orders/1", "e1")
	if req.Header.Get(HeaderID) != id ||
		!Verify(secret, id, req.Header.Get(HeaderTimestamp), []byte(body), req.Header.Get(HeaderSignature)) {
		t.Fatalf("signature does not verify: %v", req.Header)
	}
	var ev map[string]any
	if err := json.Unmarshal([]byte(body), &ev); err != nil || ev["type"] != "shop.order.paid" || ev["source"] != "//shop.example.com" {
		t.Fatalf("event %v, %v", ev, err)
	}
	ds := f.store.all()
	if len(ds) != 1 || ds[0].State != DeliverySucceeded || ds[0].AttemptCount != 1 || ds[0].ResponseCode != 200 || ds[0].DeliverTime.IsZero() {
		t.Fatalf("deliveries %+v", ds)
	}

	// The next rounds reread the Lag, but record and send nothing again.
	f.clock.add(time.Second)
	f.round(t)
	f.round(t)
	if f.recv.count() != 1 || len(f.store.all()) != 1 {
		t.Fatalf("a reread sent again: %d POSTs, %d deliveries", f.recv.count(), len(f.store.all()))
	}
}

// A second dispatcher over the same store -- another server -- that has not
// seen the change records it under the same name, and sends nothing twice.
func TestRoundIsSafeOnSeveralServers(t *testing.T) {
	f := newFixture(t, Options{})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	other := NewDispatcher(f.store, []Feed{f.table.feed()}, f.d.opts)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	if err := other.dispatch(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.round(t)
	if err := other.Round(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.recv.count() != 1 || len(f.store.all()) != 1 {
		t.Fatalf("%d POSTs, %d deliveries; want one of each", f.recv.count(), len(f.store.all()))
	}
}

func TestRoundRetriesThenFails(t *testing.T) {
	f := newFixture(t, Options{MaxAttempts: 3})
	f.hook(t, "webhooks/a", nil)
	f.recv.status = http.StatusServiceUnavailable
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)

	d := f.store.all()[0]
	if d.State != DeliveryPending || d.AttemptCount != 1 || d.ResponseCode != 503 ||
		!d.NextAttemptTime.Equal(f.clock.now().Add(Backoff(1))) || !strings.Contains(d.ErrorMessage, "503") {
		t.Fatalf("after one failure: %+v", d)
	}
	// Not due yet: nothing is sent.
	f.round(t)
	if f.recv.count() != 1 {
		t.Fatalf("sent before its backoff: %d", f.recv.count())
	}
	f.clock.add(Backoff(1))
	f.round(t)
	f.clock.add(Backoff(2))
	f.round(t)
	d = f.store.all()[0]
	if d.State != DeliveryFailed || d.AttemptCount != 3 || !d.NextAttemptTime.IsZero() || f.recv.count() != 3 {
		t.Fatalf("after MaxAttempts: %+v, %d POSTs", d, f.recv.count())
	}
}

func TestRoundPausedWaitsAndDeletedFails(t *testing.T) {
	f := newFixture(t, Options{})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	if err := f.d.dispatch(f.ctx); err != nil {
		t.Fatal(err)
	}

	f.store.mu.Lock()
	w := f.store.webhooks["webhooks/a"]
	w.Paused = true
	f.store.webhooks["webhooks/a"] = w
	f.store.mu.Unlock()
	f.round(t)
	if d := f.store.all()[0]; d.State != DeliveryPending || d.AttemptCount != 0 || f.recv.count() != 0 {
		t.Fatalf("a paused webhook's delivery moved: %+v", d)
	}

	f.store.mu.Lock()
	delete(f.store.webhooks, "webhooks/a")
	f.store.mu.Unlock()
	f.round(t)
	if d := f.store.all()[0]; d.State != DeliveryFailed || d.ErrorMessage != "the webhook was deleted" || f.recv.count() != 0 {
		t.Fatalf("a deleted webhook's delivery: %+v", d)
	}
}

func TestRoundClaimLostIsNotSent(t *testing.T) {
	f := newFixture(t, Options{})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.store.claimRace = true
	f.round(t)
	if f.recv.count() != 0 || f.store.all()[0].State != DeliveryPending {
		t.Fatal("a delivery another server claimed was sent")
	}
}

func TestRotationSignsWithBothSecrets(t *testing.T) {
	sealer, err := NewSealer(key(7), nil)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, Options{Sealer: sealer})
	oldSecret, _ := NewSecret()
	newSecret, _ := NewSecret()
	sealedOld, _ := sealer.Seal(oldSecret)
	sealedNew, _ := sealer.Seal(newSecret)
	f.hook(t, "webhooks/a", func(w *Webhook) {
		w.Secret, w.PreviousSecret, w.RotateTime = sealedNew, sealedOld, f.clock.now()
	})
	f.round(t)
	send := func(n int) *http.Request {
		f.clock.add(time.Second)
		f.table.put(order{Name: fmt.Sprintf("orders/%d", n), Etag: "e", State: "paid", Update: f.clock.now()})
		f.clock.add(time.Second)
		f.round(t)
		return f.recv.got[len(f.recv.got)-1]
	}
	req := send(1)
	sigs := strings.Fields(req.Header.Get(HeaderSignature))
	body := []byte(f.recv.bodies[len(f.recv.bodies)-1])
	ts := req.Header.Get(HeaderTimestamp)
	if len(sigs) != 2 || !Verify(oldSecret, req.Header.Get(HeaderID), ts, body, sigs[1]) || !Verify(newSecret, req.Header.Get(HeaderID), ts, body, sigs[0]) {
		t.Fatalf("within the grace both secrets sign: %v", sigs)
	}
	f.clock.add(24 * time.Hour)
	if sigs := strings.Fields(send(2).Header.Get(HeaderSignature)); len(sigs) != 1 {
		t.Fatalf("after the grace only the new secret signs: %v", sigs)
	}
}

func TestLifecycleFeedAndScope(t *testing.T) {
	f := newFixture(t, Options{})
	f.hook(t, "webhooks/club", func(w *Webhook) { w.Scope = "sites/club" })
	f.round(t)
	f.clock.add(time.Second)
	at := f.clock.now()
	f.table.put(order{Name: "orders/1", Etag: "e1", Scope: "sites/club", Create: at, Update: at})
	f.table.put(order{Name: "orders/2", Etag: "e1", Scope: "sites/other", Create: at, Update: at})
	// Deleted long ago and touched since: sent nobody.
	f.table.put(order{Name: "orders/3", Etag: "e1", Scope: "sites/club", Create: at.Add(-time.Hour), Update: at, Delete: at.Add(-time.Minute)})
	f.clock.add(time.Second)
	f.round(t)
	ds := f.store.all()
	if len(ds) != 1 || ds[0].Subject != "orders/1" || ds[0].EventType != "shop.order.created" {
		t.Fatalf("deliveries %+v", ds)
	}
}

// Each round rereads the Lag before the cursor; a write that commits late,
// stamped before what was read already, is still sent.
func TestLagCatchesLateCommits(t *testing.T) {
	f := newFixture(t, Options{Lag: 30 * time.Second})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	f.clock.add(10 * time.Second)
	early := f.clock.now()
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/2", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)
	// orders/1 was stamped before orders/2 but commits only now.
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: early})
	f.round(t)
	if len(f.store.all()) != 2 {
		t.Fatalf("a late commit within Lag was missed: %+v", f.store.all())
	}
}

// Once a round has read Lag past an idle cursor, the rounds after read only
// what is newer than it.
func TestSettledCursorSkipsLag(t *testing.T) {
	f := newFixture(t, Options{Lag: 30 * time.Second})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	pos := f.store.cursors["orders"].Position
	last := func() Query { return f.table.queries[len(f.table.queries)-1] }
	if q := last(); !q.SinceInclusive || !q.Since.Equal(pos.Add(-30*time.Second)) {
		t.Fatalf("first read %+v, want the Lag before the cursor", q)
	}
	f.clock.add(31 * time.Second)
	f.round(t) // reads past pos+Lag: settles
	f.round(t)
	if q := last(); q.SinceInclusive || !q.Since.Equal(pos) {
		t.Fatalf("settled read %+v, want only what is newer than the cursor", q)
	}
	if q := last(); !q.OrderByUpdateTime || len(q.ReadMask) != 4 || !q.Until.Equal(f.clock.now().Add(-time.Millisecond)) {
		t.Fatalf("read %+v: want ordered, masked, up to now less HookSettle", q)
	}
}

// A webhook created after a round read the webhooks is not owed the changes
// that round would take: the round stops at its read, less HookSettle.
func TestHookSettleBoundsTheRound(t *testing.T) {
	f := newFixture(t, Options{HookSettle: 5 * time.Second})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	f.clock.add(10 * time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now().Add(-2 * time.Second)})
	f.round(t)
	if len(f.store.all()) != 0 {
		t.Fatal("a change within HookSettle of the round was taken")
	}
	f.clock.add(5 * time.Second)
	f.round(t)
	if len(f.store.all()) != 1 {
		t.Fatal("the next round did not take it")
	}
}

func TestPruneDropsFinishedDeliveries(t *testing.T) {
	f := newFixture(t, Options{Retention: time.Hour})
	f.hook(t, "webhooks/a", nil)
	f.round(t)
	f.clock.add(time.Second)
	f.table.put(order{Name: "orders/1", Etag: "e1", State: "paid", Update: f.clock.now()})
	f.clock.add(time.Second)
	f.round(t)
	if len(f.store.all()) != 1 {
		t.Fatal("not delivered")
	}
	f.clock.add(2 * time.Hour)
	f.round(t)
	if len(f.store.all()) != 0 {
		t.Fatal("a finished delivery past Retention was kept")
	}
}

func TestStartRunsRounds(t *testing.T) {
	f := newFixture(t, Options{})
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	f.d.Start(ctx, time.Millisecond, func(err error) { t.Error(err) })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := f.store.GetCursor(ctx, "orders"); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Start ran no round")
		}
		time.Sleep(time.Millisecond)
	}
}
