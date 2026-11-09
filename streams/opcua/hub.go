package opcua

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// One client subscription per store, and the routing of its notifications to
// the channels that asked for them.
//
// OPC UA servers limit subscriptions per session and charge per subscription
// for every publishing cycle, so a store keeps one and adds a monitored item to
// it per Subscribe. It is created with the first item and deleted with the
// last.
//
// # Identity across reconnects
//
// When the server loses a session, gopcua recreates the subscription and its
// monitored items on the new one, and the server hands out new ids for both.
// What survives is the client handle, the number this package chose for each
// item and the one every notification names. So routes are keyed by client
// handle, and the server's ids are learned again after a recreate.
//
// A namespace index is the same kind of number: a restarted server may
// renumber its namespaces, so an item named by namespace URI is checked
// against the new session's namespace table, and moved when its index
// changed. gopcua recreates an item with the node id it had, which on a
// renumbered server names another node, or none.
//
// gopcua resumes its publishing loop itself once a reconnect is done, so
// unlike async-opcua 0.19 a recreated subscription needs no nudge to be heard.

// nextHandle hands out client handles from a range far above the small
// numbers callers tend to use, so a monitored item of this package's is not
// mistaken for one the caller created on the same client.
var nextHandle atomic.Uint32

func init() { nextHandle.Store(0x7000_0000) }

// hub is a store's client subscription and its routing table.
type hub struct {
	s *store

	// slot is held across every create and delete of the subscription and
	// its items, so one subscribe and one cancel never race over whether the
	// subscription exists. It guards the fields below it.
	slot    sync.Mutex
	sub     *gopcua.Subscription
	subID   uint32          // the subscription's id when last seen
	session *gopcua.Session // the session the items were last checked on

	// notifs is every notification the subscription sends; one goroutine
	// reads it for the life of the store, so gopcua's publishing loop, which
	// blocks until a notification is read, never waits on a slow reader.
	notifs  chan *gopcua.PublishNotificationData
	start   sync.Once
	stop    chan struct{}
	stopped chan struct{}

	mu     sync.Mutex // guards routes and closed
	routes map[uint32]*route
	closed bool

	nsMu sync.Mutex // guards ns
	ns   namespaces
}

// route is where one monitored item's notifications go.
type route struct {
	handle  uint32 // the client handle it is routed under; guarded by mu
	subject string
	node    *ua.NodeID
	named   *named
	queue   uint32
	item    uint32 // the server's id for the item; 0 when not known
	out     chan streams.Message
	done    chan struct{}
	end     sync.Once
	initial initial
	dropped uint64
}

// initial is what becomes of an item's first notification.
//
// The server reports a new item's current value at once. That value was not
// written after the subscription, so by default it is skipped; but an item
// whose queue is full before the first publishing cycle has discarded it for a
// later write, which must be delivered. So the node is read before the item is
// made, and the first notification is skipped only when it is that value: the
// same value, status and source timestamp, which OPC UA changes only with the
// value or status (Part 4, 7.11).
type initial struct {
	deliver bool          // deliver the first notification
	read    *ua.DataValue // skip it if it is this; nil skips any
}

// newHub returns an empty hub: no subscription exists until the first
// Subscribe asks for one.
func newHub(s *store) *hub {
	return &hub{
		s:       s,
		notifs:  make(chan *gopcua.PublishNotificationData),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
		routes:  map[uint32]*route{},
	}
}

// keepAlive is how long a healthy subscription goes without sending anything:
// one keep-alive period, and how often the watchdog looks at the session.
func (h *hub) keepAlive() time.Duration {
	return h.s.cfg.interval * time.Duration(h.s.cfg.keepAlive)
}

// run starts the notification reader and the watchdog, once.
func (h *hub) run() {
	h.start.Do(func() {
		go h.dispatch()
		go h.watch()
	})
}

// attach adds a monitored item on node and returns the channel of its
// notifications, live once this returns.
func (h *hub) attach(ctx context.Context, subject string, node *ua.NodeID, n *named, queue uint32, buffer int) (<-chan streams.Message, error) {
	r := &route{
		subject: subject, node: node, named: n, queue: queue,
		out:  make(chan streams.Message, buffer),
		done: make(chan struct{}),
	}
	if h.s.cfg.initialValue {
		r.initial.deliver = true
	} else {
		r.initial = h.before(ctx, node)
	}
	handle := nextHandle.Add(1)
	r.handle = handle

	// The server is asked without the caller's cancellation, so a subscribe
	// canceled midway cannot cut a request off halfway and leave an item the
	// client never learns of; the item is deleted below instead.
	call := context.WithoutCancel(ctx)
	h.slot.Lock()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.slot.Unlock()
		return nil, errors.New("opcua: the provider is closed")
	}
	// Routed before the server is asked, because the server may report the
	// item before it answers the request.
	h.routes[handle] = r
	h.mu.Unlock()
	h.run()
	err := h.create(call, handle, r)
	if err != nil {
		h.mu.Lock()
		delete(h.routes, handle)
		h.mu.Unlock()
		h.releaseIfEmpty(call)
	}
	h.slot.Unlock()
	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-r.done:
		}
		h.detach(r)
	}()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return r.out, nil
}

// before reads what node holds now, for recognizing the server's first report.
func (h *hub) before(ctx context.Context, node *ua.NodeID) initial {
	res, err := h.s.client.Read(ctx, &ua.ReadRequest{
		NodesToRead:        []*ua.ReadValueID{{NodeID: node, AttributeID: ua.AttributeIDValue}},
		TimestampsToReturn: ua.TimestampsToReturnSource,
	})
	if err != nil || len(res.Results) != 1 || res.Results[0] == nil {
		return initial{}
	}
	return initial{read: res.Results[0]}
}

// create adds the monitored item for r under handle, creating the
// subscription first if there is none. The caller holds slot.
func (h *hub) create(ctx context.Context, handle uint32, r *route) error {
	cfg := h.s.cfg
	if h.sub == nil {
		sub, err := h.s.client.Subscribe(ctx, &gopcua.SubscriptionParameters{
			Interval:                   cfg.interval,
			LifetimeCount:              cfg.lifetime,
			MaxKeepAliveCount:          cfg.keepAlive,
			MaxNotificationsPerPublish: cfg.maxNotifications,
		}, h.notifs)
		if err != nil {
			return fmt.Errorf("opcua: cannot monitor %q: %w", r.subject, err)
		}
		h.sub, h.subID, h.session = sub, sub.SubscriptionID, h.s.client.Session()
	}

	req := gopcua.NewMonitoredItemCreateRequestWithDefaults(r.node, ua.AttributeIDValue, handle)
	req.RequestedParameters.SamplingInterval = float64(cfg.sampling) / float64(time.Millisecond)
	req.RequestedParameters.QueueSize = r.queue
	req.RequestedParameters.DiscardOldest = true
	// Every write is a message, even one repeating the last value: its source
	// timestamp differs, and this trigger reports that.
	req.RequestedParameters.Filter = ua.NewExtensionObject(&ua.DataChangeFilter{
		Trigger: ua.DataChangeTriggerStatusValueTimestamp,
	})
	res, err := h.sub.Monitor(ctx, ua.TimestampsToReturnBoth, req)
	if err != nil {
		return fmt.Errorf("opcua: cannot monitor %q: %w", r.subject, err)
	}
	if len(res.Results) != 1 {
		return fmt.Errorf("opcua: cannot monitor %q: the server returned %d results", r.subject, len(res.Results))
	}
	result := res.Results[0]
	if result.StatusCode != ua.StatusOK {
		return refusal(r.subject, result.StatusCode)
	}
	r.item = result.MonitoredItemID
	if result.RevisedQueueSize < r.queue {
		h.s.log.Warn(ctx, "the server holds fewer changes per publishing cycle than asked; a burst larger than that between cycles keeps only its newest values",
			telemetry.Fields{"subject": r.subject, "requested": r.queue, "revised": result.RevisedQueueSize})
	}
	h.s.log.Debug(ctx, "monitoring", telemetry.Fields{
		"subject": r.subject, "subscription": h.subID, "item": r.item,
		"queue": result.RevisedQueueSize, "sampling_ms": result.RevisedSamplingInterval,
	})
	return nil
}

// refusal is the error for a monitored item the server refused.
func refusal(subject string, status ua.StatusCode) error {
	switch status {
	case ua.StatusBadNodeIDUnknown, ua.StatusBadNodeIDInvalid, ua.StatusBadAttributeIDInvalid,
		ua.StatusBadUserAccessDenied, ua.StatusBadNotReadable:
		return fmt.Errorf("%w: opcua: the server refused to monitor %q: %w", streams.ErrUnsupported, subject, status)
	}
	return fmt.Errorf("opcua: the server refused to monitor %q: %w", subject, status)
}

// finish removes the route under handle and closes its channel. The caller
// holds mu, which dispatch holds while it sends, so nothing is sent on a
// closed channel.
func (h *hub) finish(handle uint32) *route {
	r := h.routes[handle]
	if r == nil {
		return nil
	}
	delete(h.routes, handle)
	r.end.Do(func() {
		close(r.out)
		close(r.done)
	})
	return r
}

// detach deletes r's monitored item, and the subscription when it was the
// last.
func (h *hub) detach(r *route) {
	ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
	defer cancel()
	h.slot.Lock()
	defer h.slot.Unlock()

	h.mu.Lock()
	ended := h.finish(r.handle) == r
	left := len(h.routes)
	h.mu.Unlock()
	if !ended {
		// Close or a failed move ended it, and dealt with the server.
		return
	}
	if r.dropped > 0 {
		h.s.log.Warn(ctx, "notifications were dropped because the reader fell behind",
			telemetry.Fields{"subject": r.subject, "dropped": r.dropped})
	}
	if left == 0 {
		// Deleting the subscription deletes its items.
		h.releaseIfEmpty(ctx)
		return
	}
	h.unmonitor(ctx, r)
}

// unmonitor deletes r's item from the subscription. One whose server id was
// lost to a recreate stays until the subscription goes; its handle no longer
// routes anywhere, so what it reports is dropped. The caller holds slot.
func (h *hub) unmonitor(ctx context.Context, r *route) {
	if h.sub == nil || r.item == 0 {
		return
	}
	res, err := h.sub.Unmonitor(ctx, r.item)
	switch {
	case err != nil:
		h.s.log.Warn(ctx, "cannot delete a monitored item", telemetry.Fields{"subject": r.subject, "error": err.Error()})
	case len(res.Results) == 1 && res.Results[0] != ua.StatusOK && res.Results[0] != ua.StatusBadMonitoredItemIDInvalid:
		h.s.log.Warn(ctx, "the server refused to delete a monitored item", telemetry.Fields{"subject": r.subject, "status": res.Results[0].Error()})
	default:
		h.s.log.Debug(ctx, "monitored item deleted", telemetry.Fields{"subject": r.subject, "item": r.item})
	}
}

// releaseIfEmpty deletes the subscription when no route needs it any more.
// The caller holds slot.
func (h *hub) releaseIfEmpty(ctx context.Context) {
	h.mu.Lock()
	empty := len(h.routes) == 0
	h.mu.Unlock()
	if !empty || h.sub == nil {
		return
	}
	sub := h.sub
	h.sub = nil
	if err := sub.Cancel(ctx); err != nil {
		h.s.log.Debug(ctx, "the subscription was already gone", telemetry.Fields{"error": err.Error()})
		return
	}
	h.s.log.Debug(ctx, "subscription deleted", nil)
}

// close ends every route and deletes the subscription.
func (h *hub) close() {
	ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
	defer cancel()
	h.slot.Lock()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.slot.Unlock()
		return
	}
	h.closed = true
	for handle := range h.routes {
		h.finish(handle)
	}
	h.mu.Unlock()
	h.releaseIfEmpty(ctx)
	h.slot.Unlock()

	// Canceling forgot the subscription under gopcua's own lock, which it
	// holds while it hands over a notification, so none can arrive now.
	close(h.stop)
	// Never started, there is nothing to wait for.
	h.start.Do(func() { close(h.stopped) })
	<-h.stopped
}

// items is how many monitored items the store's streams hold.
func (h *hub) items() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.routes)
}

// dispatch reads every notification and hands it to its route.
func (h *hub) dispatch() {
	defer close(h.stopped)
	for {
		select {
		case <-h.stop:
			return
		case n := <-h.notifs:
			h.route(n)
		}
	}
}

// route hands one notification's values to their streams. It never waits: a
// stream whose reader has fallen its buffer behind loses the notification, as
// the server's own queue would.
func (h *hub) route(n *gopcua.PublishNotificationData) {
	ctx := context.Background()
	if n.Error != nil {
		// gopcua reconnects and restores the subscription on its own, so a
		// transport error is worth a record and not worth ending delivery.
		h.s.log.Warn(ctx, "subscription interrupted", telemetry.Fields{"error": n.Error.Error()})
		return
	}
	change, ok := n.Value.(*ua.DataChangeNotification)
	if !ok {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, item := range change.MonitoredItems {
		r := h.routes[item.ClientHandle]
		if r == nil || !r.first(item.Value) {
			continue
		}
		msg, err := decode(h.s.registry, r.subject, item.Value)
		if err != nil {
			h.s.log.Warn(ctx, "skipping a value", telemetry.Fields{"subject": r.subject, "error": err.Error()})
			continue
		}
		select {
		case r.out <- msg:
		default:
			r.dropped++
			if bits.OnesCount64(r.dropped) == 1 {
				h.s.log.Warn(ctx, "reader is behind; dropping notifications",
					telemetry.Fields{"subject": r.subject, "dropped": r.dropped})
			}
		}
	}
}

// first reports whether a notification is to be delivered, settling what
// becomes of the item's first one.
func (r *route) first(v *ua.DataValue) bool {
	if r.initial.deliver {
		return true
	}
	read := r.initial.read
	r.initial = initial{deliver: true}
	return read != nil && !sameValue(read, v)
}

// sameValue reports whether a notification is the value a read returned: the same
// value, status and source timestamp.
func sameValue(read, reported *ua.DataValue) bool {
	if reported == nil {
		return false
	}
	var a, b any
	if read.Value != nil {
		a = read.Value.Value()
	}
	if reported.Value != nil {
		b = reported.Value.Value()
	}
	return reflect.DeepEqual(a, b) &&
		read.Status == reported.Status &&
		read.SourceTimestamp.Equal(reported.SourceTimestamp) &&
		read.SourcePicoseconds == reported.SourcePicoseconds
}

// watch looks at the session once a keep-alive period, for as long as the
// store is open.
func (h *hub) watch() {
	tick := time.NewTicker(h.keepAlive())
	defer tick.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-tick.C:
			h.check()
		}
	}
}

// check catches up with a reconnect: a subscription gopcua recreated has new
// server ids for its items, and a new session may have renumbered the
// namespaces items named by URI were resolved in.
func (h *hub) check() {
	if h.s.client.State() != gopcua.Connected {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
	defer cancel()
	h.slot.Lock()
	defer h.slot.Unlock()
	if h.sub == nil {
		return
	}

	// SubscriptionIDs takes the lock gopcua recreates under, so the
	// subscription's id read after it is the recreated one.
	if !slices.Contains(h.s.client.SubscriptionIDs(), h.subID) {
		h.subID = h.sub.SubscriptionID
		h.s.log.Info(ctx, "the subscription was recreated", telemetry.Fields{"subscription": h.subID})
		h.relearn(ctx)
	}
	if session := h.s.client.Session(); session != h.session {
		if h.rebind(ctx) {
			h.session = session
		}
	}
}

// relearn asks the server for the ids of a recreated subscription's items
// (Server.GetMonitoredItems, Part 5, 9.1), so a later cancel deletes the
// right one. A server without the method leaves them unknown. The caller
// holds slot.
func (h *hub) relearn(ctx context.Context) {
	h.mu.Lock()
	for _, r := range h.routes {
		r.item = 0
	}
	h.mu.Unlock()

	res, err := h.s.client.Call(ctx, &ua.CallMethodRequest{
		ObjectID:       ua.NewNumericNodeID(0, id.Server),
		MethodID:       ua.NewNumericNodeID(0, id.Server_GetMonitoredItems),
		InputArguments: []*ua.Variant{ua.MustVariant(h.subID)},
	})
	if err != nil || res.StatusCode != ua.StatusOK || len(res.OutputArguments) != 2 {
		h.s.log.Debug(ctx, "cannot learn the recreated items' ids; they go with the subscription", nil)
		return
	}
	servers, _ := res.OutputArguments[0].Value().([]uint32)
	clients, _ := res.OutputArguments[1].Value().([]uint32)
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range min(len(servers), len(clients)) {
		if r := h.routes[clients[i]]; r != nil {
			r.item = servers[i]
		}
	}
}

// rebind moves every item named by namespace URI whose index the server's
// namespace table no longer gives it. A moved item gets a new client handle,
// so whatever the stale one reports, if the server still has it, routes
// nowhere. It reports whether every item was checked. The caller holds slot.
func (h *hub) rebind(ctx context.Context) bool {
	h.mu.Lock()
	type move struct {
		handle uint32
		r      *route
	}
	var named []move
	for handle, r := range h.routes {
		if r.named != nil {
			named = append(named, move{handle, r})
		}
	}
	h.mu.Unlock()

	done := true
	for _, m := range named {
		node, err := h.resolve(ctx, m.r.subject, *m.r.named)
		if err != nil {
			// A namespace the server no longer has will not come back by
			// asking again on this session.
			done = done && errors.Is(err, streams.ErrUnsupported)
			h.s.log.Warn(ctx, "cannot check a monitored node", telemetry.Fields{"subject": m.r.subject, "error": err.Error()})
			continue
		}
		if node.Namespace() == m.r.node.Namespace() {
			continue
		}
		h.s.log.Info(ctx, "the server renumbered a namespace; moving the monitored item", telemetry.Fields{
			"subject": m.r.subject, "from": m.r.node.Namespace(), "to": node.Namespace(),
		})
		h.unmonitor(ctx, m.r)
		handle := nextHandle.Add(1)
		h.mu.Lock()
		delete(h.routes, m.handle)
		h.routes[handle] = m.r
		m.r.handle = handle
		m.r.node = node
		// What the renumbered node holds is news to this stream.
		m.r.initial = initial{deliver: true}
		h.mu.Unlock()
		if err := h.create(ctx, handle, m.r); err != nil {
			// Ending the stream says the node is gone better than a silence
			// would.
			h.s.log.Warn(ctx, "cannot move a monitored item; ending its stream", telemetry.Fields{"subject": m.r.subject, "error": err.Error()})
			h.mu.Lock()
			h.finish(handle)
			h.mu.Unlock()
		}
	}
	return done
}
