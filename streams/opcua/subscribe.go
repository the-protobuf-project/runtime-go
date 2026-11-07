package opcua

import (
	"context"
	"fmt"
	"math"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// cancelTimeout bounds how long ending a subscription waits for the server to
// confirm it. The caller has already walked away, so this only decides how long
// the server-side subscription may outlive it before the server's own lifetime
// count reaps it.
const cancelTimeout = 5 * time.Second

// Subscribe returns a channel of the subject's node values.
//
// The first message is the node's current value, which is how OPC UA starts
// every monitored item; each change follows. [streams.Prefetch] sets the
// monitored item's queue as well as the channel, so it is how many changes the
// server holds between publishing intervals before it starts discarding the
// oldest.
//
// A value whose status is not good is logged and skipped rather than
// delivered, because a message has nowhere to say that it is not a reading.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	node, err := m.node(ctx, subject)
	if err != nil {
		return nil, err
	}
	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: OPC UA has no shared subscriptions; every subscriber sees every change", streams.ErrUnsupported)
	}
	depth := core.Prefetch(o)
	if uint64(depth) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: a prefetch of %d is above what an OPC UA queue can hold (%d)", streams.ErrUnsupported, depth, uint32(math.MaxUint32))
	}

	// Unbuffered on purpose: gopcua blocks its publish loop until this is read,
	// and the goroutine below reads it continuously, so a buffer here would only
	// be a second queue behind the server's.
	notifs := make(chan *gopcua.PublishNotificationData)
	sub, err := m.store.client.Subscribe(ctx, &gopcua.SubscriptionParameters{Interval: m.store.cfg.interval}, notifs)
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot subscribe to %q: %w", subject, err)
	}

	item := gopcua.NewMonitoredItemCreateRequestWithDefaults(node, ua.AttributeIDValue, 1)
	item.RequestedParameters.QueueSize = uint32(max(depth, 1))
	res, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, item)
	if err == nil && res.Results[0].StatusCode != ua.StatusOK {
		err = res.Results[0].StatusCode
	}
	if err != nil {
		m.cancel(sub, notifs)
		return nil, fmt.Errorf("opcua: cannot monitor %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "subscribed", telemetry.Fields{"subject": subject, "queue": item.RequestedParameters.QueueSize})

	out := make(chan streams.Message, depth)
	go func() {
		defer close(out)
		defer m.cancel(sub, notifs)
		for {
			select {
			case <-ctx.Done():
				return
			case n := <-notifs:
				if !m.deliver(ctx, subject, n, out) {
					return
				}
			}
		}
	}()
	return out, nil
}

// deliver hands one notification's values to out, reporting false once ctx is
// done.
func (m *manager) deliver(ctx context.Context, subject string, n *gopcua.PublishNotificationData, out chan<- streams.Message) bool {
	if n.Error != nil {
		// gopcua reconnects and restores subscriptions on its own, so a
		// transport error is worth a record and not worth ending delivery.
		m.store.log.Warn(ctx, "subscription interrupted", telemetry.Fields{"subject": subject, "error": n.Error.Error()})
		return true
	}
	change, ok := n.Value.(*ua.DataChangeNotification)
	if !ok {
		return true
	}
	for _, item := range change.MonitoredItems {
		msg, err := decode(m.store.registry, subject, item.Value)
		if err != nil {
			m.store.log.Warn(ctx, "skipping a value", telemetry.Fields{"subject": subject, "error": err.Error()})
			continue
		}
		select {
		case out <- msg:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// cancel deletes a subscription on the server, draining its notifications
// until that is confirmed so gopcua's publish loop is never left blocked on a
// channel nobody reads.
func (m *manager) cancel(sub *gopcua.Subscription, notifs <-chan *gopcua.PublishNotificationData) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, stop := context.WithTimeout(context.Background(), cancelTimeout)
		defer stop()
		if err := sub.Cancel(ctx); err != nil {
			m.store.log.Warn(ctx, "could not end a subscription", telemetry.Fields{"error": err.Error()})
		}
	}()
	for {
		select {
		case <-notifs:
		case <-done:
			return
		}
	}
}
