package opcua

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// cancelTimeout bounds how long ending a subscription waits for the server to
// confirm it. The caller has already walked away, so this only decides how long
// the server-side subscription may outlive it before the server's own lifetime
// count reaps it.
const cancelTimeout = 5 * time.Second

// Subscribe returns a channel of the subject's node values.
//
// Each Subscribe adds a monitored item to the one client subscription the
// store keeps, reporting on status, value or timestamp change, so a write that
// repeats the last value is still a message. The server reports an item's
// current value as soon as it is created; that value was not written after the
// subscription, so it is skipped unless [WithInitialValue] says otherwise.
//
// [streams.Prefetch] sets the monitored item's queue size, which is how many
// changes the server holds between publishing intervals before it starts
// discarding the oldest; [WithQueueSize] is used without it. The channel holds
// [WithBuffer] notifications ahead of its reader, and past that drops the
// newest rather than stall the session.
//
// A value whose status is not good is logged and skipped rather than
// delivered, because a message has nowhere to say that it is not a reading.
//
// Canceling ctx deletes the item, and the subscription with the last one, and
// closes the channel; closing the store closes every channel.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	node, n, err := m.node(ctx, subject)
	if err != nil {
		return nil, err
	}
	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: OPC UA has no consumer groups; every monitored item receives every change, so there is nothing to share", streams.ErrUnsupported)
	}
	queue := m.store.cfg.queueSize
	if o.Prefetch > 0 {
		if uint64(o.Prefetch) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: a prefetch of %d is above what an OPC UA queue can hold (%d)", streams.ErrUnsupported, o.Prefetch, uint32(math.MaxUint32))
		}
		queue = uint32(o.Prefetch)
	}
	buffer := max(m.store.cfg.buffer, int(min(queue, math.MaxInt32)))

	out, err := m.store.hub.attach(ctx, subject, node, n, queue, buffer)
	if err != nil {
		return nil, err
	}
	m.store.log.Debug(ctx, "subscribed", telemetry.Fields{"subject": subject, "queue": queue})
	return out, nil
}
