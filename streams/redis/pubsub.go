package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// streamManager publishes to and subscribes from one stream.
//
// It holds the metadata read at Bind time, so subjects are validated without a
// round trip on every call.
type streamManager struct {
	handler *streamHandler
	stream  streams.Stream
}

var _ streams.Manager = (*streamManager)(nil)

// checkSubject rejects a subject the stream does not declare.
//
// Publishing to an undeclared subject would create a channel nobody reads, and
// subscribing to one would wait forever — both silent failures.
func (m *streamManager) checkSubject(ctx context.Context, subject string) error {
	return m.handler.declares(ctx, m.stream, subject)
}

// Publish sends a value on a subject.
//
// For an immediate stream the value goes out over pub/sub. For a scheduled one
// it is held until its TTL expires — see [streamManager.schedule].
//
// Headers travel inside the frame (version 2 of [core.PackWith]), because a
// PUBLISH or a string key has nowhere else to put them. A message without
// headers is still the version-1 frame every reader understands.
func (m *streamManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	o := streams.NewOptions(opts...)
	if err := m.checkTTL(ctx, subject, o); err != nil {
		return "", err
	}
	if err := refuseUnhonored(o); err != nil {
		return "", err
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return "", err
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.PackWith(m.handler.codec, id, value, headers)
	if err != nil {
		m.handler.log.Error(ctx, "could not encode the value", err,
			telemetry.Fields{"subject": subject, "id": id})
		return "", err
	}

	if m.handler.scheduled() {
		return id, m.schedule(ctx, subject, id, body, o)
	}

	channel := m.handler.keys.channel(m.stream.ID, subject)
	m.handler.log.Debug(ctx, "publishing", telemetry.Fields{
		"subject": subject, "id": id, "channel": channel, "bytes": len(body),
	})

	// Exactly once. Subscribe confirms its subscription before returning, so
	// there is no window a second send would cover — it would only deliver the
	// message twice. That includes go-redis's own repeat after a dropped
	// connection, which sendOnce turns off.
	if err := sendOnce(ctx, m.handler.pub, "PUBLISH", channel, body); err != nil {
		m.handler.log.Error(ctx, "could not publish", err,
			telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("redis: cannot publish on %q: %w", subject, err)
	}

	m.handler.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id})
	return id, nil
}

// checkTTL refuses a TTL on an immediate stream and its absence on a
// scheduled one.
func (m *streamManager) checkTTL(ctx context.Context, subject string, o streams.Options) error {
	if m.handler.scheduled() {
		if o.TTL <= 0 {
			// A zero TTL would never expire, so the notification could never
			// fire. Accepting it silently would strand the subscriber.
			m.handler.log.Error(ctx, "a scheduled message needs a positive TTL", nil,
				telemetry.Fields{"subject": subject})
			return fmt.Errorf("%w: a scheduled message needs a positive TTL, got %v", streams.ErrUnsupported, o.TTL)
		}
		return nil
	}
	if o.TTL > 0 {
		// An immediate stream cannot honor a delay. Saying so beats publishing
		// now and letting the caller believe it was scheduled.
		m.handler.log.Error(ctx, "this stream delivers immediately and cannot schedule", nil,
			telemetry.Fields{"subject": subject, "ttl": o.TTL.String()})
		return fmt.Errorf("%w: stream %s delivers immediately; use ConnectScheduled or UseScheduled for a TTL", streams.ErrUnsupported, m.stream.ID)
	}
	return nil
}

// schedule holds a message until its TTL expires.
//
// Two keys are written. The pending key carries the TTL and nothing else — its
// expiry event is the delivery, and its name encodes the stream, subject, and
// message id because the event carries only a key name. The payload key holds
// the body and does not expire, because the pending key's value is already gone
// by the time the event fires.
func (m *streamManager) schedule(ctx context.Context, subject, id string, body []byte, o streams.Options) error {
	pending := m.handler.keys.pending(m.stream.ID, subject, id)

	m.handler.log.Debug(ctx, "scheduling", telemetry.Fields{
		"subject": subject, "id": id, "ttl": o.TTL.String(), "pending": pending,
	})

	// Never repeated: a repeat after the first had landed restarts the timer,
	// and one that lands after a short timer already fired schedules the
	// message a second time.
	pipe := m.handler.rdb.TxPipeline()
	m.queueSchedule(ctx, pipe, subject, id, body, o.TTL)
	if _, err := pipe.Exec(ctx); err != nil {
		m.handler.log.Error(ctx, "could not schedule the message", err,
			telemetry.Fields{"subject": subject, "id": id})
		return fmt.Errorf("redis: cannot schedule on %q: %w", subject, err)
	}

	m.handler.log.Debug(ctx, "scheduled", telemetry.Fields{"subject": subject, "id": id})
	return nil
}

// queueSchedule queues the two writes a scheduled message needs onto pipe.
//
// The payload is queued first: if the pending key expired before its body
// existed, the subscriber would be told about a message it cannot read. PX
// rather than EX so a sub-second delay is honored, and at least one
// millisecond because Redis refuses a zero expiry.
func (m *streamManager) queueSchedule(ctx context.Context, pipe goredis.Pipeliner, subject, id string, body []byte, ttl time.Duration) {
	millis := max(ttl.Milliseconds(), 1)
	_ = pipe.Process(ctx, newOnce(ctx, "SET", m.handler.keys.payload(id), body))
	_ = pipe.Process(ctx, newOnce(ctx, "SET", m.handler.keys.pending(m.stream.ID, subject, id), id, "PX", millis))
}

// Subscribe returns a channel of messages for a subject.
//
// The channel is closed when ctx is done. That is the only way to stop the
// subscription, and it is what keeps the delivery goroutine and the server-side
// subscription from outliving the caller.
func (m *streamManager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if err := refuseGroup(streams.NewOptions(opts...)); err != nil {
		return nil, err
	}
	if m.handler.scheduled() {
		return m.subscribeScheduled(ctx, subject, opts...)
	}

	channel := m.handler.keys.channel(m.stream.ID, subject)
	sub := m.handler.rdb.Subscribe(ctx, channel)

	// Confirm the subscription is live before returning, so a value published
	// after Subscribe returns is delivered rather than raced.
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		m.handler.log.Error(ctx, "could not subscribe", err,
			telemetry.Fields{"subject": subject, "channel": channel})
		return nil, fmt.Errorf("redis: cannot subscribe to %q: %w", subject, err)
	}

	m.handler.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject, "channel": channel})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)
		defer func() { _ = sub.Close() }()

		delivered := 0
		defer func() {
			m.handler.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-sub.Channel():
				if !ok {
					return
				}
				msg, err := core.Unpack(m.handler.registry, subject, []byte(raw.Payload))
				if err != nil {
					// A malformed payload is one bad message, not a reason to
					// tear down a healthy subscription.
					m.handler.log.Warn(ctx, "dropping a malformed message",
						telemetry.Fields{"subject": subject, "error": err.Error()})
					continue
				}
				delivered++
				// Send and cancellation race deliberately: without the ctx case
				// here, a consumer that stops reading would block this
				// goroutine forever and leak the subscription with it.
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// subscribeScheduled delivers messages as their TTLs expire.
//
// Redis publishes one keyspace event per expired key across the whole database,
// so this filters by key prefix down to the stream and subject asked for.
func (m *streamManager) subscribeScheduled(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	channel := "__keyevent@" + strconv.Itoa(m.handler.db) + "__:expired"
	sub := m.handler.rdb.Subscribe(ctx, channel)

	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		m.handler.log.Error(ctx, "could not subscribe to keyspace events", err,
			telemetry.Fields{"subject": subject, "channel": channel})
		return nil, fmt.Errorf("redis: cannot subscribe to keyspace events: %w", err)
	}

	want := m.handler.keys.pendingPrefix(m.stream.ID, subject)
	m.handler.log.Info(ctx, "subscribed to scheduled deliveries", telemetry.Fields{
		"subject": subject, "channel": channel, "prefix": want,
	})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)
		defer func() { _ = sub.Close() }()

		delivered := 0
		defer func() {
			m.handler.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-sub.Channel():
				if !ok {
					return
				}
				// The event payload is the expired key name. Anything outside
				// this stream and subject belongs to someone else.
				key := strings.TrimSpace(event.Payload)
				if !strings.HasPrefix(key, want) {
					continue
				}
				id := strings.TrimPrefix(key, want)

				msg, err := m.claim(ctx, subject, id)
				if err != nil {
					m.handler.log.Warn(ctx, "could not claim a scheduled payload",
						telemetry.Fields{"subject": subject, "id": id, "error": err.Error()})
					continue
				}
				delivered++
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// claim reads a scheduled payload and removes it, so one expiry delivers one
// message even with several subscribers racing for it.
func (m *streamManager) claim(ctx context.Context, subject, id string) (streams.Message, error) {
	raw, err := m.handler.rdb.GetDel(ctx, m.handler.keys.payload(id)).Bytes()
	if err != nil {
		return streams.Message{}, err
	}
	return core.Unpack(m.handler.registry, subject, raw)
}

// PublishBatch sends several values on a subject.
//
// One pipeline for the whole batch: a PUBLISH each on an immediate stream, the
// SET pair each on a scheduled one inside one transaction, so a scheduled
// batch is scheduled whole or not at all. Nothing in it is repeated after a
// dropped connection, for the reasons [streamManager.Publish] gives, and a
// failure is reported against every entry it reached, since some may still
// have landed.
func (m *streamManager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if err := m.checkTTL(ctx, subject, o); err != nil {
		return nil, err
	}
	if err := refuseUnhonored(o); err != nil {
		return nil, err
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(values))
	if len(values) == 0 {
		return ids, nil
	}

	var pipe goredis.Pipeliner
	if m.handler.scheduled() {
		pipe = m.handler.rdb.TxPipeline()
	} else {
		pipe = m.handler.rdb.Pipeline()
	}
	channel := m.handler.keys.channel(m.stream.ID, subject)

	var failures []error
	queued := make([]int, 0, len(values))
	for i, value := range values {
		id := core.NewID()
		body, err := core.PackWith(m.handler.codec, id, value, headers)
		if err != nil {
			failures = append(failures, fmt.Errorf("entry %d: %w", i, err))
			continue
		}
		ids[i] = id
		queued = append(queued, i)
		if m.handler.scheduled() {
			m.queueSchedule(ctx, pipe, subject, id, body, o.TTL)
		} else {
			_ = pipe.Process(ctx, newOnce(ctx, "PUBLISH", channel, body))
		}
	}

	if len(queued) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			m.handler.log.Error(ctx, "could not publish a batch", err,
				telemetry.Fields{"subject": subject, "count": len(queued)})
			// A pipeline reports one failure for the lot, so every entry it
			// carried carries it; some may still have landed.
			for _, i := range queued {
				failures = append(failures, fmt.Errorf("entry %d: redis: cannot publish on %q: %w", i, subject, err))
			}
		}
	}
	return ids, core.BatchError(subject, len(values), failures)
}

// refuseGroup refuses [streams.Group] where Redis cannot share a subject.
//
// A group changes what a subscription means: honoring it by fanning out
// anyway would double the work the caller asked to split.
func refuseGroup(o streams.Options) error {
	if o.Group != "" {
		return fmt.Errorf("%w: Redis cannot share a subscription among a group (%q); every subscriber receives every message, so drop the Group option or use a durable consumer", streams.ErrUnsupported, o.Group)
	}
	return nil
}
