package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/ulid"
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

// envelope is the wire form: the id travels with the payload so a subscriber
// reports the same id the publisher assigned.
type envelope struct {
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data"`
}

// checkSubject rejects a subject the stream does not declare.
//
// Publishing to an undeclared subject would create a channel nobody reads, and
// subscribing to one would wait forever — both silent failures.
func (m *streamManager) checkSubject(ctx context.Context, subject string) error {
	if slices.Contains(m.stream.Subjects, subject) {
		return nil
	}
	m.handler.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return fmt.Errorf("%w: %q (stream %s declares %v)",
		streams.ErrUnknownSubject, subject, m.stream.ID, m.stream.Subjects)
}

// Publish sends a value on a subject.
//
// For an immediate stream the value goes out over pub/sub. For a scheduled one
// it is held until its TTL expires — see [streamManager.schedule].
func (m *streamManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	o := streams.NewOptions(opts...)

	id := o.ID
	if id == "" {
		id = ulid.Generate().GetTimeCode()
	}

	data, err := streams.Encode(value)
	if err != nil {
		m.handler.log.Error(ctx, "could not encode the value", err,
			telemetry.Fields{"subject": subject, "id": id})
		return "", err
	}
	body, err := json.Marshal(envelope{ID: id, Data: data})
	if err != nil {
		return "", fmt.Errorf("redis: cannot encode message: %w", err)
	}

	if m.handler.scheduled() {
		return id, m.schedule(ctx, subject, id, body, o)
	}

	if o.TTL > 0 {
		// An immediate stream cannot honor a delay. Saying so beats publishing
		// now and letting the caller believe it was scheduled.
		m.handler.log.Error(ctx, "this stream delivers immediately and cannot schedule", nil,
			telemetry.Fields{"subject": subject, "ttl": o.TTL.String()})
		return "", fmt.Errorf("%w: stream %s delivers immediately; use ConnectScheduled for a TTL", streams.ErrUnsupported, m.stream.ID)
	}

	channel := m.handler.keys.channel(m.stream.ID, subject)
	m.handler.log.Debug(ctx, "publishing", telemetry.Fields{
		"subject": subject, "id": id, "channel": channel, "bytes": len(body),
	})

	// Exactly once. Subscribe confirms its subscription before returning, so
	// there is no window a second send would cover — it would only deliver the
	// message twice.
	if err := m.handler.rdb.Publish(ctx, channel, body).Err(); err != nil {
		m.handler.log.Error(ctx, "could not publish", err,
			telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("redis: cannot publish on %q: %w", subject, err)
	}

	m.handler.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id})
	return id, nil
}

// schedule holds a message until its TTL expires.
//
// Two keys are written. The pending key carries the TTL and nothing else — its
// expiry event is the delivery, and its name encodes the stream, subject, and
// message id because the event carries only a key name. The payload key holds
// the body and does not expire, because the pending key's value is already gone
// by the time the event fires.
func (m *streamManager) schedule(ctx context.Context, subject, id string, body []byte, o streams.Options) error {
	if o.TTL <= 0 {
		// A zero TTL would never expire, so the notification could never fire.
		// Accepting it silently would strand the subscriber.
		m.handler.log.Error(ctx, "a scheduled message needs a positive TTL", nil,
			telemetry.Fields{"subject": subject, "id": id})
		return fmt.Errorf("%w: a scheduled message needs a positive TTL, got %v", streams.ErrUnsupported, o.TTL)
	}

	pending := m.handler.keys.pending(m.stream.ID, subject, id)
	payload := m.handler.keys.payload(id)

	m.handler.log.Debug(ctx, "scheduling", telemetry.Fields{
		"subject": subject, "id": id, "ttl": o.TTL.String(), "pending": pending,
	})

	// The payload is written first: if the pending key expired before its body
	// existed, the subscriber would be told about a message it cannot read.
	pipe := m.handler.rdb.TxPipeline()
	pipe.Set(ctx, payload, body, 0)
	pipe.Set(ctx, pending, id, o.TTL)
	if _, err := pipe.Exec(ctx); err != nil {
		m.handler.log.Error(ctx, "could not schedule the message", err,
			telemetry.Fields{"subject": subject, "id": id})
		return fmt.Errorf("redis: cannot schedule on %q: %w", subject, err)
	}

	m.handler.log.Debug(ctx, "scheduled", telemetry.Fields{"subject": subject, "id": id})
	return nil
}

// Subscribe returns a channel of messages for a subject.
//
// The channel is closed when ctx is done. That is the only way to stop the
// subscription, and it is what keeps the delivery goroutine and the server-side
// subscription from outliving the caller.
func (m *streamManager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if m.handler.scheduled() {
		return m.subscribeScheduled(ctx, subject)
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

	out := make(chan streams.Message)
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
				msg, err := decodeEnvelope(subject, []byte(raw.Payload))
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
func (m *streamManager) subscribeScheduled(ctx context.Context, subject string) (<-chan streams.Message, error) {
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

	out := make(chan streams.Message)
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
	return decodeEnvelope(subject, raw)
}

// decodeEnvelope turns a wire payload back into a Message.
func decodeEnvelope(subject string, payload []byte) (streams.Message, error) {
	var e envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return streams.Message{}, fmt.Errorf("redis: malformed message: %w", err)
	}
	return streams.Message{ID: e.ID, Subject: subject, Data: e.Data}, nil
}
