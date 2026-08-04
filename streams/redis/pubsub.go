package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

// manager is a publisher and subscriber bound to one stream.
//
// It holds the stream metadata read at Bind time, so publishing and subscribing
// can validate subjects without a round trip on every call.
type manager struct {
	provider *Provider
	stream   streams.Stream
}

var _ streams.Manager = (*manager)(nil)

// Bind returns a publisher and subscriber for an existing stream.
//
// The stream is read here so that an unknown ID fails at Bind rather than at
// the first publish, and so the subject list is available for validation.
func (p *Provider) Bind(ctx context.Context, id string) (streams.Manager, error) {
	s, err := p.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &manager{provider: p, stream: s}, nil
}

// checkSubject rejects a subject the stream does not declare.
//
// Publishing to an undeclared subject would otherwise create a channel nobody
// subscribes to, and subscribing to one would wait forever — both silent.
func (m *manager) checkSubject(subject string) error {
	if slices.Contains(m.stream.Subjects, subject) {
		return nil
	}
	return fmt.Errorf("%w: %q (stream %s declares %v)",
		streams.ErrUnknownSubject, subject, m.stream.ID(), m.stream.Subjects)
}

// Publish sends a message on a subject.
//
// For an ordinary stream the message goes out immediately over pub/sub. For a
// notification stream it is held until its TTL expires — see [publishNotify].
func (m *manager) Publish(ctx context.Context, subject string, msg streams.Message) error {
	if err := m.checkSubject(subject); err != nil {
		return err
	}
	if msg.ID() == "" {
		msg.SetID(ulid.Generate().GetTimeCode())
	}

	body, err := json.Marshal(envelope{ID: msg.ID(), Data: msg.Data})
	if err != nil {
		return fmt.Errorf("streams/redis: failed to encode message: %w", err)
	}

	if m.provider.isNotify() {
		return m.publishNotify(ctx, subject, msg, body)
	}

	// Exactly once. Subscribe confirms its subscription before returning, so
	// there is no window a second send would cover — it would only deliver the
	// message twice.
	if err := m.provider.rdb.Publish(ctx,
		m.provider.keys.channel(m.stream.ID(), subject), body).Err(); err != nil {
		return fmt.Errorf("streams/redis: failed to publish on %q: %w", subject, err)
	}
	return nil
}

// publishNotify schedules a message to be delivered when its TTL expires.
//
// Two keys are written. The pending key carries the TTL and nothing else — its
// expiry event *is* the notification, and its name encodes the stream, subject,
// and message ID because the event carries only a key name. The payload key
// holds the body and does not expire, because the pending key's value is
// already gone by the time the event fires.
func (m *manager) publishNotify(ctx context.Context, subject string, msg streams.Message, body []byte) error {
	// A zero TTL would never expire, so the notification could never fire.
	// Accepting it silently would strand the subscriber waiting forever.
	if msg.TTL <= 0 {
		return fmt.Errorf("streams/redis: a notification needs a positive TTL, got %v", msg.TTL)
	}

	pending := m.provider.keys.pending(m.stream.ID(), subject, msg.ID())
	payload := m.provider.keys.payload(msg.ID())

	// The payload is written first: if the pending key expired before its body
	// existed, the subscriber would be notified about a message it cannot read.
	pipe := m.provider.rdb.TxPipeline()
	pipe.Set(ctx, payload, body, 0)
	pipe.Set(ctx, pending, msg.ID(), msg.TTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("streams/redis: failed to schedule notification on %q: %w", subject, err)
	}
	return nil
}

// envelope is the wire form of a message: the ID travels with the payload so a
// subscriber can report the same ID the publisher assigned.
type envelope struct {
	ID   string `json:"id"`
	Data any    `json:"data"`
}

// Subscribe returns a channel of messages for a subject.
//
// The channel is closed when ctx is done. That is the only way to stop the
// subscription, and it is what keeps the delivery goroutine and the server-side
// subscription from outliving the caller.
func (m *manager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(subject); err != nil {
		return nil, err
	}
	if m.provider.isNotify() {
		return m.subscribeNotify(ctx, subject)
	}

	sub := m.provider.rdb.Subscribe(ctx, m.provider.keys.channel(m.stream.ID(), subject))

	// Confirm the subscription is live before returning, so a message published
	// after Subscribe returns is delivered rather than raced.
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("streams/redis: failed to subscribe to %q: %w", subject, err)
	}

	out := make(chan streams.Message)
	go func() {
		defer close(out)
		defer func() { _ = sub.Close() }()

		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-sub.Channel():
				if !ok {
					return
				}
				msg, err := decode(raw.Payload)
				if err != nil {
					// A malformed payload is one bad message, not a reason to
					// tear down a healthy subscription.
					continue
				}
				// Send and cancellation race deliberately: without the ctx case
				// here, a consumer that stops reading would block this goroutine
				// forever and leak it along with the subscription.
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

// subscribeNotify delivers messages as their TTLs expire.
//
// Redis publishes one keyspace event per expired key across the whole database,
// so this filters by key prefix to the stream and subject asked for. An earlier
// version ignored the subject entirely and handed every subscriber every
// expiry in the database, including unrelated keys.
func (m *manager) subscribeNotify(ctx context.Context, subject string) (<-chan streams.Message, error) {
	sub := m.provider.rdb.Subscribe(ctx, expiryChannel(m.provider.db))
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("streams/redis: failed to subscribe to keyspace events: %w", err)
	}

	want := m.provider.keys.pendingPattern(m.stream.ID(), subject)

	out := make(chan streams.Message)
	go func() {
		defer close(out)
		defer func() { _ = sub.Close() }()

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
				msgID := strings.TrimPrefix(key, want)

				msg, err := m.provider.claimPayload(ctx, msgID)
				if err != nil {
					continue
				}
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

// claimPayload reads a notification body and removes it, so one expiry
// delivers one message even with several subscribers racing for it.
func (p *Provider) claimPayload(ctx context.Context, msgID string) (streams.Message, error) {
	raw, err := p.rdb.GetDel(ctx, p.keys.payload(msgID)).Bytes()
	if err != nil {
		return streams.Message{}, err
	}
	return decode(string(raw))
}

// decode turns a wire payload back into a Message.
func decode(payload string) (streams.Message, error) {
	var e envelope
	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return streams.Message{}, fmt.Errorf("streams/redis: malformed message: %w", err)
	}
	return streams.NewMessage(e.ID, e.Data, 0), nil
}
