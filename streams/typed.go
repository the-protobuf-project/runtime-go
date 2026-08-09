package streams

import (
	"context"
	"encoding/json"
	"fmt"
)

// encode turns a published value into bytes. Providers use it so every backend
// puts the same shape on the wire.
func encode(value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("streams: failed to encode value: %w", err)
	}
	return b, nil
}

// decode unmarshals a payload into dest.
func decode(data []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("streams: decode destination is nil")
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("streams: failed to decode message: %w", err)
	}
	return nil
}

// Encode is [encode] exported for providers in other modules.
func Encode(value any) ([]byte, error) { return encode(value) }

// Typed is a view over a [Manager] bound to one model.
//
// It is a wrapper, not a separate client: the underlying Manager is untyped and
// shared, so one provider — one connection, one configuration — serves every
// model in a program.
type Typed[T any] struct {
	m Manager
}

// For returns a view of m that publishes and receives T.
//
//	orders := streams.For[Order](mgr)
//	orders.Publish(ctx, "order.placed", o)
//
//	msgs, _ := orders.Subscribe(ctx, "order.placed")
//	for o := range msgs { … }   // o is an Order
func For[T any](m Manager) Typed[T] {
	return Typed[T]{m: m}
}

// Unwrap returns the underlying Manager, for the operations the view does not
// cover or to hand to something expecting the untyped contract.
func (t Typed[T]) Unwrap() Manager { return t.m }

// Publish sends a value on a subject and returns its message id.
func (t Typed[T]) Publish(ctx context.Context, subject string, value T, opts ...Option) (string, error) {
	return t.m.Publish(ctx, subject, value, opts...)
}

// Subscribe returns a channel of decoded values for a subject.
//
// A message that fails to decode as T is skipped rather than delivered as a
// zero value — a malformed payload is one bad message, not a reason to hand the
// caller something that looks valid. Use the untyped [Manager.Subscribe] when
// you need to see those.
//
// The channel is closed when ctx is done.
func (t Typed[T]) Subscribe(ctx context.Context, subject string) (<-chan T, error) {
	msgs, err := t.m.Subscribe(ctx, subject)
	if err != nil {
		return nil, err
	}

	out := make(chan T)
	go func() {
		defer close(out)
		for msg := range msgs {
			var v T
			if derr := msg.Decode(&v); derr != nil {
				continue
			}
			select {
			case out <- v:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
