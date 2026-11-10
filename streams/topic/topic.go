package topic

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Topic is one subject of a bound stream, typed to the message T it carries
// and encoded with its spec's [Codec].
//
// It is a small value, a manager, a spec pointer, a codec and a telemetry
// handle, so making one per call, as generated accessors do, costs nothing.
type Topic[T any] struct {
	m     streams.Manager
	spec  *Spec[T]
	codec Codec[T]
	tel   *Telemetry
}

// New returns a topic of the stream m is bound to, reporting nothing until
// given telemetry with [Topic.WithTelemetry].
//
// The subject is not checked here: a stream that does not declare it fails the
// first publish or subscribe with [streams.ErrUnknownSubject], as a raw call
// would.
func New[T any](m streams.Manager, spec *Spec[T]) Topic[T] {
	codec := spec.Codec
	if codec == nil {
		codec = JSON[T]()
	}
	return Topic[T]{m: m, spec: spec, codec: codec, tel: noTelemetry}
}

// WithTelemetry returns t reporting through tel.
func (t Topic[T]) WithTelemetry(tel *Telemetry) Topic[T] {
	if tel == nil {
		tel = noTelemetry
	}
	t.tel = tel
	return t
}

// Subject is the subject it publishes and subscribes on.
func (t Topic[T]) Subject() string { return t.spec.Subject }

// Spec is its subject and defaults.
func (t Topic[T]) Spec() *Spec[T] { return t.spec }

// Codec is its wire format.
func (t Topic[T]) Codec() Codec[T] { return t.codec }

// Manager is the manager it publishes and receives through.
func (t Topic[T]) Manager() streams.Manager { return t.m }

// Publisher publishes messages: done when the broker accepted them.
func (t Topic[T]) Publisher() Publisher[T] { return Publisher[T]{t: t} }

// Subscriber receives messages at most once, from now on.
func (t Topic[T]) Subscriber() Subscriber[T] { return Subscriber[T]{t: t} }

// Consumer receives messages at least once, under a named consumer whose
// position the broker keeps.
func (t Topic[T]) Consumer() Consumer[T] { return Consumer[T]{t: t} }

// publishOptions are the options a publish of value starts from, with over
// after them, so every setting over makes wins and its headers merge on top.
func (t Topic[T]) publishOptions(value T, over []streams.Option) []streams.Option {
	s := t.spec
	opts := make([]streams.Option, 0, 4+len(s.HeaderFields)+len(over))
	if s.PartitionKey != nil {
		if key, ok := s.PartitionKey(value); ok {
			opts = append(opts, streams.PartitionKey(key))
		}
	}
	if s.Expiry > 0 {
		opts = append(opts, streams.Expires(s.Expiry))
	}
	if s.Retain {
		opts = append(opts, streams.Retain())
	}
	if len(s.Headers) > 0 {
		opts = append(opts, streams.WithHeaders(s.Headers))
	}
	for _, hf := range s.HeaderFields {
		if v, ok := hf.Field(value); ok {
			opts = append(opts, streams.Header(hf.Header, v))
		}
	}
	return append(opts, over...)
}

// receiveOptions are the options a subscription or consumer starts from, with
// over after them.
func (t Topic[T]) receiveOptions(over []streams.Option) []streams.Option {
	opts := make([]streams.Option, 0, 2+len(over))
	if t.spec.Group != "" {
		opts = append(opts, streams.Group(t.spec.Group))
	}
	if t.spec.Prefetch > 0 {
		opts = append(opts, streams.Prefetch(t.spec.Prefetch))
	}
	return append(opts, over...)
}

// encode encodes values as one encode operation.
func (t Topic[T]) encode(ctx context.Context, values []T) ([]streams.Encoded, error) {
	_, op := t.tel.start(ctx, "encode", t.spec.Subject, t.codec.Name(), telemetry.SpanKindInternal)
	out := make([]streams.Encoded, len(values))
	for i, v := range values {
		data, err := t.codec.Encode(v)
		if err != nil {
			op.FinishErr(err)
			return nil, err
		}
		out[i] = streams.Encoded{Codec: t.codec.Name(), Data: data}
		t.tel.sized(ctx, t.spec.Subject, "send", len(data))
	}
	op.WrittenRecords(len(values))
	op.Finish(telemetry.OutcomeOK)
	return out, nil
}

// decode decodes msg as one decode operation: with the topic's codec when
// the publisher used it, and through the message's own codec otherwise, so a
// protobuf topic still reads a JSON message an older publisher wrote.
func (t Topic[T]) decode(ctx context.Context, msg streams.Message) (T, error) {
	_, op := t.tel.start(ctx, "decode", msg.Subject, msg.CodecName(), telemetry.SpanKindInternal)
	var (
		v   T
		err error
	)
	if msg.CodecName() == t.codec.Name() {
		v, err = t.codec.Decode(msg.Data)
	} else {
		v, err = decoder[T]()(msg.Decode)
	}
	if err == nil {
		op.ReadRecords(1)
	}
	op.FinishErr(err)
	return v, err
}

// decoder returns how a value of T is decoded through a function that fills a
// destination.
//
// A generated topic carries a pointer to a message, and decoding into a nil *T
// would hand the codec a **T it cannot use, so a pointer type gets a fresh
// value to decode into; anything else decodes through its address.
func decoder[T any]() func(into func(dest any) error) (T, error) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Pointer {
		elem := typ.Elem()
		return func(into func(any) error) (T, error) {
			v, _ := reflect.New(elem).Interface().(T)
			return v, into(v)
		}
	}
	return func(into func(any) error) (T, error) {
		var v T
		err := into(&v)
		return v, err
	}
}

// Publisher publishes T on a topic.
type Publisher[T any] struct {
	t Topic[T]
}

// Publish publishes value with the topic's defaults, opts laid over them, and
// returns its id.
func (p Publisher[T]) Publish(ctx context.Context, value T, opts ...streams.Option) (string, error) {
	ids, err := p.send(ctx, []T{value}, p.t.publishOptions(value, opts), false)
	if len(ids) == 0 {
		return "", err
	}
	return ids[0], err
}

// PublishBatch publishes values in order and returns their ids, one per value.
//
// It uses the provider's [streams.Batch] where it has one and publishes one at
// a time where it does not, so the call works everywhere and is fast where the
// broker allows. Consecutive values that share a partition key and headers go
// out as one batch, since a batch carries one set of options; values that
// alternate keys cost one batch each.
//
// On an error the ids returned are those of every value handed to the broker
// so far, as [streams.Batch.PublishBatch] reports them.
func (p Publisher[T]) PublishBatch(ctx context.Context, values []T) ([]string, error) {
	ids := make([]string, 0, len(values))
	for start := 0; start < len(values); {
		opts := p.t.publishOptions(values[start], nil)
		resolved := streams.NewOptions(opts...)
		end := start + 1
		for end < len(values) && sameOptions(streams.NewOptions(p.t.publishOptions(values[end], nil)...), resolved) {
			end++
		}
		run, err := p.send(ctx, values[start:end], opts, true)
		ids = append(ids, run...)
		if err != nil {
			return ids, err
		}
		start = end
	}
	return ids, nil
}

// send publishes values that share one set of options as one send operation:
// encoded by the topic's codec, stamped with the trace and the send time, and
// handed to the provider's batch where batch asks for it and there is one.
func (p Publisher[T]) send(ctx context.Context, values []T, opts []streams.Option, batch bool) ([]string, error) {
	t := p.t
	subject := t.spec.Subject
	ctx, op := t.tel.start(ctx, "send", subject, t.codec.Name(), telemetry.SpanKindProducer)
	if len(values) > 1 {
		op.Span().SetAttribute("messaging.batch.message_count", len(values))
	}

	encoded, err := t.encode(ctx, values)
	if err != nil {
		op.FinishErr(err)
		return nil, err
	}
	opts = append(opts, streams.WithHeaders(t.tel.stamp(ctx, time.Now())))

	var ids []string
	if b, berr := streams.AsBatch(t.m); batch && berr == nil {
		anys := make([]any, len(encoded))
		for i, e := range encoded {
			anys[i] = e
		}
		ids, err = b.PublishBatch(ctx, subject, anys, opts...)
	} else if batch && !errors.Is(berr, streams.ErrUnsupported) {
		err = berr
	} else {
		ids = make([]string, 0, len(encoded))
		for _, e := range encoded {
			id, perr := t.m.Publish(ctx, subject, e, opts...)
			if perr != nil {
				err = perr
				break
			}
			ids = append(ids, id)
		}
	}

	if len(values) == 1 && len(ids) == 1 {
		op.Span().SetAttribute("messaging.message.id", ids[0])
	}
	if err == nil {
		op.WrittenRecords(len(ids))
	}
	op.FinishErr(err)
	return ids, err
}

// sameOptions reports whether two publishes can share one batch.
func sameOptions(a, b streams.Options) bool {
	return a.TTL == b.TTL && a.ID == b.ID && a.Group == b.Group && a.Prefetch == b.Prefetch &&
		a.PartitionKey == b.PartitionKey && a.Expires == b.Expires && a.Retain == b.Retain &&
		maps.Equal(a.Headers, b.Headers)
}

// Subscriber receives T from a topic, at most once.
type Subscriber[T any] struct {
	t Topic[T]
}

// Subscribe subscribes with the topic's defaults, opts laid over them, and
// returns a channel of decoded values that closes when ctx is done.
//
// A message that does not decode is skipped, reported as a failed decode,
// rather than ending the subscription: one bad publisher should not stop
// delivery of everyone else's messages.
func (s Subscriber[T]) Subscribe(ctx context.Context, opts ...streams.Option) (<-chan T, error) {
	received, err := s.Receive(ctx, opts...)
	if err != nil {
		return nil, err
	}
	out := make(chan T)
	go func() {
		defer close(out)
		for r := range received {
			select {
			case out <- r.value:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Receive is [Subscriber.Subscribe] keeping each message's context, so the
// work done with it is traced as part of the message's trace.
func (s Subscriber[T]) Receive(ctx context.Context, opts ...streams.Option) (<-chan Received[T], error) {
	t := s.t
	msgs, err := t.m.Subscribe(ctx, t.spec.Subject, t.receiveOptions(opts)...)
	if err != nil {
		return nil, err
	}
	out := make(chan Received[T])
	go func() {
		defer close(out)
		for msg := range msgs {
			mctx, op := t.tel.received(ctx, msg, time.Now(), t.groupAttr())
			v, err := t.decode(mctx, msg)
			if err != nil {
				op.FinishErr(err)
				continue
			}
			select {
			case out <- Received[T]{value: v, raw: msg, ctx: mctx}:
				op.ReadRecords(1)
				op.Finish(telemetry.OutcomeOK)
			case <-ctx.Done():
				op.Finish(telemetry.OutcomeCancelled)
				return
			}
		}
	}()
	return out, nil
}

// groupAttr is the subscription group, as a span attribute.
func (t Topic[T]) groupAttr() map[string]any {
	if t.spec.Group == "" {
		return nil
	}
	return map[string]any{"messaging.consumer.group.name": t.spec.Group}
}

// Received is a decoded message a [Subscriber] handed over.
type Received[T any] struct {
	value T
	raw   streams.Message
	ctx   context.Context
}

// Message is the decoded message.
func (r Received[T]) Message() T { return r.value }

// Raw is the message as it arrived: id, subject, headers, payload.
func (r Received[T]) Raw() streams.Message { return r.raw }

// Context carries the message's process span, to parent the work done with
// it.
func (r Received[T]) Context() context.Context { return r.ctx }

// Consumer receives T from a topic at least once, under a named consumer.
type Consumer[T any] struct {
	t Topic[T]
}

// Name is the consumer name the topic's spec chose, to pass to
// [Consumer.Consume] when there is no reason to pick another.
func (c Consumer[T]) Name() string { return c.t.spec.Consumer }

// Consume delivers messages under consumer name, from where it left off.
//
// It fails with [streams.ErrUnsupported] where the provider keeps no consumer
// positions.
func (c Consumer[T]) Consume(ctx context.Context, name string, opts ...streams.Option) (<-chan Delivery[T], error) {
	durable, err := streams.AsDurable(c.t.m)
	if err != nil {
		return nil, err
	}
	deliveries, err := durable.Consume(ctx, c.t.spec.Subject, name, c.t.receiveOptions(opts)...)
	if err != nil {
		return nil, err
	}
	return c.decodeAll(ctx, name, deliveries), nil
}

// ConsumeFrom delivers messages under consumer name, starting at at.
//
// It fails with [streams.ErrUnsupported] where the provider keeps no log to
// read from.
func (c Consumer[T]) ConsumeFrom(ctx context.Context, name string, at streams.Position, opts ...streams.Option) (<-chan Delivery[T], error) {
	positioned, err := streams.AsPositioned(c.t.m)
	if err != nil {
		return nil, err
	}
	deliveries, err := positioned.ConsumeFrom(ctx, c.t.spec.Subject, name, at, c.t.receiveOptions(opts)...)
	if err != nil {
		return nil, err
	}
	return c.decodeAll(ctx, name, deliveries), nil
}

// decodeAll decodes each delivery, starting its process operation, which the
// delivery's ack or nak finishes.
//
// One that does not decode never will, so it is acknowledged rather than
// redelivered forever: naking it would loop, and leaving it unsettled would
// hold a prefetch slot until the broker gave up on the consumer, then do the
// same to the next one.
func (c Consumer[T]) decodeAll(ctx context.Context, name string, in <-chan streams.Delivery) <-chan Delivery[T] {
	t := c.t
	extra := map[string]any{"messaging.consumer.group.name": name}
	out := make(chan Delivery[T])
	go func() {
		defer close(out)
		for d := range in {
			attrs := maps.Clone(extra)
			attrs["streams.delivery.attempt"] = d.Attempt
			mctx, op := t.tel.received(ctx, d.Message, time.Now(), attrs)
			v, err := t.decode(mctx, d.Message)
			if err != nil {
				op.Span().AddEvent("acknowledged undecodable")
				if aerr := d.Ack(mctx); aerr != nil {
					err = errors.Join(err, aerr)
				}
				op.FinishErr(err)
				continue
			}
			op.ReadRecords(1)
			select {
			case out <- Delivery[T]{value: v, raw: d, ctx: mctx, op: op}:
			case <-ctx.Done():
				op.Finish(telemetry.OutcomeCancelled)
				return
			}
		}
	}()
	return out
}

// Delivery is a decoded message lent by a [Consumer], to settle once handled.
//
// Its process operation runs from receipt until Ack, Nak or NakAfter, so it
// covers the handling, and [Delivery.Context] parents the handler's own spans.
// Leaving it unsettled leaves it to the broker's redelivery, as with the
// [streams.Delivery] it wraps, and leaves its operation open.
type Delivery[T any] struct {
	value T
	raw   streams.Delivery
	ctx   context.Context
	op    *telemetry.Operation
}

// Message is the decoded message.
func (d Delivery[T]) Message() T { return d.value }

// Raw is the message as it arrived: id, subject, headers, payload.
func (d Delivery[T]) Raw() streams.Delivery { return d.raw }

// Context carries the delivery's process span, to parent the handler's work.
func (d Delivery[T]) Context() context.Context { return d.ctx }

// Attempt is how many times it has been delivered, starting at 1; 0 where the
// provider cannot count.
func (d Delivery[T]) Attempt() int { return d.raw.Attempt }

// Ack reports it handled, so it is not delivered again.
func (d Delivery[T]) Ack(ctx context.Context) error {
	return d.settle("ack", d.raw.Ack(ctx))
}

// Nak returns it for redelivery.
func (d Delivery[T]) Nak(ctx context.Context) error {
	return d.settle("nak", d.raw.Nak(ctx))
}

// NakAfter returns it for redelivery no sooner than delay from now, where the
// broker can hold it back; see [streams.Delivery.NakAfter].
func (d Delivery[T]) NakAfter(ctx context.Context, delay time.Duration) error {
	return d.settle("nak_after", d.raw.NakAfter(ctx, delay))
}

// settle finishes the delivery's process operation with how it was settled.
func (d Delivery[T]) settle(how string, err error) error {
	if d.op != nil {
		d.op.Span().SetAttribute("streams.settled", how)
		d.op.FinishErr(err)
	}
	return err
}
