package streams

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// SentAtHeader carries the time a message was handed to the broker, RFC 3339
// with nanoseconds, UTC. A receiver subtracts it from its own clock to report
// how long the message was in transit; across machines that includes the skew
// between their clocks, so it is exact only on one host.
const SentAtHeader = "sent-at"

// Instrument returns provider reporting every operation through signals, as
// backend ("nats", "kafka", "rabbitmq"), and the zero Signals returns provider
// as it is.
//
// Every call is a [telemetry.Operation] of component "streams": create, get,
// update, delete, list and bind on the provider, and on each manager it binds:
//
//	streams.publish        producer  a publish, to the broker's acknowledgement
//	streams.publish_batch  producer  a batch, its record count written
//	streams.subscribe      client    opening a subscription
//	streams.consume        client    opening a durable consumer
//	streams.receive        consumer  one message, receipt to hand-off
//	streams.deliver        consumer  one delivery, receipt to ack or nak
//
// A publish stamps the message's headers with its trace context and the send
// time ([SentAtHeader]); a receive or deliver continues that trace, so it is
// the publish's child in the receiving program, in whatever language, and
// records streams.transit.duration. [Message.Context] carries the receiving
// span to the caller. A delivery's span stays open until it is settled, so it
// covers the handling.
//
// The bound managers keep every capability the provider's have: [AsDurable],
// [AsPositioned] and [AsBatch] find them through the wrapper, instrumented.
func Instrument(provider Streams, signals telemetry.Signals, backend string) Streams {
	if !signals.Enabled() {
		return provider
	}
	meter := signals.Meter
	if meter == nil {
		meter = telemetry.NoopMeter
	}
	return &instrumented{next: provider, ins: &instruments{
		obs:     signals.Observer("streams"),
		backend: backend,
		transit: meter.Histogram("streams.transit.duration",
			telemetry.WithDescription("Time from a message's sent-at header to its receipt"), telemetry.WithUnit("s")),
	}}
}

// instruments are what every wrapper of one provider reports through.
type instruments struct {
	obs     *telemetry.Observer
	backend string
	transit telemetry.Histogram
}

func (in *instruments) start(ctx context.Context, operation string, kind telemetry.SpanKind, attrs map[string]any, opts ...telemetry.SpanOption) (context.Context, *telemetry.Operation) {
	all := map[string]any{"messaging.system": in.backend}
	for k, v := range attrs {
		all[k] = v
	}
	return in.obs.Start(ctx, in.backend, operation, kind, append(opts, telemetry.WithSpanAttributes(all))...)
}

// stamp is the header set a publish under ctx carries: its trace context and
// the send time.
func (in *instruments) stamp(ctx context.Context) Option {
	h := Headers{SentAtHeader: time.Now().UTC().Format(time.RFC3339Nano)}
	in.obs.Tracer().Inject(ctx, h)
	return WithHeaders(h)
}

// received starts the span of msg arriving through operation, as the child of
// the publish its headers carry, and records its transit.
func (in *instruments) received(ctx context.Context, operation string, msg Message, extra map[string]any) (context.Context, *telemetry.Operation) {
	now := time.Now()
	attrs := map[string]any{
		"messaging.destination.name":  msg.Subject,
		"messaging.message.id":        msg.ID,
		"messaging.message.body.size": len(msg.Data),
		"streams.codec":               msg.CodecName(),
	}
	for k, v := range extra {
		attrs[k] = v
	}
	if sent, err := time.Parse(time.RFC3339Nano, msg.Headers.Get(SentAtHeader)); err == nil {
		if transit := now.Sub(sent); transit >= 0 {
			in.transit.Record(ctx, transit.Seconds(), telemetry.Labels{"subject": msg.Subject, "backend": in.backend})
			attrs["streams.transit.duration"] = transit.Seconds()
		}
	}
	parent := in.obs.Tracer().Extract(ctx, msg.Headers)
	return in.start(parent, operation, telemetry.SpanKindConsumer, attrs, telemetry.WithStartTime(now))
}

// --- the provider --------------------------------------------------------------

type instrumented struct {
	next Streams
	ins  *instruments
}

func (s *instrumented) admin(ctx context.Context, operation, id string) (context.Context, *telemetry.Operation) {
	return s.ins.start(ctx, operation, telemetry.SpanKindClient, map[string]any{"streams.stream": id})
}

func (s *instrumented) Create(ctx context.Context, in Stream) (Stream, error) {
	ctx, op := s.admin(ctx, "create", in.ID)
	out, err := s.next.Create(ctx, in)
	op.FinishErr(err)
	return out, err
}

func (s *instrumented) Get(ctx context.Context, id string) (Stream, error) {
	ctx, op := s.admin(ctx, "get", id)
	out, err := s.next.Get(ctx, id)
	finishLookup(op, err)
	return out, err
}

func (s *instrumented) Bind(ctx context.Context, id string) (Manager, error) {
	ctx, op := s.admin(ctx, "bind", id)
	m, err := s.next.Bind(ctx, id)
	finishLookup(op, err)
	if err != nil {
		return nil, err
	}
	return &instrumentedManager{next: m, ins: s.ins}, nil
}

func (s *instrumented) Update(ctx context.Context, id string, in Stream) (Stream, error) {
	ctx, op := s.admin(ctx, "update", id)
	out, err := s.next.Update(ctx, id, in)
	finishLookup(op, err)
	return out, err
}

func (s *instrumented) Delete(ctx context.Context, id string) error {
	ctx, op := s.admin(ctx, "delete", id)
	err := s.next.Delete(ctx, id)
	finishLookup(op, err)
	return err
}

func (s *instrumented) List(ctx context.Context) ([]Stream, error) {
	ctx, op := s.admin(ctx, "list", "")
	out, err := s.next.List(ctx)
	if err == nil {
		op.ReadRecords(len(out))
	}
	op.FinishErr(err)
	return out, err
}

// Close closes the provider when it holds something to close.
func (s *instrumented) Close() error {
	if c, ok := s.next.(Closer); ok {
		return c.Close()
	}
	return nil
}

// finishLookup finishes op, a missing stream being an expected outcome rather
// than a failure.
func finishLookup(op *telemetry.Operation, err error) {
	if classify(err) == telemetry.OutcomeNotFound {
		op.Finish(telemetry.OutcomeNotFound)
		return
	}
	op.FinishErr(err)
}

// classify is the outcome of a streams error.
func classify(err error) telemetry.Outcome {
	switch {
	case err == nil:
		return telemetry.OutcomeOK
	case errors.Is(err, ErrNotFound):
		return telemetry.OutcomeNotFound
	case errors.Is(err, ErrUnsupported):
		return telemetry.OutcomeUnsupported
	case errors.Is(err, ErrUnknownSubject), errors.Is(err, ErrInvalidHeader):
		return telemetry.OutcomeInvalid
	default:
		return telemetry.OutcomeOf(err)
	}
}

// finish finishes op with err classified as a streams error.
func finish(op *telemetry.Operation, err error) {
	if err != nil && classify(err).IsError() {
		op.Span().SetError(err)
	}
	op.Finish(classify(err))
}

// --- a bound manager -----------------------------------------------------------

type instrumentedManager struct {
	next Manager
	ins  *instruments
}

func (m *instrumentedManager) Publish(ctx context.Context, subject string, value any, opts ...Option) (string, error) {
	ctx, op := m.ins.start(ctx, "publish", telemetry.SpanKindProducer, map[string]any{"messaging.destination.name": subject})
	id, err := m.next.Publish(ctx, subject, value, append(opts, m.ins.stamp(ctx))...)
	if err == nil {
		op.Span().SetAttribute("messaging.message.id", id)
		op.WrittenRecords(1)
	}
	finish(op, err)
	return id, err
}

func (m *instrumentedManager) Subscribe(ctx context.Context, subject string, opts ...Option) (<-chan Message, error) {
	sctx, op := m.ins.start(ctx, "subscribe", telemetry.SpanKindClient, map[string]any{"messaging.destination.name": subject})
	in, err := m.next.Subscribe(sctx, subject, opts...)
	finish(op, err)
	if err != nil {
		return nil, err
	}
	group := map[string]any{}
	if g := NewOptions(opts...).Group; g != "" {
		group["messaging.consumer.group.name"] = g
	}
	out := make(chan Message)
	go func() {
		defer close(out)
		for msg := range in {
			mctx, rop := m.ins.received(ctx, "receive", msg, group)
			msg.ctx = mctx
			select {
			case out <- msg:
				rop.ReadRecords(1)
				rop.Finish(telemetry.OutcomeOK)
			case <-ctx.Done():
				rop.Finish(telemetry.OutcomeCancelled)
				return
			}
		}
	}()
	return out, nil
}

// capability hands [AsDurable], [AsPositioned] and [AsBatch] the instrumented
// half the wrapped manager has, if it has it.
func (m *instrumentedManager) capability(c Capability) (any, bool) {
	switch c {
	case CapabilityDurable:
		if d, ok := m.next.(Durable); ok {
			return instrumentedDurable{m: m, next: d}, true
		}
	case CapabilityPositioned:
		if p, ok := m.next.(Positioned); ok {
			return instrumentedPositioned{m: m, next: p}, true
		}
	case CapabilityBatch:
		if b, ok := m.next.(Batch); ok {
			return instrumentedBatch{m: m, next: b}, true
		}
	}
	return nil, false
}

type instrumentedBatch struct {
	m    *instrumentedManager
	next Batch
}

func (b instrumentedBatch) PublishBatch(ctx context.Context, subject string, values []any, opts ...Option) ([]string, error) {
	ctx, op := b.m.ins.start(ctx, "publish_batch", telemetry.SpanKindProducer, map[string]any{
		"messaging.destination.name": subject, "messaging.batch.message_count": len(values),
	})
	ids, err := b.next.PublishBatch(ctx, subject, values, append(opts, b.m.ins.stamp(ctx))...)
	if err == nil {
		op.WrittenRecords(len(ids))
	}
	finish(op, err)
	return ids, err
}

type instrumentedDurable struct {
	m    *instrumentedManager
	next Durable
}

func (d instrumentedDurable) Consume(ctx context.Context, subject, consumer string, opts ...Option) (<-chan Delivery, error) {
	cctx, op := d.m.ins.start(ctx, "consume", telemetry.SpanKindClient, map[string]any{
		"messaging.destination.name": subject, "messaging.consumer.group.name": consumer,
	})
	in, err := d.next.Consume(cctx, subject, consumer, opts...)
	finish(op, err)
	if err != nil {
		return nil, err
	}
	return d.m.deliveries(ctx, consumer, in), nil
}

type instrumentedPositioned struct {
	m    *instrumentedManager
	next Positioned
}

func (p instrumentedPositioned) ConsumeFrom(ctx context.Context, subject, consumer string, at Position, opts ...Option) (<-chan Delivery, error) {
	cctx, op := p.m.ins.start(ctx, "consume", telemetry.SpanKindClient, map[string]any{
		"messaging.destination.name": subject, "messaging.consumer.group.name": consumer, "streams.position": int(at),
	})
	in, err := p.next.ConsumeFrom(cctx, subject, consumer, at, opts...)
	finish(op, err)
	if err != nil {
		return nil, err
	}
	return p.m.deliveries(ctx, consumer, in), nil
}

// deliveries starts each delivery's span and hands it on with an acker that
// finishes the span when the delivery is settled.
func (m *instrumentedManager) deliveries(ctx context.Context, consumer string, in <-chan Delivery) <-chan Delivery {
	out := make(chan Delivery)
	go func() {
		defer close(out)
		for d := range in {
			dctx, op := m.ins.received(ctx, "deliver", d.Message, map[string]any{
				"messaging.consumer.group.name": consumer, "streams.delivery.attempt": d.Attempt,
			})
			op.ReadRecords(1)
			d.ctx = dctx
			d.acker = settling(d.acker, op)
			select {
			case out <- d:
			case <-ctx.Done():
				op.Finish(telemetry.OutcomeCancelled)
				return
			}
		}
	}()
	return out
}

// settling wraps a delivery's acker so settling it finishes op, keeping the
// broker's delayed redelivery where it has one.
func settling(next Acker, op *telemetry.Operation) Acker {
	base := settlingAcker{next: next, op: op}
	if da, ok := next.(DelayedAcker); ok {
		return settlingDelayedAcker{settlingAcker: base, delayed: da}
	}
	return base
}

type settlingAcker struct {
	next Acker
	op   *telemetry.Operation
}

func (a settlingAcker) settle(how string, err error) error {
	a.op.Span().SetAttribute("streams.settled", how)
	finish(a.op, err)
	return err
}

func (a settlingAcker) Ack(ctx context.Context) error {
	if a.next == nil {
		return a.settle("ack", nil)
	}
	return a.settle("ack", a.next.Ack(ctx))
}

func (a settlingAcker) Nak(ctx context.Context) error {
	if a.next == nil {
		return a.settle("nak", nil)
	}
	return a.settle("nak", a.next.Nak(ctx))
}

type settlingDelayedAcker struct {
	settlingAcker
	delayed DelayedAcker
}

func (a settlingDelayedAcker) NakAfter(ctx context.Context, delay time.Duration) error {
	return a.settle("nak_after", a.delayed.NakAfter(ctx, delay))
}

func (a settlingDelayedAcker) MaxNakDelay() time.Duration { return a.delayed.MaxNakDelay() }

var (
	_ Streams      = (*instrumented)(nil)
	_ Closer       = (*instrumented)(nil)
	_ Manager      = (*instrumentedManager)(nil)
	_ Durable      = instrumentedDurable{}
	_ Positioned   = instrumentedPositioned{}
	_ Batch        = instrumentedBatch{}
	_ DelayedAcker = settlingDelayedAcker{}
)
