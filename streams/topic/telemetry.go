package topic

import (
	"context"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// SentAtHeader carries the time a message was handed to the broker; see
// [streams.SentAtHeader].
const SentAtHeader = streams.SentAtHeader

// Telemetry is what topics report through: every step of a message as a
// [telemetry.Operation] of component "streams", and the two measurements that
// are not operations.
//
// One message's trace, across both programs:
//
//	streams.send     producer   publish call to the broker's acknowledgement
//	  streams.encode
//	streams.process  consumer   receipt to hand-off (Subscriber) or to ack/nak (Consumer)
//	  streams.decode
//
// The send span's context travels in the message's headers, through the
// tracer's Inject and Extract, so process is send's child in the receiving
// program, whatever language it is written in.
//
// Over a provider wrapped by [streams.Instrument], the provider's own spans
// sit between: send holds encode and streams.publish, and the provider's
// streams.receive or streams.deliver takes the place of process, with decode
// its child. A topic never starts a second span for a message the provider
// already traced. Each step is also a share of
// runtime.operation.count and runtime.operation.duration, labeled component,
// backend, operation and outcome, so encode and decode time, send time and
// handling time are each one query. Beside them:
//
//	streams.transit.duration  s   sent-at to receipt
//	streams.message.size      By  encoded size, sent and received
//
// Make one per program and broker and hand it to every topic: its instruments
// are built once, here.
type Telemetry struct {
	obs     *telemetry.Observer
	backend string
	transit telemetry.Histogram
	size    telemetry.Histogram
}

// NewTelemetry returns topic telemetry reporting through signals for the
// broker backend names ("nats", "kafka", "rabbitmq"). The zero Signals
// reports nothing.
func NewTelemetry(signals telemetry.Signals, backend string) *Telemetry {
	meter := signals.Meter
	if meter == nil {
		meter = telemetry.NoopMeter
	}
	return &Telemetry{
		obs:     signals.Observer("streams"),
		backend: backend,
		transit: meter.Histogram("streams.transit.duration",
			telemetry.WithDescription("Time from a message's sent-at header to its receipt"), telemetry.WithUnit("s")),
		size: meter.Histogram("streams.message.size",
			telemetry.WithDescription("Encoded size of one message"), telemetry.WithUnit("By")),
	}
}

// noTelemetry is what a topic made without [Topic.WithTelemetry] reports
// through: nothing.
var noTelemetry = NewTelemetry(telemetry.Signals{}, "")

// start begins one operation of subject.
func (t *Telemetry) start(ctx context.Context, operation, subject, codec string, kind telemetry.SpanKind, opts ...telemetry.SpanOption) (context.Context, *telemetry.Operation) {
	attrs := map[string]any{"messaging.destination.name": subject, "streams.codec": codec}
	if t.backend != "" {
		attrs["messaging.system"] = t.backend
	}
	return t.obs.Start(ctx, t.backend, operation, kind, append(opts, telemetry.WithSpanAttributes(attrs))...)
}

// stamp writes ctx's trace context and the send time into a fresh header set.
func (t *Telemetry) stamp(ctx context.Context, now time.Time) streams.Headers {
	h := streams.Headers{SentAtHeader: now.UTC().Format(time.RFC3339Nano)}
	t.obs.Tracer().Inject(ctx, h)
	return h
}

// sized records an encoded size.
func (t *Telemetry) sized(ctx context.Context, subject, direction string, n int) {
	t.size.Record(ctx, float64(n), telemetry.Labels{"subject": subject, "direction": direction})
}

// received starts the process operation of msg, a child of the send its
// headers carry, at now, and records how long it was in transit. A message an
// instrumented provider already traced gets no operation of its own: the
// returned one reports nothing, and the context is the provider's span.
func (t *Telemetry) received(ctx context.Context, msg streams.Message, now time.Time, extra map[string]any) (context.Context, *telemetry.Operation) {
	if msg.Traced() {
		_, op := noTelemetry.start(ctx, "process", msg.Subject, msg.CodecName(), telemetry.SpanKindConsumer)
		return msg.Context(), op
	}
	parent := t.obs.Tracer().Extract(ctx, msg.Headers)
	attrs := map[string]any{
		"messaging.message.id":        msg.ID,
		"messaging.message.body.size": len(msg.Data),
	}
	for k, v := range extra {
		attrs[k] = v
	}
	if sent, err := time.Parse(time.RFC3339Nano, msg.Headers.Get(SentAtHeader)); err == nil {
		if transit := now.Sub(sent); transit >= 0 {
			t.transit.Record(ctx, transit.Seconds(), telemetry.Labels{"subject": msg.Subject})
			attrs["streams.transit.duration"] = transit.Seconds()
		}
	}
	t.sized(ctx, msg.Subject, "receive", len(msg.Data))
	return t.start(parent, "process", msg.Subject, msg.CodecName(), telemetry.SpanKindConsumer,
		telemetry.WithStartTime(now), telemetry.WithSpanAttributes(attrs))
}
