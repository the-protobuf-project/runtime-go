package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/the-protobuf-project/runtime-go/events"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// The header keys every message carries: the CloudEvents attributes, and the
// CloudEvents partitioning extension's partitionkey.
const (
	HeaderSpecVersion  = "ce-specversion"
	HeaderID           = "ce-id"
	HeaderSource       = "ce-source"
	HeaderType         = "ce-type"
	HeaderSubject      = "ce-subject"
	HeaderTime         = "ce-time"
	HeaderPartitionKey = "ce-partitionkey"
	HeaderContentType  = "content-type"
	// HeaderError says why a dead-lettered event could not be published.
	HeaderError = "outbox-error"
)

// maxErrorHeader bounds HeaderError's value.
const maxErrorHeader = 1024

// Options tune a Sink. The zero value of each field is its default.
type Options struct {
	// Subject maps an event type to the subject it is published on. Default:
	// the type itself.
	Subject func(eventType string) string
	// Lanes is how many partition keys publish at once. Default 16.
	Lanes int
	// DeadLetter is a subject an undeliverable event is published to instead
	// of stalling its key. Empty stalls, the default.
	DeadLetter string
	// Source is the ce-source header. Default: the event body's own source.
	Source string
	// MetricPrefix names the metrics: <prefix>.attempts and
	// <prefix>.delivery_lag. Default "stream".
	MetricPrefix string
	// Meter receives the metrics. Nil drops them.
	Meter telemetry.Meter
	// Now is the clock. Default time.Now.
	Now func() time.Time
}

// The dead_lettered outcome: an event its broker refused, sent to the
// dead-letter subject instead. The other outcomes are events'.
const OutcomeDeadLettered = "dead_lettered"

// Sink is an [events.Sink] over a stream. Build one with NewSink.
type Sink struct {
	pub      streams.Publisher
	stream   string
	declared []string
	opts     Options
	attempts telemetry.Counter
	lag      telemetry.Histogram
}

var _ events.Sink = (*Sink)(nil)

// NewSink binds stream on s and checks it declares a subject for every one of
// types, and for the dead-letter subject, so a missing declaration fails here
// rather than stalling events later.
func NewSink(ctx context.Context, s streams.Streams, stream string, types []string, opts Options) (*Sink, error) {
	st, err := s.Get(ctx, stream)
	if err != nil {
		return nil, err
	}
	m, err := s.Bind(ctx, stream)
	if err != nil {
		return nil, err
	}
	sink := newSink(m, st, opts)
	var missing []string
	for _, t := range types {
		if subj := sink.opts.Subject(t); !core.DeclaresPattern(st.Subjects, subj) {
			missing = append(missing, fmt.Sprintf("%s (event type %s)", subj, t))
		}
	}
	if dl := sink.opts.DeadLetter; dl != "" && !core.DeclaresPattern(st.Subjects, dl) {
		missing = append(missing, dl+" (dead letter)")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: stream %s does not declare %v; declare them when the stream is created",
			streams.ErrUnknownSubject, stream, missing)
	}
	return sink, nil
}

// newSink is a Sink over a publisher bound to st, with the defaults applied.
func newSink(pub streams.Publisher, st streams.Stream, opts Options) *Sink {
	if opts.Subject == nil {
		opts.Subject = func(t string) string { return t }
	}
	if opts.Lanes <= 0 {
		opts.Lanes = 16
	}
	if opts.MetricPrefix == "" {
		opts.MetricPrefix = "stream"
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	m := opts.Meter
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &Sink{
		pub: pub, stream: st.ID, declared: st.Subjects, opts: opts,
		attempts: m.Counter(opts.MetricPrefix+".attempts", telemetry.WithUnit("1"),
			telemetry.WithDescription("Events published to the stream, by outcome: succeeded, retrying (failed, will retry), failed (undeliverable) or dead_lettered.")),
		lag: m.Histogram(opts.MetricPrefix+".delivery_lag", telemetry.WithUnit("s"),
			telemetry.WithDescription("From an event's time to its broker acknowledging it."),
			telemetry.WithBuckets(0.5, 1, 2, 5, 10, 30, 60, 300, 1800, 3600, 21600, 86400)),
	}
}

// Deliver publishes batch. Each partition key goes to one lane, a lane
// publishes its events in batch order, and a failure stops that key's later
// events -- they stay unsent, for the dispatcher to offer again in order.
func (s *Sink) Deliver(ctx context.Context, batch []events.Event) ([]bool, error) {
	sent := make([]bool, len(batch))
	lanes := make([][]int, s.opts.Lanes)
	for i := range batch {
		l := lane(batch[i].PartitionKey, s.opts.Lanes)
		lanes[l] = append(lanes[l], i)
	}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, idx := range lanes {
		if len(idx) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			stopped := map[string]bool{}
			for _, i := range idx {
				e := &batch[i]
				if stopped[e.PartitionKey] {
					continue
				}
				err := s.publish(ctx, e)
				if err == nil {
					sent[i] = true
					continue
				}
				stopped[e.PartitionKey] = true
				mu.Lock()
				errs = append(errs, fmt.Errorf("event %s: %w", e.ID, err))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return sent, errors.Join(errs...)
}

// publish sends one event, dead-lettering it when its broker refuses it for
// good and a dead-letter subject is set.
func (s *Sink) publish(ctx context.Context, e *events.Event) error {
	subject := s.opts.Subject(e.Type)
	var err error
	if !core.DeclaresPattern(s.declared, subject) {
		err = fmt.Errorf("%w: stream %s does not declare %s (event type %s)",
			streams.ErrUnknownSubject, s.stream, subject, e.Type)
	} else {
		err = s.send(ctx, subject, e, s.headers(e))
	}
	if err == nil {
		s.attempts.Add(ctx, 1, telemetry.Labels{"outcome": events.OutcomeSucceeded})
		s.lag.Record(ctx, s.opts.Now().Sub(e.Time).Seconds(), telemetry.Labels{})
		return nil
	}
	if !settled(err) {
		s.attempts.Add(ctx, 1, telemetry.Labels{"outcome": events.OutcomeRetrying})
		return err
	}
	if s.opts.DeadLetter == "" {
		s.attempts.Add(ctx, 1, telemetry.Labels{"outcome": events.OutcomeFailed})
		return fmt.Errorf("%w: %w", events.ErrUndeliverable, err)
	}
	h := s.headers(e)
	reason := err.Error()
	if len(reason) > maxErrorHeader {
		reason = reason[:maxErrorHeader]
	}
	h[HeaderError] = oneLine(reason)
	if dlErr := s.send(ctx, s.opts.DeadLetter, e, h); dlErr != nil {
		s.attempts.Add(ctx, 1, telemetry.Labels{"outcome": events.OutcomeRetrying})
		return fmt.Errorf("dead-lettering after %w: %w", err, dlErr)
	}
	s.attempts.Add(ctx, 1, telemetry.Labels{"outcome": OutcomeDeadLettered})
	return nil
}

// send publishes e's body on subject under e's id and partition key.
func (s *Sink) send(ctx context.Context, subject string, e *events.Event, h streams.Headers) error {
	_, err := s.pub.Publish(ctx, subject, json.RawMessage(e.Body),
		streams.ID(e.ID), streams.PartitionKey(e.PartitionKey), streams.WithHeaders(h))
	return err
}

// headers are e's CloudEvent attributes as message headers.
func (s *Sink) headers(e *events.Event) streams.Headers {
	source := s.opts.Source
	if source == "" {
		var body struct {
			Source string `json:"source"`
		}
		_ = json.Unmarshal(e.Body, &body)
		source = body.Source
	}
	h := streams.Headers{
		HeaderSpecVersion:  "1.0",
		HeaderID:           e.ID,
		HeaderType:         e.Type,
		HeaderSubject:      e.Subject,
		HeaderTime:         e.Time.UTC().Format(time.RFC3339Nano),
		HeaderPartitionKey: e.PartitionKey,
		HeaderContentType:  events.ContentType,
	}
	if source != "" {
		h[HeaderSource] = source
	}
	return h
}

// settled reports a failure no retry fixes: the contract's settled errors.
func settled(err error) bool {
	return errors.Is(err, streams.ErrUnknownSubject) || errors.Is(err, streams.ErrUnsupported) ||
		errors.Is(err, streams.ErrInvalidHeader)
}

// lane is the lane key's events publish on.
func lane(key string, lanes int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(lanes))
}

// oneLine replaces line breaks, which a header value cannot hold.
func oneLine(s string) string {
	out := []rune(s)
	for i, r := range out {
		if r < 0x20 || r == 0x7f {
			out[i] = ' '
		}
	}
	return string(out)
}
