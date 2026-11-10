package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/events"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// published is one message the fake broker accepted.
type published struct {
	subject string
	opts    streams.Options
	body    json.RawMessage
}

// broker is a streams provider in memory: one stream, a log of what was
// published, and scripted failures by message id.
type broker struct {
	mu     sync.Mutex
	stream streams.Stream
	log    []published
	fail   map[string]error
	delay  time.Duration
}

func newBroker(subjects ...string) *broker {
	return &broker{stream: streams.Stream{ID: "orders", Subjects: subjects}, fail: map[string]error{}}
}

func (b *broker) Create(context.Context, streams.Stream) (streams.Stream, error) {
	return b.stream, nil
}

func (b *broker) Get(_ context.Context, id string) (streams.Stream, error) {
	if id != b.stream.ID {
		return streams.Stream{}, streams.ErrNotFound
	}
	return b.stream, nil
}

func (b *broker) Bind(ctx context.Context, id string) (streams.Manager, error) {
	if _, err := b.Get(ctx, id); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *broker) Update(context.Context, string, streams.Stream) (streams.Stream, error) {
	return b.stream, nil
}
func (b *broker) Delete(context.Context, string) error           { return nil }
func (b *broker) List(context.Context) ([]streams.Stream, error) { return nil, nil }
func (b *broker) Subscribe(context.Context, string, ...streams.Option) (<-chan streams.Message, error) {
	return nil, streams.ErrUnsupported
}

func (b *broker) Publish(_ context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	o := streams.NewOptions(opts...)
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if subject != "dead" {
		if err := b.fail[o.ID]; err != nil {
			return "", err
		}
	}
	b.log = append(b.log, published{subject: subject, opts: o, body: value.(json.RawMessage)})
	return o.ID, nil
}

func (b *broker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.log))
	for i := range b.log {
		out[i] = b.log[i].opts.ID
	}
	return out
}

func event(id, key, typ string) events.Event {
	at := time.Date(2026, 11, 9, 10, 0, 0, 500, time.UTC)
	body, _ := events.CloudEvent(id, "//shop.example.com", typ, "orders/"+key, at, []byte(`{"total":7}`))
	return events.Event{ID: id, Type: typ, Subject: "orders/" + key, Time: at, PartitionKey: key, Body: []byte(body)}
}

// Every event goes out under its own id, on its type's subject, keyed by its
// partition key, with the CloudEvent unchanged and its attributes as headers.
func TestSinkPublishesTheContract(t *testing.T) {
	b := newBroker("shop.order.created")
	s, err := NewSink(t.Context(), b, "orders", []string{"shop.order.created"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	e := event("01A", "agg-1", "shop.order.created")
	sent, err := s.Deliver(t.Context(), []events.Event{e})
	if err != nil || !sent[0] {
		t.Fatalf("Deliver = %v, %v", sent, err)
	}
	p := b.log[0]
	if p.subject != "shop.order.created" || p.opts.ID != "01A" || p.opts.PartitionKey != "agg-1" || string(p.body) != string(e.Body) {
		t.Fatalf("published %+v", p)
	}
	want := streams.Headers{
		"ce-specversion": "1.0", "ce-id": "01A", "ce-source": "//shop.example.com", "ce-type": "shop.order.created",
		"ce-subject": "orders/agg-1", "ce-time": "2026-11-09T10:00:00.0000005Z", "ce-partitionkey": "agg-1",
		"content-type": events.ContentType,
	}
	if len(p.opts.Headers) != len(want) {
		t.Fatalf("headers %v, want %v", p.opts.Headers, want)
	}
	for k, v := range want {
		if p.opts.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, p.opts.Headers[k], v)
		}
	}
	if err := p.opts.Headers.Validate(); err != nil {
		t.Errorf("headers a broker would refuse: %v", err)
	}
}

// A stream that does not declare a subject for every type fails at NewSink.
func TestNewSinkChecksTheDeclaredSubjects(t *testing.T) {
	b := newBroker("shop.order.created")
	_, err := NewSink(t.Context(), b, "orders", []string{"shop.order.created", "shop.order.paid"}, Options{})
	if !errors.Is(err, streams.ErrUnknownSubject) {
		t.Fatalf("NewSink = %v, want ErrUnknownSubject for shop.order.paid", err)
	}
	if _, err := NewSink(t.Context(), b, "orders", nil, Options{DeadLetter: "dead"}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Fatalf("an undeclared dead-letter subject: %v", err)
	}
	// Patterns count, and Subject maps types onto them.
	b = newBroker("shop.order.*")
	if _, err := NewSink(t.Context(), b, "orders", []string{"shop.order.paid"}, Options{}); err != nil {
		t.Fatalf("a pattern declaring the subject: %v", err)
	}
	b = newBroker("shop.order.events")
	opts := Options{Subject: func(string) string { return "shop.order.events" }}
	if _, err := NewSink(t.Context(), b, "orders", []string{"shop.order.paid", "shop.order.created"}, opts); err != nil {
		t.Fatalf("every type on one subject: %v", err)
	}
}

// A failure stops its key's later events; other keys go on; one key's events
// keep their order however many lanes run.
func TestSinkKeepsOrderPerKey(t *testing.T) {
	b := newBroker("t")
	b.delay = time.Millisecond
	s, err := NewSink(t.Context(), b, "orders", []string{"t"}, Options{Lanes: 4})
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]events.Event, 0, 60)
	for i := range 60 {
		batch = append(batch, event(fmt.Sprintf("%02d", i), fmt.Sprintf("k%d", i%6), "t"))
	}
	b.fail["07"] = errors.New("broker down") // key k1
	sent, err := s.Deliver(t.Context(), batch)
	if err == nil || errors.Is(err, events.ErrUndeliverable) {
		t.Fatalf("Deliver = %v, want a transient failure", err)
	}
	for i, e := range batch {
		blocked := e.PartitionKey == "k1" && i >= 7
		if sent[i] == blocked {
			t.Fatalf("event %s (key %s) sent = %v", e.ID, e.PartitionKey, sent[i])
		}
	}
	last := map[string]string{}
	for _, p := range b.log {
		if prev := last[p.opts.PartitionKey]; prev > p.opts.ID {
			t.Fatalf("key %s published %s after %s", p.opts.PartitionKey, p.opts.ID, prev)
		}
		last[p.opts.PartitionKey] = p.opts.ID
	}
}

// A broker's settled refusal is undeliverable, or dead-lettered when a
// dead-letter subject is set.
func TestSinkUndeliverableAndDeadLetter(t *testing.T) {
	b := newBroker("t", "dead")
	b.fail["01A"] = fmt.Errorf("%w: too large", streams.ErrUnsupported)
	s, err := NewSink(t.Context(), b, "orders", []string{"t"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := s.Deliver(t.Context(), []events.Event{event("01A", "a", "t")})
	if sent[0] || !errors.Is(err, events.ErrUndeliverable) {
		t.Fatalf("Deliver = %v, %v; want ErrUndeliverable", sent, err)
	}

	s, err = NewSink(t.Context(), b, "orders", []string{"t"}, Options{DeadLetter: "dead"})
	if err != nil {
		t.Fatal(err)
	}
	sent, err = s.Deliver(t.Context(), []events.Event{event("01A", "a", "t")})
	if err != nil || !sent[0] {
		t.Fatalf("Deliver with a dead letter = %v, %v", sent, err)
	}
	p := b.log[len(b.log)-1]
	if p.subject != "dead" || p.opts.ID != "01A" || p.opts.Headers[HeaderType] != "t" ||
		p.opts.Headers[HeaderError] == "" || p.opts.Headers.Validate() != nil {
		t.Fatalf("dead letter %+v", p)
	}

	// A type the stream does not declare, met after NewSink, is undeliverable.
	sent, err = s.Deliver(t.Context(), []events.Event{event("01B", "a", "other")})
	if err != nil || !sent[0] || b.log[len(b.log)-1].subject != "dead" {
		t.Fatalf("an undeclared type: %v, %v, %v", sent, err, b.ids())
	}
}

func TestSinkTwoTypesOfOneKeyStayInOrder(t *testing.T) {
	b := newBroker("a", "b")
	s, err := NewSink(t.Context(), b, "orders", []string{"a", "b"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	batch := []events.Event{event("1", "k", "a"), event("2", "k", "b"), event("3", "k", "a")}
	if _, err := s.Deliver(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	if got := b.ids(); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("published %v", got)
	}
}
