package topic_test

// Typed topics over an in-memory provider: bind, publish, subscribe, consume,
// batch, and the codec and option plumbing in between.
//
// The provider fans each publish out to every subscriber and consumer of its
// subject and records the options it was given, so a test can see what a
// topic's spec put on the wire without a broker. Its capabilities are
// switches, so each test picks the provider it needs: durable or not, native
// batch or not. It mirrors runtime-rs's tests/topic.rs.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/streams/topic"
)

// --- messages ----------------------------------------------------------------

type Customer struct {
	Tenant string `json:"tenant"`
}

type Order struct {
	OrderID  string    `json:"order_id"`
	Customer *Customer `json:"customer,omitempty"`
	Quantity int       `json:"quantity"`
}

func order(id, tenant string) Order {
	return Order{OrderID: id, Customer: &Customer{Tenant: tenant}, Quantity: 1}
}

// --- specs, as protoc-gen-streams writes them ---------------------------------

func orderID(o Order) (string, bool) { return o.OrderID, true }

func tenant(o Order) (string, bool) {
	if o.Customer == nil {
		return "", false
	}
	return o.Customer.Tenant, true
}

func placed() *topic.Spec[Order] {
	return &topic.Spec[Order]{
		Subject:      "order.placed",
		Consumer:     "billing",
		Group:        "checkout",
		Headers:      streams.Headers{"source": "checkout"},
		HeaderFields: []topic.HeaderField[Order]{{Header: "tenant", Field: tenant}},
		PartitionKey: orderID,
		Expiry:       time.Minute,
		Prefetch:     32,
		Batch:        topic.BatchPolicy{MaxSize: 100, MaxDelay: 10 * time.Millisecond},
	}
}

func shipped() *topic.Spec[Order] {
	return &topic.Spec[Order]{
		Subject:  "order.shipped",
		Consumer: "shipping",
		Batch:    topic.BatchPolicy{MaxSize: 3, MaxDelay: 20 * time.Millisecond},
	}
}

func declaration() streams.Stream {
	return streams.Stream{ID: "order", Name: "order", Subjects: []string{"order.placed", "order.shipped"}}
}

// --- the provider -------------------------------------------------------------

type call struct {
	subject string
	codec   string
	data    []byte
	opts    streams.Options
}

type memory struct {
	durable, batch bool
	codec          streams.Codec

	mu        sync.Mutex
	declared  map[string]streams.Stream
	published []call
	received  []streams.Options
	batches   int
	subs      map[string][]chan streams.Message
	consumers map[string][]chan streams.Delivery
	nextID    int

	acks, naks atomic.Int32
}

func newMemory(durable, batch bool) *memory {
	return &memory{
		durable: durable, batch: batch, codec: streams.JSON,
		declared:  map[string]streams.Stream{},
		subs:      map[string][]chan streams.Message{},
		consumers: map[string][]chan streams.Delivery{},
	}
}

func (m *memory) Create(_ context.Context, s streams.Stream) (streams.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.declared[s.ID] = s
	return s, nil
}

func (m *memory) Get(_ context.Context, id string) (streams.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.declared[id]
	if !ok {
		return streams.Stream{}, fmt.Errorf("memory: stream %s: %w", id, streams.ErrNotFound)
	}
	return s, nil
}

func (m *memory) Bind(ctx context.Context, id string) (streams.Manager, error) {
	if _, err := m.Get(ctx, id); err != nil {
		return nil, err
	}
	switch {
	case m.durable && m.batch:
		return both{durable{plain{m}}}, nil
	case m.durable:
		return durable{plain{m}}, nil
	case m.batch:
		return batched{plain{m}}, nil
	default:
		return plain{m}, nil
	}
}

func (m *memory) Update(_ context.Context, id string, s streams.Stream) (streams.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.declared[id] = s
	return s, nil
}

func (m *memory) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.declared, id)
	return nil
}

func (m *memory) List(context.Context) ([]streams.Stream, error) { return nil, nil }

// deliver hands data to every subscriber and consumer of subject.
// deliver frames value as a provider would, headers in the frame, and hands
// what a provider would read back to every subscriber and consumer of subject.
func (m *memory) deliver(subject string, value any, headers streams.Headers) (string, error) {
	m.nextID++
	id := fmt.Sprintf("m-%d", m.nextID)
	frame, err := core.PackWith(m.codec, id, value, headers)
	if err != nil {
		return "", err
	}
	msg, err := core.Unpack(streams.NewRegistry(m.codec), subject, frame)
	if err != nil {
		return "", err
	}
	for _, c := range m.subs[subject] {
		c <- msg
	}
	for _, c := range m.consumers[subject] {
		c <- streams.NewDelivery(msg, 1, acker{m})
	}
	return id, nil
}

func (m *memory) publish(subject string, value any, opts ...streams.Option) (string, error) {
	o := streams.NewOptions(opts...)
	m.mu.Lock()
	defer m.mu.Unlock()
	c := call{subject: subject, opts: o}
	if e, ok := value.(streams.Encoded); ok {
		c.codec, c.data = e.Codec, e.Data
	}
	m.published = append(m.published, c)
	return m.deliver(subject, value, o.Headers)
}

// inject delivers raw bytes under codec as if some other publisher had sent
// them.
func (m *memory) inject(subject, codec string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, _ = m.deliver(subject, streams.Encoded{Codec: codec, Data: data}, nil)
}

func (m *memory) subscribe(subject string, opts ...streams.Option) chan streams.Message {
	c := make(chan streams.Message, 64)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, streams.NewOptions(opts...))
	m.subs[subject] = append(m.subs[subject], c)
	return c
}

func (m *memory) consume(subject string, opts ...streams.Option) chan streams.Delivery {
	c := make(chan streams.Delivery, 64)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, streams.NewOptions(opts...))
	m.consumers[subject] = append(m.consumers[subject], c)
	return c
}

func (m *memory) publishBatch(subject string, values []any, opts ...streams.Option) ([]string, error) {
	m.mu.Lock()
	m.batches++
	m.mu.Unlock()
	ids := make([]string, 0, len(values))
	for _, v := range values {
		id, err := m.publish(subject, v, opts...)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (m *memory) calls() []call {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.published)
}

type acker struct{ m *memory }

func (a acker) Ack(context.Context) error { a.m.acks.Add(1); return nil }
func (a acker) Nak(context.Context) error { a.m.naks.Add(1); return nil }

// plain publishes and subscribes, and nothing else.
type plain struct{ m *memory }

func (p plain) Publish(_ context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	return p.m.publish(subject, value, opts...)
}

func (p plain) Subscribe(_ context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	return p.m.subscribe(subject, opts...), nil
}

type durable struct{ plain }

func (d durable) Consume(_ context.Context, subject, _ string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	return d.m.consume(subject, opts...), nil
}

type batched struct{ plain }

func (b batched) PublishBatch(_ context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	return b.m.publishBatch(subject, values, opts...)
}

type both struct{ durable }

func (b both) PublishBatch(_ context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	return b.m.publishBatch(subject, values, opts...)
}

// bind declares the order stream on m and binds it.
func bind(t *testing.T, m *memory, caps ...streams.Capability) streams.Manager {
	t.Helper()
	mgr, err := topic.Bind(t.Context(), m, declaration(), caps...)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	return mgr
}

func receive[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v, ok := <-c:
		if !ok {
			t.Fatal("channel closed")
		}
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("nothing received")
	}
	var zero T
	return zero
}

// --- tests ---------------------------------------------------------------------

func TestPublishCarriesTheSpecsKeyAndHeaders(t *testing.T) {
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed())

	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	if _, err := orders.Publisher().Publish(t.Context(), order("o-2", "acme"),
		streams.PartitionKey("override"), streams.Header("source", "replay")); err != nil {
		t.Fatal(err)
	}

	calls := m.calls()
	if len(calls) != 2 {
		t.Fatalf("published %d, want 2", len(calls))
	}
	first := calls[0].opts
	if calls[0].subject != "order.placed" || first.PartitionKey != "o-1" || first.Expires != time.Minute {
		t.Errorf("first publish: subject %q key %q expires %v", calls[0].subject, first.PartitionKey, first.Expires)
	}
	if first.Headers.Get("source") != "checkout" || first.Headers.Get("tenant") != "acme" {
		t.Errorf("first publish headers: %v", first.Headers)
	}
	second := calls[1].opts
	if second.PartitionKey != "override" || second.Headers.Get("source") != "replay" || second.Headers.Get("tenant") != "acme" {
		t.Errorf("a call's options should win over the spec's: key %q headers %v", second.PartitionKey, second.Headers)
	}
}

func TestAFieldOnAnUnsetMessageLeavesItsHeaderOff(t *testing.T) {
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed())

	if _, err := orders.Publisher().Publish(t.Context(), Order{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	headers := m.calls()[0].opts.Headers
	if _, ok := headers["tenant"]; ok {
		t.Errorf("tenant header sent for an order without a customer: %v", headers)
	}
}

func TestBindFailsWithoutARequiredCapability(t *testing.T) {
	m := newMemory(false, false)
	_, err := topic.Bind(t.Context(), m, declaration(), streams.CapabilityDurable)
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

func TestBindChecksEveryRequiredCapability(t *testing.T) {
	m := newMemory(true, false)
	if _, err := topic.Bind(t.Context(), m, declaration(), streams.CapabilityDurable); err != nil {
		t.Fatalf("durable alone: %v", err)
	}
	_, err := topic.Bind(t.Context(), m, declaration(), streams.CapabilityDurable, streams.CapabilityBatch)
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("durable and batch on a provider without batch: got %v", err)
	}
}

func TestBindCreatesAStreamThatDoesNotExist(t *testing.T) {
	m := newMemory(false, false)
	bind(t, m)
	s, err := m.Get(t.Context(), "order")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.Subjects, declaration().Subjects) {
		t.Errorf("subjects %v", s.Subjects)
	}
}

func TestBindAddsMissingSubjectsAndRemovesNone(t *testing.T) {
	m := newMemory(false, false)
	if _, err := m.Create(t.Context(), streams.Stream{
		ID: "order", Name: "order", Description: "set by an operator",
		Subjects: []string{"order.placed", "order.legacy"},
	}); err != nil {
		t.Fatal(err)
	}
	bind(t, m)

	s, _ := m.Get(t.Context(), "order")
	if !slices.Equal(s.Subjects, []string{"order.placed", "order.legacy", "order.shipped"}) {
		t.Errorf("subjects %v", s.Subjects)
	}
	if s.Description != "set by an operator" {
		t.Errorf("description changed to %q", s.Description)
	}
}

func TestBindNeedsAStreamID(t *testing.T) {
	m := newMemory(false, false)
	if _, err := topic.Bind(t.Context(), m, streams.Stream{Name: "order"}); err == nil {
		t.Fatal("bound a stream without an id")
	}
}

func TestSubscriberJoinsTheSpecsGroup(t *testing.T) {
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed())

	msgs, err := orders.Subscriber().Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, msgs); got.OrderID != "o-1" || got.Customer.Tenant != "acme" {
		t.Errorf("received %+v", got)
	}
	if opts := m.received[0]; opts.Group != "checkout" || opts.Prefetch != 32 {
		t.Errorf("subscribed with group %q prefetch %d", opts.Group, opts.Prefetch)
	}
}

func TestConsumerDeliversAtLeastOnce(t *testing.T) {
	m := newMemory(true, false)
	orders := topic.New(bind(t, m, streams.CapabilityDurable), placed())

	consumer := orders.Consumer()
	if consumer.Name() != "billing" {
		t.Errorf("consumer name %q", consumer.Name())
	}
	deliveries, err := consumer.Consume(t.Context(), consumer.Name())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}
	d := receive(t, deliveries)
	if d.Message().OrderID != "o-1" || d.Attempt() != 1 || d.Raw().Subject != "order.placed" {
		t.Errorf("delivery %+v attempt %d subject %q", d.Message(), d.Attempt(), d.Raw().Subject)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if m.acks.Load() != 1 {
		t.Errorf("acks %d, want 1", m.acks.Load())
	}
}

func TestConsumeIsUnsupportedOnAPlainProvider(t *testing.T) {
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed())

	if _, err := orders.Consumer().Consume(t.Context(), "billing"); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume: got %v, want ErrUnsupported", err)
	}
	if _, err := orders.Consumer().ConsumeFrom(t.Context(), "billing", streams.FromEarliest); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("ConsumeFrom: got %v, want ErrUnsupported", err)
	}
}

func TestUndecodableMessagesAreSkippedAndAcknowledged(t *testing.T) {
	m := newMemory(true, false)
	orders := topic.New(bind(t, m), placed())

	msgs, err := orders.Subscriber().Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := orders.Consumer().Consume(t.Context(), "billing")
	if err != nil {
		t.Fatal(err)
	}
	m.inject("order.placed", "json", []byte("not json"))
	if _, err := orders.Publisher().Publish(t.Context(), order("o-1", "acme")); err != nil {
		t.Fatal(err)
	}

	if got := receive(t, msgs); got.OrderID != "o-1" {
		t.Errorf("subscriber got %+v, want the decodable order", got)
	}
	if got := receive(t, deliveries); got.Message().OrderID != "o-1" {
		t.Errorf("consumer got %+v, want the decodable order", got.Message())
	}
	if m.acks.Load() != 1 {
		t.Errorf("acks %d, want the undecodable delivery acknowledged", m.acks.Load())
	}
}

func TestPublishBatchFallsBackToSequentialPublishes(t *testing.T) {
	m := newMemory(false, false)
	orders := topic.New(bind(t, m), placed())

	ids, err := orders.Publisher().PublishBatch(t.Context(), []Order{order("a", "x"), order("b", "x"), order("c", "x")})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || len(m.calls()) != 3 || m.batches != 0 {
		t.Errorf("ids %v, publishes %d, batches %d", ids, len(m.calls()), m.batches)
	}
}

func TestPublishBatchUsesTheNativeBatchOneRunPerKey(t *testing.T) {
	m := newMemory(false, true)
	orders := topic.New(bind(t, m), placed())

	values := []Order{order("a", "x"), order("a", "x"), order("b", "x"), order("a", "x")}
	ids, err := orders.Publisher().PublishBatch(t.Context(), values)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Errorf("ids %v", ids)
	}
	if m.batches != 3 {
		t.Errorf("batches %d, want 3: a a | b | a", m.batches)
	}
	calls := m.calls()
	keys := make([]string, 0, len(calls))
	for _, c := range calls {
		keys = append(keys, c.opts.PartitionKey)
	}
	if !slices.Equal(keys, []string{"a", "a", "b", "a"}) {
		t.Errorf("keys in order %v", keys)
	}
}

func TestBatchSinkFlushesOnSize(t *testing.T) {
	m := newMemory(false, true)
	sink := topic.NewBatchSink(topic.New(bind(t, m), shipped()).Publisher())

	for _, id := range []string{"a", "b", "c", "d"} {
		if err := sink.Send(t.Context(), order(id, "x")); err != nil {
			t.Fatal(err)
		}
	}
	if m.batches != 1 || len(m.calls()) != 3 {
		t.Errorf("after 4 sends with max size 3: batches %d, publishes %d", m.batches, len(m.calls()))
	}
	ids, err := sink.Finish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 || m.batches != 2 {
		t.Errorf("finish: ids %v, batches %d", ids, m.batches)
	}
}

func TestBatchSinkFlushesOnDelay(t *testing.T) {
	m := newMemory(false, true)
	sink := topic.NewBatchSink(topic.New(bind(t, m), shipped()).Publisher())

	values := make(chan Order)
	done := make(chan []string, 1)
	go func() {
		ids, err := sink.SendAll(t.Context(), values)
		if err != nil {
			t.Error(err)
		}
		done <- ids
	}()

	values <- order("a", "x")
	deadline := time.Now().Add(2 * time.Second)
	for len(m.calls()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(m.calls()) != 1 {
		t.Fatalf("an idle sink should flush after its delay; published %d", len(m.calls()))
	}
	values <- order("b", "x")
	close(values)
	if ids := <-done; len(ids) != 2 {
		t.Errorf("ids %v", ids)
	}
}

func TestAProtobufTopicDecodesIntoAFreshMessage(t *testing.T) {
	m := newMemory(false, false)
	m.codec = protobuf.Codec
	names := topic.New(bind(t, m), &topic.Spec[*wrapperspb.StringValue]{
		Subject: "order.placed",
		Codec:   topic.Protobuf[*wrapperspb.StringValue](),
	})

	msgs, err := names.Subscriber().Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := names.Publisher().Publish(t.Context(), wrapperspb.String("o-1")); err != nil {
		t.Fatal(err)
	}
	first := receive(t, msgs)
	if _, err := names.Publisher().Publish(t.Context(), wrapperspb.String("o-2")); err != nil {
		t.Fatal(err)
	}
	second := receive(t, msgs)
	if first.GetValue() != "o-1" || second.GetValue() != "o-2" {
		t.Errorf("decoded %q then %q", first.GetValue(), second.GetValue())
	}
}

// Compile-time checks that the provider halves are what the tests think.
var (
	_ streams.Streams = (*memory)(nil)
	_ streams.Durable = durable{}
	_ streams.Batch   = batched{}
	_ streams.Durable = both{}
	_ streams.Batch   = both{}
)

func TestATopicsCodecIsItsOwn(t *testing.T) {
	m := newMemory(false, false) // the provider's own codec is JSON
	names := topic.New(bind(t, m), &topic.Spec[*wrapperspb.StringValue]{
		Subject: "order.placed",
		Codec:   topic.Protobuf[*wrapperspb.StringValue](),
	})
	msgs, err := names.Subscriber().Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := names.Publisher().Publish(t.Context(), wrapperspb.String("o-1")); err != nil {
		t.Fatal(err)
	}
	if c := m.calls()[0].codec; c != "protobuf" {
		t.Errorf("framed under %q, want the topic's protobuf", c)
	}
	if got := receive(t, msgs); got.GetValue() != "o-1" {
		t.Errorf("decoded %q", got.GetValue())
	}
}
