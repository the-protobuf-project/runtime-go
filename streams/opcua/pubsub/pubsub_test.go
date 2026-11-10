package pubsub_test

// These run against an MQTT broker started in-process by mochi-mqtt, so they
// need no broker installed and no container. Besides the provider talking to
// itself, a raw MQTT client reads what goes on the wire and writes
// NetworkMessages by hand, as another publisher would.

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/gopcua/opcua/ua"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/streams/opcua/pubsub"
)

const (
	within    = 10 * time.Second
	publisher = "plant-1"
)

// testBroker starts a broker on a free port and returns its address. See the
// mqtt package's testBroker for why the port comes from mochi.
func testBroker(t *testing.T) string {
	t.Helper()
	srv := mochi.New(&mochi.Options{InlineClient: true})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("AddHook: %v", err)
	}
	ln := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	if err := srv.AddListener(ln); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Address()
}

// connectTo is a provider publishing as id on the broker at addr.
func connectTo(t *testing.T, addr, id string, opts ...pubsub.Option) *pubsub.Provider {
	t.Helper()
	p, err := pubsub.Connect(t.Context(), addr, append([]pubsub.Option{pubsub.WithPublisherID(id)}, opts...)...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// provider is a provider publishing as [publisher] on a broker of its own,
// and that broker's address.
func provider(t *testing.T, opts ...pubsub.Option) (*pubsub.Provider, string) {
	t.Helper()
	addr := testBroker(t)
	return connectTo(t, addr, publisher, opts...), addr
}

// declare declares a stream (a WriterGroup) with a fixed id over subjects
// and binds a manager to it.
func declare(t *testing.T, p *pubsub.Provider, id string, subjects ...string) streams.Manager {
	t.Helper()
	s, err := p.Create(t.Context(), streams.Stream{ID: id, Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := p.Bind(t.Context(), s.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return m
}

func subscribe(t *testing.T, m streams.Manager, subject string, opts ...streams.Option) <-chan streams.Message {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ch, err := m.Subscribe(ctx, subject, opts...)
	if err != nil {
		t.Fatalf("Subscribe %s: %v", subject, err)
	}
	return ch
}

func publish(t *testing.T, m streams.Manager, subject string, value any, opts ...streams.Option) string {
	t.Helper()
	id, err := m.Publish(t.Context(), subject, value, opts...)
	if err != nil {
		t.Fatalf("Publish %s: %v", subject, err)
	}
	return id
}

// next is the next message within [within].
func next(t *testing.T, ch <-chan streams.Message) streams.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("subscription ended before a message arrived")
		}
		return m
	case <-time.After(within):
		t.Fatal("nothing arrived within the deadline")
	}
	return streams.Message{}
}

// quiet asserts nothing arrives within 300ms.
func quiet(t *testing.T, ch <-chan streams.Message) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("expected nothing, got %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

// value is the OPC UA value a message carries.
func value(t *testing.T, m streams.Message) any {
	t.Helper()
	var dv ua.DataValue
	if err := m.Decode(&dv); err != nil {
		t.Fatalf("not an OPC UA DataValue: %v", err)
	}
	if dv.Value == nil {
		return nil
	}
	return dv.Value.Value()
}

// rawClient is a plain MQTT client for reading and writing the wire.
type rawClient struct {
	cm  *autopaho.ConnectionManager
	got chan *paho.Publish
}

func raw(t *testing.T, addr, filter string) *rawClient {
	t.Helper()
	u, _ := url.Parse("mqtt://" + addr)
	r := &rawClient{got: make(chan *paho.Publish, 64)}
	up := make(chan struct{}, 1)
	cm, err := autopaho.NewConnection(context.Background(), autopaho.ClientConfig{
		ServerUrls: []*url.URL{u}, KeepAlive: 30, CleanStartOnInitialConnection: true,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			select {
			case up <- struct{}{}:
			default:
			}
		},
		ClientConfig: paho.ClientConfig{
			ClientID: "raw-" + core.NewID(),
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
				r.got <- pr.Packet
				return true, nil
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = cm.Disconnect(ctx)
	})
	select {
	case <-up:
	case <-time.After(within):
		t.Fatal("raw client never connected")
	}
	r.cm = cm
	if filter != "" {
		if _, err := cm.Subscribe(t.Context(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}}); err != nil {
			t.Fatalf("raw subscribe: %v", err)
		}
	}
	return r
}

func (r *rawClient) next(t *testing.T) *paho.Publish {
	t.Helper()
	select {
	case p := <-r.got:
		return p
	case <-time.After(within):
		t.Fatal("the raw client received nothing")
	}
	return nil
}

// inject publishes body at QoS 1 and returns once the broker has it.
func (r *rawClient) inject(t *testing.T, topic string, body any, retain bool, user ...paho.UserProperty) {
	t.Helper()
	var payload []byte
	if s, ok := body.(string); ok {
		payload = []byte(s)
	} else {
		payload, _ = json.Marshal(body)
	}
	p := &paho.Publish{Topic: topic, QoS: 1, Retain: retain, Payload: payload}
	if len(user) > 0 {
		p.Properties = &paho.PublishProperties{User: user}
	}
	if _, err := r.cm.Publish(t.Context(), p); err != nil {
		t.Fatalf("inject: %v", err)
	}
}

// dataSetMessage is a JSON DataSetMessage from writer carrying field as an
// Int32 (type 6) body.
func dataSetMessage(writer string, sequence uint32, field string, body int32) map[string]any {
	return map[string]any{
		"DataSetWriterName": writer,
		"SequenceNumber":    sequence,
		"Payload":           map[string]any{field: map[string]any{"Value": map[string]any{"Type": 6, "Body": body}}},
	}
}

func decodeJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var j map[string]any
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	return j
}

func TestLifecycle(t *testing.T) {
	p, _ := provider(t)
	if p.PublisherID() != publisher {
		t.Fatal(p.PublisherID())
	}
	ctx := t.Context()
	created, err := p.Create(ctx, streams.Stream{ID: "line-a", Description: "before", Subjects: []string{"boiler", "pump/Speed"}})
	if err != nil || !created.Active {
		t.Fatalf("%+v %v", created, err)
	}
	if got, _ := p.Get(ctx, "line-a"); got.Description != "before" {
		t.Fatalf("%+v", got)
	}
	if topic, _ := p.DataTopic("line-a", "boiler"); topic != "opcua/json/data/plant-1/line-a/boiler" {
		t.Fatal(topic)
	}
	generated, err := p.Create(ctx, streams.Stream{Name: "named"})
	if err != nil || !strings.HasSuffix(generated.ID, ":named") {
		t.Fatalf("%+v %v", generated, err)
	}
	if list, _ := p.List(ctx); len(list) != 2 {
		t.Fatalf("%d", len(list))
	}
	updated, err := p.Update(ctx, "line-a", streams.Stream{Description: "after", Subjects: []string{"boiler"}})
	if err != nil || updated.ID != "line-a" || updated.Description != "after" {
		t.Fatalf("%+v %v", updated, err)
	}
	if err := p.Delete(ctx, "line-a"); err != nil {
		t.Fatal(err)
	}
	_, getErr := p.Get(ctx, "line-a")
	_, bindErr := p.Bind(ctx, "line-a")
	_, updateErr := p.Update(ctx, "line-a", streams.Stream{})
	for _, err := range []error{getErr, bindErr, p.Delete(ctx, "line-a"), updateErr} {
		if !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("%v", err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRefusesWhatIsNotATopicLevel(t *testing.T) {
	p, _ := provider(t)
	for _, c := range [][2]string{{"a/b/c", "w"}, {"line+", "w"}, {"line", "w#"}, {"line", "/field"}, {"line", "w/"}} {
		if _, err := p.Create(t.Context(), streams.Stream{ID: c[0], Subjects: []string{c[1]}}); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("%v: %v", c, err)
		}
	}
	if list, _ := p.List(t.Context()); len(list) != 0 {
		t.Fatal(list)
	}
}

func TestConnectRefusesABadPrefixOrPublisher(t *testing.T) {
	addr := testBroker(t)
	for _, opt := range []pubsub.Option{pubsub.WithPrefix(""), pubsub.WithPrefix("a/#"), pubsub.WithPublisherID("a/b")} {
		if _, err := pubsub.Connect(t.Context(), addr, opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("%v", err)
		}
	}
	p, err := pubsub.Connect(t.Context(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if !strings.HasPrefix(p.PublisherID(), "runtime-go-") || p.PublisherID() != strings.ToLower(p.PublisherID()) {
		t.Fatalf("default PublisherId %q", p.PublisherID())
	}
}

func TestAFieldRoundTripsAsATypedValue(t *testing.T) {
	p, _ := provider(t)
	m := declare(t, p, "line-a", "boiler/Temperature", "boiler/Mode")
	temperature := subscribe(t, m, "boiler/Temperature")
	mode := subscribe(t, m, "boiler/Mode")

	at := time.Now().UTC().Truncate(100 * time.Nanosecond)
	sent := &ua.DataValue{Value: ua.MustVariant(21.5), Status: ua.StatusUncertainLastUsableValue, SourceTimestamp: at}
	id := publish(t, m, "boiler/Temperature", sent)
	publish(t, m, "boiler/Mode", "auto", streams.ID("chosen"))

	got := next(t, temperature)
	if got.Subject != "boiler/Temperature" || got.ID != id {
		t.Fatalf("%+v; the MessageId travels", got)
	}
	var dv ua.DataValue
	if err := got.Decode(&dv); err != nil {
		t.Fatal(err)
	}
	if dv.Value.Value() != 21.5 || dv.Status != sent.Status || !dv.SourceTimestamp.Equal(at) {
		t.Fatalf("%+v", dv)
	}
	got = next(t, mode)
	if got.ID != "chosen" || value(t, got) != "auto" {
		t.Fatalf("%+v", got)
	}
	// Each field subscription saw only its own field.
	quiet(t, temperature)
}

func TestAWriterPublishesAndDeliversItsWholeDataSet(t *testing.T) {
	p, _ := provider(t)
	m := declare(t, p, "line-a", "boiler")
	all := subscribe(t, m, "boiler")
	// A writer declared whole accepts each of its fields too.
	one := subscribe(t, m, "boiler/Pressure")

	set := pubsub.NewDataSet().With("Temperature", 80.0).With("Pressure", int32(2)).With("Running", true)
	id := publish(t, m, "boiler", set)
	want := []struct {
		subject string
		value   any
	}{{"boiler/Temperature", 80.0}, {"boiler/Pressure", int32(2)}, {"boiler/Running", true}}
	for _, w := range want {
		got := next(t, all)
		if got.ID != id {
			t.Fatal("the fields of one DataSetMessage share its id")
		}
		if got.Subject != w.subject || value(t, got) != w.value {
			t.Fatalf("%s %v, want %v", got.Subject, value(t, got), w)
		}
	}
	if got := next(t, one); got.Subject != "boiler/Pressure" {
		t.Fatal(got.Subject)
	}
	quiet(t, one)
	publish(t, m, "boiler/Running", false)
	if got := next(t, all); got.Subject != "boiler/Running" {
		t.Fatal(got.Subject)
	}
}

func TestTheWireIsPart14JSON(t *testing.T) {
	p, addr := provider(t)
	m := declare(t, p, "line-a", "boiler")
	r := raw(t, addr, "opcua/json/data/plant-1/line-a/#")

	id := publish(t, m, "boiler/Temperature", 21.5)
	publish(t, m, "boiler/Temperature", 22.0)

	msg := r.next(t)
	if msg.Topic != "opcua/json/data/plant-1/line-a/boiler" || msg.Retain {
		t.Fatalf("%s retain=%v", msg.Topic, msg.Retain)
	}
	if msg.Properties == nil || msg.Properties.ContentType != "application/json" {
		t.Fatalf("%+v", msg.Properties)
	}
	j := decodeJSON(t, msg.Payload)
	for k, want := range map[string]any{"MessageId": id, "MessageType": "ua-data", "PublisherId": "plant-1", "WriterGroupName": "line-a"} {
		if j[k] != want {
			t.Errorf("%s = %v", k, j[k])
		}
	}
	first := j["Messages"].([]any)[0].(map[string]any)
	if first["DataSetWriterName"] != "boiler" || first["DataSetWriterId"] != float64(pubsub.WriterID("boiler")) ||
		first["SequenceNumber"] != 1.0 {
		t.Fatalf("%v", first)
	}
	if _, ok := first["Timestamp"].(string); !ok {
		t.Fatal("no Timestamp")
	}
	if !strings.Contains(string(msg.Payload), `"Temperature":{"Value":{"Type":11,"Body":21.5}}`) {
		t.Fatalf("%s", msg.Payload)
	}
	// The writer numbers its messages.
	second := decodeJSON(t, r.next(t).Payload)
	if second["Messages"].([]any)[0].(map[string]any)["SequenceNumber"] != 2.0 {
		t.Fatal(second)
	}
}

func TestABatchIsOneNetworkMessage(t *testing.T) {
	p, addr := provider(t)
	m := declare(t, p, "line-a", "boiler/Count")
	sub := subscribe(t, m, "boiler/Count")
	r := raw(t, addr, "opcua/json/data/plant-1/line-a/boiler")

	batch, err := streams.AsBatch(m)
	if err != nil {
		t.Fatal(err)
	}
	values := []any{int32(0), int32(1), int32(2), int32(3), int32(4)}
	ids, err := batch.PublishBatch(t.Context(), "boiler/Count", values)
	if err != nil || len(ids) != 5 {
		t.Fatalf("%v %v", ids, err)
	}
	network, err := pubsub.ParseNetworkMessage(r.next(t).Payload)
	if err != nil || len(network.Messages) != 5 {
		t.Fatalf("one NetworkMessage for the batch: %+v %v", network, err)
	}
	for n, msg := range network.Messages {
		if *msg.SequenceNumber != uint32(n+1) || ids[n] != network.MessageID+"."+string(rune('0'+n)) {
			t.Fatalf("%d: seq %d id %s", n, *msg.SequenceNumber, ids[n])
		}
	}
	for n, id := range ids {
		got := next(t, sub)
		if got.ID != id || value(t, got) != int32(n) {
			t.Fatalf("%+v", got)
		}
	}

	// A value that is not an OPC UA value fails alone.
	ids, err = batch.PublishBatch(t.Context(), "boiler/Count", []any{int32(10), struct{ N int }{11}, int32(12)})
	if err == nil || !errors.Is(err, streams.ErrUnsupported) || len(ids) != 3 || !strings.Contains(err.Error(), "1 of 3") {
		t.Fatalf("%v %v", ids, err)
	}
	if value(t, next(t, sub)) != int32(10) || value(t, next(t, sub)) != int32(12) {
		t.Fatal("the good entries were not delivered")
	}

	// An id cannot name several messages.
	if _, err := batch.PublishBatch(t.Context(), "boiler/Count", []any{int32(1)}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestALongBatchIsSplit(t *testing.T) {
	p, _ := provider(t, pubsub.WithMaxDatasetMessages(4))
	m := declare(t, p, "line-a", "w/f")
	sub := subscribe(t, m, "w/f")
	values := make([]any, 10)
	for i := range values {
		values[i] = int32(i)
	}
	ids, err := m.(streams.Batch).PublishBatch(t.Context(), "w/f", values)
	if err != nil {
		t.Fatal(err)
	}
	prefixes := map[string]bool{}
	for n, id := range ids {
		got := next(t, sub)
		if got.ID != id || value(t, got) != int32(n) {
			t.Fatalf("%d: %+v", n, got)
		}
		prefix, _, _ := strings.Cut(id, ".")
		prefixes[prefix] = true
	}
	// Ten in fours: 4, 4, 2.
	if len(prefixes) != 3 || p.SequenceGaps() != 0 {
		t.Fatalf("%d NetworkMessages, %d gaps", len(prefixes), p.SequenceGaps())
	}
}

func TestRefusals(t *testing.T) {
	p, _ := provider(t)
	m := declare(t, p, "line-a", "boiler", "pump/Speed")
	ctx := t.Context()

	for _, subject := range []string{"typo", "pump", "pump/Other", "boiler/"} {
		if _, err := m.Publish(ctx, subject, int32(1)); !errors.Is(err, streams.ErrUnknownSubject) {
			t.Errorf("publish %s: %v", subject, err)
		}
		if _, err := m.Subscribe(ctx, subject); !errors.Is(err, streams.ErrUnknownSubject) {
			t.Errorf("subscribe %s: %v", subject, err)
		}
	}

	// A field takes an OPC UA value, a writer a DataSet.
	for subject, v := range map[string]any{"pump/Speed": struct{ N int }{1}, "boiler": int32(1)} {
		if _, err := m.Publish(ctx, subject, v); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("%s: %v", subject, err)
		}
	}
	if _, err := m.Publish(ctx, "pump/Speed", int32(1), streams.TTL(time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Error(err)
	}
	if _, err := streams.AsDurable(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Error(err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Error(err)
	}

	// Another publisher's WriterGroup can be read, not written.
	foreign := declare(t, p, "plant-2/line-a", "boiler")
	_, err := foreign.Publish(ctx, "boiler/x", int32(1))
	if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "plant-2") {
		t.Error(err)
	}
	if _, err := p.PublishMetadata(ctx, "plant-2/line-a", "boiler", ua.DataSetMetaDataType{}); !errors.Is(err, streams.ErrUnsupported) {
		t.Error(err)
	}
	if _, err := p.PublishMetadata(ctx, "line-a", "nobody", ua.DataSetMetaDataType{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Error(err)
	}

	// A closed provider publishes nothing.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish(ctx, "pump/Speed", int32(1)); err == nil || errors.Is(err, streams.ErrUnsupported) {
		t.Error(err)
	}
}

func TestReadsAnotherPublisherAndDetectsGaps(t *testing.T) {
	p, addr := provider(t)
	m := declare(t, p, "plant-2/line-b", "press")
	sub := subscribe(t, m, "press")
	r := raw(t, addr, "")
	writerTopic := "opcua/json/data/plant-2/line-b/press"
	groupTopic := "opcua/json/data/plant-2/line-b"

	// On the writer's topic, as another stack writes it: no MessageId, a
	// non-reversible value.
	r.inject(t, writerTopic, map[string]any{
		"MessageType": "ua-data", "PublisherId": "plant-2",
		"Messages": []any{map[string]any{"SequenceNumber": 1, "Payload": map[string]any{"Force": map[string]any{"Value": 12.5}}}},
	}, false)
	got := next(t, sub)
	if got.Subject != "press/Force" || value(t, got) != 12.5 {
		t.Fatalf("%+v %v", got, value(t, got))
	}

	// On the group's topic, among another writer's messages.
	r.inject(t, groupTopic, map[string]any{
		"MessageId": "g-1", "MessageType": "ua-data", "PublisherId": "plant-2",
		"Messages": []any{dataSetMessage("other", 1, "x", 1), dataSetMessage("press", 2, "Force", 2)},
	}, false)
	got = next(t, sub)
	if got.ID != "g-1.1" || value(t, got) != int32(2) || p.SequenceGaps() != 0 {
		t.Fatalf("%+v gaps %d", got, p.SequenceGaps())
	}

	// 3 and 4 never arrive: a gap of two. Then 5 again is a repeat.
	for _, seq := range []uint32{5, 5, 6} {
		r.inject(t, writerTopic, map[string]any{
			"MessageType": "ua-data", "PublisherId": "plant-2",
			"Messages": []any{dataSetMessage("press", seq, "Force", int32(seq))},
		}, false)
	}
	if value(t, next(t, sub)) != int32(5) || value(t, next(t, sub)) != int32(6) || p.SequenceGaps() != 2 {
		t.Fatalf("gaps %d", p.SequenceGaps())
	}

	// Garbage and metadata on the data topic are dropped; the stream lives on.
	r.inject(t, writerTopic, "not json", false)
	r.inject(t, writerTopic, map[string]any{"MessageType": "ua-metadata", "MetaData": map[string]any{}}, false)
	// A restarted publisher counts from 1 again, which is not a gap.
	r.inject(t, writerTopic, map[string]any{
		"MessageType": "ua-data", "PublisherId": "plant-2",
		"Messages": []any{dataSetMessage("press", 1, "Force", 100)},
	}, false)
	if value(t, next(t, sub)) != int32(100) || p.SequenceGaps() != 2 {
		t.Fatalf("gaps %d", p.SequenceGaps())
	}
}

func TestAGroupSharesTheWriter(t *testing.T) {
	p, _ := provider(t)
	m := declare(t, p, "line-a", "w/f")
	first := subscribe(t, m, "w/f", streams.Group("workers"))
	second := subscribe(t, m, "w/f", streams.Group("workers"))
	const n = 40
	for i := 0; i < n; i++ {
		publish(t, m, "w/f", int32(i))
	}
	a, b := 0, 0
	for a+b < n {
		select {
		case <-first:
			a++
		case <-second:
			b++
		case <-time.After(within):
			t.Fatalf("only %d of %d arrived", a+b, n)
		}
	}
	if a == 0 || b == 0 {
		t.Fatalf("the broker did not split the writer: %d and %d", a, b)
	}
	quiet(t, first)
	if p.SequenceGaps() != 0 {
		t.Fatal("a group member does not track gaps")
	}
}

func TestMetadataIsRetainedAndVersionsTheData(t *testing.T) {
	p, addr := provider(t)
	m := declare(t, p, "line-a", "boiler")
	set := pubsub.NewDataSet().With("Temperature", 80.0).With("Running", true)
	version, err := p.PublishMetadata(t.Context(), "line-a", "boiler", set.Metadata("boiler"))
	if err != nil || version.MajorVersion == 0 {
		t.Fatalf("a zero version is stamped: %+v %v", version, err)
	}

	// A reader that arrives afterwards still finds it, retained.
	r := raw(t, addr, "opcua/json/metadata/plant-1/line-a/boiler")
	msg := r.next(t)
	j := decodeJSON(t, msg.Payload)
	field := j["MetaData"].(map[string]any)["Fields"].([]any)[0].(map[string]any)
	if !msg.Retain || j["MessageType"] != "ua-metadata" || j["DataSetWriterName"] != "boiler" ||
		field["Name"] != "Temperature" || field["BuiltInType"] != 11.0 {
		t.Fatalf("retain=%v %s", msg.Retain, msg.Payload)
	}

	read, err := p.Metadata(t.Context(), "line-a", "boiler")
	if err != nil || *read.MetaData.ConfigurationVersion != version || len(read.MetaData.Fields) != 2 {
		t.Fatalf("%+v %v", read, err)
	}
	if _, err = p.Metadata(t.Context(), "line-a", "nobody"); !errors.Is(err, streams.ErrNotFound) {
		t.Fatal(err)
	}

	// DataSetMessages now say which metadata they follow.
	data := raw(t, addr, "opcua/json/data/plant-1/line-a/boiler")
	publish(t, m, "boiler", set)
	network, err := pubsub.ParseNetworkMessage(data.next(t).Payload)
	if err != nil || *network.Messages[0].MetaDataVersion != version {
		t.Fatalf("%+v %v", network, err)
	}
}

func TestRetainedDataIsNotDelivered(t *testing.T) {
	p, addr := provider(t)
	m := declare(t, p, "line-a", "w")
	r := raw(t, addr, "")
	r.inject(t, "opcua/json/data/plant-1/line-a/w", dataSetMessage("w", 1, "f", 1), true)
	sub := subscribe(t, m, "w")
	quiet(t, sub)
	publish(t, m, "w/f", int32(2))
	if v := value(t, next(t, sub)); v != int32(2) {
		t.Fatal(v)
	}
}

// Headers ride as MQTT 5 user properties on the NetworkMessage's PUBLISH;
// the Part 14 JSON is unchanged.
func TestHeadersAreUserPropertiesBesidePart14JSON(t *testing.T) {
	p, addr := provider(t)
	s, err := p.Create(t.Context(), streams.Stream{ID: "line-h", Subjects: []string{"boiler"}}.
		WithHeader("lang", "go").WithHeader("site", "plant-3"))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := p.Bind(t.Context(), s.ID)
	r := raw(t, addr, "opcua/json/data/plant-1/line-h/#")
	sub := subscribe(t, m, "boiler")

	publish(t, m, "boiler/Temperature", 21.5, streams.Header("site", "plant-4"), streams.Expires(2500*time.Millisecond))
	got := next(t, sub)
	if got.Subject != "boiler/Temperature" || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-4" {
		t.Fatalf("%+v", got)
	}

	msg := r.next(t)
	props := msg.Properties
	if props == nil || props.ContentType != "application/json" {
		t.Fatalf("%+v", props)
	}
	if len(props.User) != 2 || props.User[0] != (paho.UserProperty{Key: "lang", Value: "go"}) ||
		props.User[1] != (paho.UserProperty{Key: "site", Value: "plant-4"}) {
		t.Fatalf("user properties %+v", props.User)
	}
	if props.MessageExpiry == nil || *props.MessageExpiry < 2 || *props.MessageExpiry > 3 {
		t.Fatalf("expiry rounded up: %v", props.MessageExpiry)
	}
	if strings.Contains(string(msg.Payload), "lang") || decodeJSON(t, msg.Payload)["MessageType"] != "ua-data" {
		t.Fatalf("no header inside the Part 14 JSON: %s", msg.Payload)
	}

	// A batch's NetworkMessage carries them too.
	if _, err := m.(streams.Batch).PublishBatch(t.Context(), "boiler/Temperature", []any{1.0, 2.0}, streams.Header("n", "b")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got := next(t, sub)
		if got.Headers.Get("n") != "b" || got.Headers.Get("lang") != "go" {
			t.Fatalf("%+v", got.Headers)
		}
	}
}

func TestAnotherPublishersUserPropertiesArriveAsHeaders(t *testing.T) {
	p, addr := provider(t)
	m := declare(t, p, "plant-2/line-a", "w")
	sub := subscribe(t, m, "w")
	r := raw(t, addr, "")
	r.inject(t, "opcua/json/data/plant-2/line-a/w", map[string]any{
		"MessageId": "m-1", "MessageType": "ua-data", "PublisherId": "plant-2",
		"Messages": []any{dataSetMessage("w", 1, "f", 7)},
	}, false, paho.UserProperty{Key: "lang", Value: "dotnet"})
	got := next(t, sub)
	if value(t, got) != int32(7) || got.Headers.Get("lang") != "dotnet" {
		t.Fatalf("%+v", got)
	}
}

func TestRetainIsRefusedAndInvalidHeadersFailAtCreate(t *testing.T) {
	p, _ := provider(t)
	m := declare(t, p, "line-r", "w")
	_, err1 := m.Publish(t.Context(), "w/f", int32(1), streams.Retain())
	_, err2 := m.(streams.Batch).PublishBatch(t.Context(), "w/f", []any{int32(1)}, streams.Retain())
	for _, err := range []error{err1, err2} {
		if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "retained") {
			t.Errorf("retain: %v", err)
		}
	}
	if _, err := p.Create(t.Context(), streams.Stream{ID: "line-b", Subjects: []string{"w"}}.WithHeader("a b", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Error(err)
	}
	if _, err := m.Publish(t.Context(), "w/f", int32(1), streams.Header("k", "two\nlines")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Error(err)
	}
}

func TestASubscriptionEndsWithItsContext(t *testing.T) {
	p, _ := provider(t)
	m := declare(t, p, "line-a", "w")
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := m.Subscribe(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("a message after cancel")
		}
	case <-time.After(within):
		t.Fatal("the channel did not close")
	}
}
