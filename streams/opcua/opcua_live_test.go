//go:build live

package opcua_test

// The provider against Microsoft's OPC PLC simulator, the OPC Foundation's
// .NET stack, from docker/compose.opcua.yaml:
//
//	docker compose -p tpp-streams-live-opcua -f docker/compose.opcua.yaml up -d --wait
//	go test -tags live ./opcua -run Live
//	STREAMS_E2E_RESTART=1 go test -tags live ./opcua -run Live
//
// SecurityPolicy None and an anonymous user; opcua_e2e_test.go is the secured
// equivalent. The nodes written are compose.opcua.nodes.json's, in opc-plc's
// namespace, named here by URI. OPCUA_URL (default opc.tcp://127.0.0.1:14840)
// and OPCUA_CONTAINER (default tpp-streams-opcua) point it elsewhere. The
// restart test runs only with STREAMS_E2E_RESTART=1, because it restarts that
// container. Every test skips when the simulator cannot be reached.
//
// Every test holds one lock: they write the same nodes, and the restart test
// takes the server away from everything else.

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsopcua "github.com/the-protobuf-project/runtime-go/streams/opcua"
)

const (
	plcNamespace = "http://microsoft.com/Opc/OpcPlc/"
	liveWait     = 15 * time.Second
)

// serial is held by every live and e2e test.
var serial sync.Mutex

// env is the environment variable key, or fallback when it is unset.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// plcNode names an opc-plc node by namespace URI, which stays stable when the
// server renumbers its namespaces.
func plcNode(name string) string { return "nsu=" + plcNamespace + ";s=" + name }

// reachable skips the test unless something listens at endpoint.
func reachable(t *testing.T, endpoint string) {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("endpoint %q: %v", endpoint, err)
	}
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(t.Context(), "tcp", u.Host)
	if err != nil {
		t.Skipf("no OPC UA server at %s (%v); is the compose file up?", endpoint, err)
	}
	_ = conn.Close()
}

// liveClient connects anonymously with no security to the plain simulator.
func liveClient(t *testing.T) *gopcua.Client {
	t.Helper()
	endpoint := env("OPCUA_URL", "opc.tcp://127.0.0.1:14840")
	reachable(t, endpoint)
	c, err := gopcua.NewClient(endpoint,
		gopcua.SecurityMode(ua.MessageSecurityModeNone),
		gopcua.AuthAnonymous(),
		gopcua.ReconnectInterval(500*time.Millisecond),
		gopcua.RequestTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(t.Context()); err != nil {
		t.Fatalf("Connect to %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

var plcNodes = []string{"Temperature", "Count", "Running", "Mode", "Sequence", "Serial"}

// liveBound is a store on c with a stream over names, and a manager bound to it.
func liveBound(t *testing.T, c *gopcua.Client, names ...string) (streams.Streams, streams.Manager) {
	t.Helper()
	s := streamsopcua.Use(c)
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	subjects := make([]string, len(names))
	for i, n := range names {
		subjects[i] = plcNode(n)
	}
	stream, err := s.Create(t.Context(), streams.Stream{Name: "plc", Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return s, m
}

// liveSubscribe subscribes for the life of the test.
func liveSubscribe(t *testing.T, m streams.Manager, subject string, opts ...streams.Option) <-chan streams.Message {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	msgs, err := m.Subscribe(ctx, subject, opts...)
	if err != nil {
		t.Fatalf("Subscribe(%s): %v", subject, err)
	}
	return msgs
}

// liveNext waits up to liveWait for a message.
func liveNext(t *testing.T, ch <-chan streams.Message) streams.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("the channel closed before a message arrived")
		}
		return msg
	case <-time.After(liveWait):
		t.Fatal("timed out waiting for a notification")
		return streams.Message{}
	}
}

// decodeAs decodes msg into a T.
func decodeAs[T any](t *testing.T, msg streams.Message) T {
	t.Helper()
	var v T
	if err := msg.Decode(&v); err != nil {
		t.Fatalf("Decode %s: %v", msg.Data, err)
	}
	return v
}

// restartContainer restarts a container and waits until it reports healthy.
func restartContainer(t *testing.T, name string) {
	t.Helper()
	if out, err := exec.CommandContext(t.Context(), "docker", "restart", name).CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s: %v: %s", name, err, out)
	}
	for range 120 {
		out, _ := exec.CommandContext(t.Context(), "docker", "inspect", "-f", "{{.State.Health.Status}}", name).Output()
		if strings.TrimSpace(string(out)) == "healthy" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s did not come back healthy", name)
}

func TestLiveLifecycle(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	s := streamsopcua.Use(liveClient(t))
	ctx := t.Context()
	stream, err := s.Create(ctx, streams.Stream{Name: "plc", Subjects: []string{plcNode("Count")}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if list, _ := s.List(ctx); len(list) != 1 || list[0].ID != stream.ID {
		t.Fatalf("List = %+v", list)
	}
	stream.Subjects = []string{plcNode("Count"), plcNode("Mode")}
	if updated, uerr := s.Update(ctx, stream.ID, stream); uerr != nil || len(updated.Subjects) != 2 {
		t.Fatalf("Update = %+v, %v", updated, uerr)
	}
	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := m.Publish(ctx, plcNode("Running"), true); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Fatalf("Publish = %v, want ErrUnknownSubject", err)
	}
	// A namespace the server does not have fails the bind, by name.
	stranger, _ := s.Create(ctx, streams.Stream{Subjects: []string{"nsu=urn:nobody;s=x"}})
	if _, err := s.Bind(ctx, stranger.ID); err == nil || !strings.Contains(err.Error(), "urn:nobody") {
		t.Fatalf("Bind = %v, want an error naming urn:nobody", err)
	}
	_ = s.Delete(ctx, stream.ID)
	_ = s.Delete(ctx, stranger.ID)
	if _, err := s.Get(ctx, stream.ID); !errors.Is(err, streams.ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
}

func TestLiveTypedValuesRoundTrip(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	_, m := liveBound(t, liveClient(t), plcNodes...)
	ctx := t.Context()
	temp, cnt, run, mode, seq := liveSubscribe(t, m, plcNode("Temperature")), liveSubscribe(t, m, plcNode("Count")),
		liveSubscribe(t, m, plcNode("Running")), liveSubscribe(t, m, plcNode("Mode")), liveSubscribe(t, m, plcNode("Sequence"))

	before := time.Now().Add(-time.Second) // a second's allowance for the container's clock
	for subject, v := range map[string]any{
		"Temperature": 21.5, "Count": int32(-42), "Running": true, "Mode": "cooling", "Sequence": int64(-9223372036854775808),
	} {
		if _, err := m.Publish(ctx, plcNode(subject), v); err != nil {
			t.Fatalf("Publish %s: %v", subject, err)
		}
	}
	check := func(name string, msg streams.Message) {
		t.Helper()
		if msg.Subject != plcNode(name) {
			t.Errorf("%s arrived on %q", name, msg.Subject)
		}
		// The .NET stack stamps a write with its source time when it carries
		// none, and that is the message's id.
		at, err := time.Parse(time.RFC3339Nano, msg.ID)
		if err != nil || at.Before(before) {
			t.Errorf("%s: id %q is not a source timestamp after the write", name, msg.ID)
		}
	}
	msg := liveNext(t, temp)
	check("Temperature", msg)
	if got := decodeAs[float64](t, msg); got != 21.5 {
		t.Errorf("Temperature = %v", got)
	}
	msg = liveNext(t, cnt)
	check("Count", msg)
	if got := decodeAs[int32](t, msg); got != -42 {
		t.Errorf("Count = %v", got)
	}
	msg = liveNext(t, run)
	check("Running", msg)
	if got := decodeAs[bool](t, msg); !got {
		t.Errorf("Running = %v", got)
	}
	msg = liveNext(t, mode)
	check("Mode", msg)
	if got := decodeAs[string](t, msg); got != "cooling" {
		t.Errorf("Mode = %v", got)
	}
	msg = liveNext(t, seq)
	check("Sequence", msg)
	if got := decodeAs[int64](t, msg); got != -9223372036854775808 {
		t.Errorf("Sequence = %v", got)
	}
}

func TestLiveBadWritesNameTheStatus(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	_, m := liveBound(t, liveClient(t), plcNodes...)
	ctx := t.Context()
	_, err := m.Publish(ctx, plcNode("Count"), "forty-two")
	if !errors.Is(err, ua.StatusBadTypeMismatch) || errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("Publish a string to an Int32 = %v, want BadTypeMismatch", err)
	}
	_, err = m.Publish(ctx, plcNode("Serial"), "SN-2")
	if !errors.Is(err, streams.ErrUnsupported) || !errors.Is(err, ua.StatusBadNotWritable) && !errors.Is(err, ua.StatusBadUserAccessDenied) {
		t.Fatalf("Publish to a read-only node = %v, want a settled refusal", err)
	}
}

// A *ua.DataValue is written as given: opc-plc's user nodes accept a write
// that carries its own quality and source time, as a gateway forwarding a
// device's reading would send.
func TestLiveStatusAndTimestampsSurvive(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	c := liveClient(t)
	_, m := liveBound(t, c, plcNodes...)
	ctx := t.Context()
	temp := plcNode("Temperature")
	msgs := liveSubscribe(t, m, temp)

	source := time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)
	// A value of its own, since the node keeps the last run's: writing the
	// very same DataValue again is, rightly, no change to report.
	reading := 19.0 + float64(os.Getpid()%1000)/1000
	if _, err := m.Publish(ctx, temp, &ua.DataValue{Value: ua.MustVariant(reading), SourceTimestamp: source}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := liveNext(t, msgs)
	if got := decodeAs[float64](t, msg); got != reading || msg.ID != source.Format(time.RFC3339Nano) {
		t.Fatalf("got %v with id %q, want %v stamped %s", got, msg.ID, reading, source)
	}

	// A status the subscriber skips (it is not a reading) is still the node's.
	if _, err := m.Publish(ctx, temp, &ua.DataValue{Value: ua.MustVariant(reading + 1), Status: ua.StatusUncertainLastUsableValue, SourceTimestamp: source}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ns, err := c.FindNamespace(ctx, plcNamespace)
	if err != nil {
		t.Fatal(err)
	}
	node := ua.NewStringNodeID(ns, "Temperature")
	res, err := c.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{{NodeID: node, AttributeID: ua.AttributeIDValue}}, TimestampsToReturn: ua.TimestampsToReturnBoth})
	if err != nil || res.Results[0].Status != ua.StatusUncertainLastUsableValue || !res.Results[0].SourceTimestamp.Equal(source) {
		t.Fatalf("Read = %+v, %v; want the written status and source time", res, err)
	}
}

// opc-plc's BadFastUInt1 changes every second and is periodically Bad; the
// subscriber receives its good readings and skips the rest.
func TestLiveSimulatedBadQualityIsSkipped(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	s := streamsopcua.Use(liveClient(t))
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	subject := plcNode("BadFastUInt1")
	stream, _ := s.Create(t.Context(), streams.Stream{Subjects: []string{subject}})
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	msgs := liveSubscribe(t, m, subject)
	for range 3 {
		decodeAs[uint32](t, liveNext(t, msgs))
	}
}

func TestLiveBatchWritesInOrder(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	_, m := liveBound(t, liveClient(t), plcNodes...)
	ctx := t.Context()
	seq := plcNode("Sequence")
	msgs := liveSubscribe(t, m, seq, streams.Prefetch(500))
	b, _ := streams.AsBatch(m)
	values := make([]any, 200)
	for i := range values {
		values[i] = int64(i + 1)
	}
	ids, err := b.PublishBatch(ctx, seq, values)
	if err != nil || len(ids) != 200 {
		t.Fatalf("PublishBatch = %d ids, %v", len(ids), err)
	}
	for want := int64(1); want <= 200; want++ {
		if got := decodeAs[int64](t, liveNext(t, msgs)); got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	}
	ids, err = b.PublishBatch(ctx, seq, []any{int64(1), "x", int64(3)})
	if len(ids) != 3 || err == nil || !strings.Contains(err.Error(), "1 of 3") || !strings.Contains(err.Error(), "entry 1") || !errors.Is(err, ua.StatusBadTypeMismatch) {
		t.Fatalf("PublishBatch = %d ids, %v; want entry 1 to fail with BadTypeMismatch", len(ids), err)
	}
}

func TestLiveRefusals(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	_, m := liveBound(t, liveClient(t), plcNodes...)
	ctx := t.Context()
	temp := plcNode("Temperature")
	if _, err := m.Subscribe(ctx, temp, streams.Group("g")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Subscribe with a group = %v", err)
	}
	for _, opt := range []streams.Option{
		streams.TTL(time.Second), streams.Header("lang", "go"), streams.Expires(5 * time.Second), streams.Retain(),
	} {
		if _, err := m.Publish(ctx, temp, 1.0, opt); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("Publish = %v, want ErrUnsupported", err)
		}
	}
	if _, err := streams.AsDurable(m); err == nil {
		t.Error("AsDurable succeeded")
	}
	if _, err := streams.AsPositioned(m); err == nil {
		t.Error("AsPositioned succeeded")
	}
}

// Every subscribe adds an item to one subscription; the server agrees an item
// dropped is gone, and the last takes the subscription with it. A subscribe
// after that makes a new one.
func TestLiveDroppingASubscriptionDeletesTheMonitoredItem(t *testing.T) {
	serial.Lock()
	defer serial.Unlock()
	c := liveClient(t)
	s, m := liveBound(t, c, plcNodes...)
	ctx := t.Context()

	a, cancelA := context.WithCancel(ctx)
	if _, err := m.Subscribe(a, plcNode("Temperature")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, cancelB := context.WithCancel(ctx)
	if _, err := m.Subscribe(b, plcNode("Count")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ids := c.SubscriptionIDs()
	if len(ids) != 1 || streamsopcua.MonitoredItems(s) != 2 {
		t.Fatalf("client holds subscriptions %v and %d items; want one holding two", ids, streamsopcua.MonitoredItems(s))
	}
	cancelA()
	cancelB()
	deadline := time.Now().Add(liveWait)
	for len(c.SubscriptionIDs()) != 0 || streamsopcua.MonitoredItems(s) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the subscription stayed: %v", c.SubscriptionIDs())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The server agrees: the subscription is gone.
	res, err := c.Call(ctx, &ua.CallMethodRequest{
		ObjectID:       ua.NewNumericNodeID(0, 2253),  // Server
		MethodID:       ua.NewNumericNodeID(0, 11492), // GetMonitoredItems
		InputArguments: []*ua.Variant{ua.MustVariant(ids[0])},
	})
	if err == nil && res.StatusCode == ua.StatusOK {
		t.Fatalf("the server still holds subscription %d", ids[0])
	}

	msgs := liveSubscribe(t, m, plcNode("Count"))
	want := int32(os.Getpid() % 1000)
	if _, err := m.Publish(ctx, plcNode("Count"), want); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := decodeAs[int32](t, liveNext(t, msgs)); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

// liveRestart restarts container while a subscription to Count is live, and
// checks writes fail while it is down and the subscription carries on after.
func liveRestart(t *testing.T, c *gopcua.Client, m streams.Manager, container string) {
	t.Helper()
	ctx := t.Context()
	count := plcNode("Count")
	msgs := liveSubscribe(t, m, count)
	if _, err := m.Publish(ctx, count, int32(1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := decodeAs[int32](t, liveNext(t, msgs)); got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
	before := c.Session()

	restartContainer(t, container)
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := m.Publish(ctx, count, int32(7))
		if err == nil {
			break
		}
		if errors.Is(err, streams.ErrUnknownSubject) || time.Now().After(deadline) {
			t.Fatalf("never came back: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if c.Session() == before {
		t.Error("the client carried on with the old session")
	}
	// The recreated item reports the node's value, then the write; a value
	// the restarted server reports before the write may come first.
	deadline = time.Now().Add(30 * time.Second)
	for decodeAs[int32](t, liveNext(t, msgs)) != 7 {
		if time.Now().After(deadline) {
			t.Fatal("7 never arrived")
		}
	}
	if _, err := m.Publish(ctx, count, int32(8)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := decodeAs[int32](t, liveNext(t, msgs)); got != 8 {
		t.Fatalf("got %d, want 8", got)
	}
}

func TestLiveSubscriptionsSurviveAServerRestart(t *testing.T) {
	if os.Getenv("STREAMS_E2E_RESTART") != "1" {
		t.Skip("set STREAMS_E2E_RESTART=1 to restart the simulator")
	}
	serial.Lock()
	defer serial.Unlock()
	c := liveClient(t)
	_, m := liveBound(t, c, plcNodes...)
	liveRestart(t, c, m, env("OPCUA_CONTAINER", "tpp-streams-opcua"))
}
