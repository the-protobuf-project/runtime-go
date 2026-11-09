//go:build live

package pubsub_test

// OPC UA PubSub interop: the provider subscribes to what Microsoft's OPC
// Publisher, a third-party Part 14 implementation on the OPC Foundation's
// .NET stack, publishes over MQTT from the opc-plc simulator. Start them, from
// streams/, with
//
//	docker compose -p tpp-streams-live-opcua-pubsub -f docker/compose.opcua.pubsub.yaml up -d --wait
//	go test -race -tags live ./opcua/pubsub -run Live
//
// OPCUA_PUBSUB_MQTT_ADDR (default 127.0.0.1:11885) points it elsewhere. OPC
// Publisher sends a DataSetMessage each publishing interval (500 ms) in which
// a value changed, so the first one can take a few seconds while it connects
// to the PLC after a cold start. Without the broker the tests skip.

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/streams/opcua/pubsub"
)

// OPC Publisher's PublisherId, WriterGroup and DataSetWriter, from the
// compose file and compose.opcua.pubsub.publishednodes.json.
const (
	liveStream = "opc-publisher/plc"
	liveWriter = "simulation"
	liveWithin = 60 * time.Second
)

func liveAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("OPCUA_PUBSUB_MQTT_ADDR")
	if addr == "" {
		addr = "127.0.0.1:11885"
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Skipf("no broker at %s (start docker/compose.opcua.pubsub.yaml): %v", addr, err)
	}
	_ = conn.Close()
	return addr
}

// liveConnect is a provider with a fresh PublisherId that declares OPC
// Publisher's WriterGroup, so it can read what OPC Publisher sends.
func liveConnect(t *testing.T) *pubsub.Provider {
	t.Helper()
	p := connectTo(t, liveAddr(t), "tpp-live-"+strings.ToLower(core.NewID()))
	if _, err := p.Create(t.Context(), streams.Stream{ID: liveStream, Subjects: []string{liveWriter}}); err != nil {
		t.Fatal(err)
	}
	return p
}

func liveNext(t *testing.T, ch <-chan streams.Message) streams.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("the subscription ended")
		}
		return m
	case <-time.After(liveWithin):
		t.Fatal("OPC Publisher sent nothing within the deadline")
	}
	return streams.Message{}
}

// stepUp is the StepUp counter's value, which opc-plc publishes as a UInt32.
func stepUp(t *testing.T, m streams.Message) uint32 {
	t.Helper()
	n, ok := value(t, m).(uint32)
	if !ok {
		t.Fatalf("StepUp is a UInt32 on opc-plc, got %T", value(t, m))
	}
	return n
}

func TestLiveReadsOPCPublisherDataSets(t *testing.T) {
	p := liveConnect(t)
	m, err := p.Bind(t.Context(), liveStream)
	if err != nil {
		t.Fatal(err)
	}
	steps := subscribe(t, m, liveWriter+"/StepUp")
	all := subscribe(t, m, liveWriter)

	// StepUp counts up on the PLC; OPC Publisher reports each new value.
	first := liveNext(t, steps)
	if first.Subject != "simulation/StepUp" {
		t.Fatal(first.Subject)
	}
	var dv ua.DataValue
	if err := first.Decode(&dv); err != nil {
		t.Fatal(err)
	}
	if dv.SourceTimestamp.IsZero() || dv.ServerTimestamp.IsZero() {
		t.Fatalf("the PLC's timestamps travel: %+v", dv)
	}
	last := stepUp(t, first)
	for i := 0; i < 4; i++ {
		n := stepUp(t, liveNext(t, steps))
		if n <= last {
			t.Fatalf("StepUp went from %d to %d", last, n)
		}
		last = n
	}

	// The whole writer: every field OPC Publisher sends, typed.
	seen := map[string]bool{}
	for len(seen) < 2 {
		msg := liveNext(t, all)
		field := strings.TrimPrefix(msg.Subject, "simulation/")
		v := value(t, msg)
		var ok bool
		switch field {
		case "StepUp":
			_, ok = v.(uint32)
		case "RandomSignedInt32":
			_, ok = v.(int32)
		case "AlternatingBoolean":
			_, ok = v.(bool)
		default:
			t.Fatalf("unexpected field %s", field)
		}
		if !ok {
			t.Fatalf("%s is a %T", field, v)
		}
		seen[field] = true
	}
	if p.SequenceGaps() != 0 {
		t.Fatalf("OPC Publisher's SequenceNumbers arrived with %d missing", p.SequenceGaps())
	}
}

func TestLiveReadsOPCPublisherMetadata(t *testing.T) {
	p := liveConnect(t)
	// OPC Publisher retains it, so it is there whenever a reader asks.
	var meta pubsub.MetaDataMessage
	var err error
	for i := 0; i < 30; i++ {
		if meta, err = p.Metadata(t.Context(), liveStream, liveWriter); !errors.Is(err, streams.ErrNotFound) {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatalf("OPC Publisher retained no metadata: %v", err)
	}
	if meta.PublisherID != "opc-publisher" || meta.WriterGroupName != "plc" || len(meta.MetaData.Fields) != 3 {
		t.Fatalf("%+v", meta)
	}
	for _, f := range meta.MetaData.Fields {
		if f.Name == "StepUp" && f.BuiltInType != 7 {
			t.Fatalf("StepUp is a UInt32, described as %d", f.BuiltInType)
		}
	}
}

func TestLiveOPCPublishersGroupIsNotOursToWrite(t *testing.T) {
	p := liveConnect(t)
	m, _ := p.Bind(t.Context(), liveStream)
	if _, err := m.Publish(t.Context(), "simulation/StepUp", uint32(1)); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatal(err)
	}
}

// The provider against a real broker, beside OPC Publisher: its own
// writers, a batch, metadata, headers and expiry.
func TestLiveRoundTripsOnMosquitto(t *testing.T) {
	addr := liveAddr(t)
	id := "tpp-live-" + strings.ToLower(core.NewID())
	p := connectTo(t, addr, id)
	s, err := p.Create(t.Context(), streams.Stream{ID: "line-a", Subjects: []string{"boiler"}}.WithHeader("lang", "go"))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := p.Bind(t.Context(), s.ID)
	all := subscribe(t, m, "boiler")

	sent := publish(t, m, "boiler/Temperature", 21.5, streams.Expires(30*time.Second), streams.Header("site", "plant-3"))
	got := next(t, all)
	if got.ID != sent || value(t, got) != 21.5 || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-3" {
		t.Fatalf("%+v", got)
	}
	values := make([]any, 250)
	for i := range values {
		values[i] = int32(i)
	}
	ids, err := m.(streams.Batch).PublishBatch(t.Context(), "boiler/Count", values)
	if err != nil {
		t.Fatal(err)
	}
	for n, id := range ids {
		got := next(t, all)
		if got.ID != id || value(t, got) != int32(n) {
			t.Fatalf("%d: %+v", n, got)
		}
	}
	version, err := p.PublishMetadata(t.Context(), "line-a", "boiler", pubsub.NewDataSet().With("Temperature", 1.0).Metadata("boiler"))
	if err != nil {
		t.Fatal(err)
	}
	reader := connectTo(t, addr, "tpp-live-reader-"+strings.ToLower(core.NewID()))
	if _, err = reader.Create(t.Context(), streams.Stream{ID: id + "/line-a", Subjects: []string{"boiler"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	read, err := reader.Metadata(ctx, id+"/line-a", "boiler")
	if err != nil || *read.MetaData.ConfigurationVersion != version {
		t.Fatalf("%+v %v", read, err)
	}
	if p.SequenceGaps() != 0 {
		t.Fatal(p.SequenceGaps())
	}
}
