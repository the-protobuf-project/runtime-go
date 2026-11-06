package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/opcua"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// batch is this program's model, too structured for any one OPC UA type.
type batch struct {
	Recipe string  `json:"recipe"`
	Litres float64 `json:"litres"`
}

const (
	temperature = "ns=1;s=Line1.Temperature" // a Double
	lastBatch   = "ns=1;s=Line1.LastBatch"   // a ByteString
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(telemetry.LevelInfo)})))

	srv := startServer(ctx)
	defer func() { _ = srv.Close() }()

	s, err := opcua.Connect(ctx, srv.URLs()[0], opcua.WithLogger(logger))
	if err != nil {
		log.Fatalf("the OPC UA server is not reachable: %v", err)
	}
	defer func() { _ = s.(streams.Closer).Close() }()

	stream, err := s.Create(ctx, streams.Stream{Name: "line1", Subjects: []string{temperature, lastBatch}})
	if err != nil {
		log.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		log.Fatalf("Bind: %v", err)
	}

	if _, derr := streams.AsDurable(m); errors.Is(derr, streams.ErrUnsupported) {
		log.Printf("OPC UA lends nothing, so there is nothing to acknowledge: %v", derr)
	}

	log.Println("--- a subscriber starts with the node's state ---")

	sub, stop := context.WithCancel(ctx)
	temps, err := m.Subscribe(sub, temperature)
	if err != nil {
		log.Fatalf("Subscribe: %v", err)
	}
	var c float64
	_ = receive(temps).Decode(&c)
	log.Printf("current temperature: %.1f", c)

	// A float64 is written as a Double, so an HMI reading this node sees a
	// number rather than this module's frame.
	if _, perr := m.Publish(ctx, temperature, 72.5); perr != nil {
		log.Fatalf("Publish: %v", perr)
	}
	_ = receive(temps).Decode(&c)
	log.Printf("changed to: %.1f", c)

	log.Println("--- a struct travels framed ---")

	batches, err := m.Subscribe(sub, lastBatch)
	if err != nil {
		log.Fatalf("Subscribe: %v", err)
	}
	receive(batches) // the node's current, empty, value

	pid, err := m.Publish(ctx, lastBatch, batch{Recipe: "lager", Litres: 1200})
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	msg := receive(batches)
	var b batch
	if derr := msg.Decode(&b); derr != nil {
		log.Fatalf("Decode: %v", derr)
	}
	log.Printf("%+v arrived as %s, the id it was published under (%s)", b, msg.ID, pid)

	// Cancel the subscriptions and let them end before closing the session,
	// so each one's delete reaches the server while the session is still open.
	stop()
	for range temps {
	}
	for range batches {
	}
	log.Println("done")
}

// startServer runs an OPC UA server in this process with the two nodes the
// example uses.
func startServer(ctx context.Context) *server.Server {
	srv := server.New(
		server.EndPoint("127.0.0.1", 48400),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
	)
	ns := server.NewNodeNameSpace(srv, "line1")
	srv.AddNamespace(ns)
	root, _ := srv.Namespace(0)
	root.Objects().AddRef(ns.Objects(), id.HasComponent, true)

	ns.Objects().AddRef(ns.AddNewVariableStringNode("Line1.Temperature", 68.0), id.HasComponent, true)
	ns.Objects().AddRef(ns.AddNewVariableStringNode("Line1.LastBatch", []byte{}), id.HasComponent, true)

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("starting the OPC UA server: %v", err)
	}
	return srv
}

func receive(ch <-chan streams.Message) streams.Message {
	select {
	case msg, ok := <-ch:
		if !ok {
			log.Fatal("the channel closed early")
		}
		return msg
	case <-time.After(10 * time.Second):
		log.Fatal("timed out waiting for a message")
	}
	return streams.Message{}
}
