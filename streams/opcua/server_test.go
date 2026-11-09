package opcua_test

// These run against an OPC UA server started in-process by gopcua's own server
// package, so they need no server installed and no container.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsopcua "github.com/the-protobuf-project/runtime-go/streams/opcua"
)

// The nodes testServer creates, by the subject a stream declares them as. The
// namespace is 1 because it is the first one added after the server's own.
const (
	temperature = "ns=1;s=temperature" // Double
	payload     = "ns=1;s=payload"     // ByteString, for framed values
	readOnly    = "ns=1;s=read_only"   // Int32, not writable
	missing     = "ns=1;s=missing"     // declared by the test, absent on the server
	count       = "ns=1;s=count"       // Int32

	// namespaceURI names the test namespace, which a subject can name it by
	// (nsu=) instead of its index.
	namespaceURI = "urn:tpp-streams:test"
)

// testServer starts a server on a free port and returns its endpoint.
//
// gopcua advertises the endpoint it was configured with, so it has to be told a
// port rather than given :0. The port is found by binding one and releasing
// it, which leaves a short window for something else to take it; on a test
// machine that has not been worth more machinery than this.
func testServer(t *testing.T) string {
	t.Helper()
	return startServer(t, freePort(t), false).URLs()[0]
}

// freePort finds a port nothing is listening on.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// startServer starts a server on port. Renumbered, it registers another
// namespace before the test one, so the test namespace's index is 2, not 1.
func startServer(t *testing.T, port int, renumbered bool) *server.Server {
	t.Helper()
	srv := server.New(
		server.EndPoint("127.0.0.1", port),
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
	)

	root, _ := srv.Namespace(0)
	if renumbered {
		other := server.NewNodeNameSpace(srv, "urn:tpp-streams:other")
		root.Objects().AddRef(other.Objects(), id.HasComponent, true)
	}
	ns := server.NewNodeNameSpace(srv, namespaceURI)
	root.Objects().AddRef(ns.Objects(), id.HasComponent, true)

	ns.Objects().AddRef(ns.AddNewVariableStringNode("temperature", 0.0), id.HasComponent, true)
	ns.Objects().AddRef(ns.AddNewVariableStringNode("payload", []byte{}), id.HasComponent, true)
	ns.Objects().AddRef(ns.AddNewVariableStringNode("count", int32(0)), id.HasComponent, true)

	ro := ns.AddNewVariableStringNode("read_only", int32(7))
	_ = ro.SetAttribute(ua.AttributeIDUserAccessLevel, server.DataValueFromValue(byte(ua.AccessLevelTypeCurrentRead)))
	ns.Objects().AddRef(ro, id.HasComponent, true)

	var err error
	for range 50 {
		// A port a stopped server just released can take a moment to free.
		if err = srv.Start(context.Background()); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("starting the server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// connect dials the test server with opts and declares one stream over every
// node.
func connect(t *testing.T, opts ...streamsopcua.Option) (streams.Streams, streams.Manager) {
	t.Helper()
	ctx := context.Background()

	opts = append([]streamsopcua.Option{streamsopcua.WithPublishingInterval(20 * time.Millisecond)}, opts...)
	s, err := streamsopcua.Connect(ctx, testServer(t), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })

	stream, err := s.Create(ctx, streams.Stream{
		Name:     "line1",
		Subjects: []string{temperature, payload, readOnly, missing, count},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(ctx, stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return s, m
}

// subscribe subscribes for the life of the test, and at its end cancels and
// waits for the channel to close.
//
// The wait matters: cleanups run last-registered first, so this one finishes
// before connect's closes the session. Closing the session under a live
// subscription would race the subscription's own delete, which a real server
// answers with BadSessionIdInvalid and gopcua's test server answers with a nil
// pointer panic.
func subscribe(t *testing.T, m streams.Manager, subject string) <-chan streams.Message {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		cancel()
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		for range msgs {
		}
	})
	settleServer()
	return msgs
}

// settleServer waits out gopcua's test server reporting a new item's current
// value. It does that on a goroutine of its own that reads the node without the
// lock its Write handler takes, so a write made at once races it inside the
// server; a real server has no such race.
func settleServer() { time.Sleep(100 * time.Millisecond) }

// next waits for one message, failing the test rather than hanging it.
func next(t *testing.T, ch <-chan streams.Message) streams.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("the channel closed before a message arrived")
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
		return streams.Message{}
	}
}
