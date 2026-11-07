package mqtt_test

// These run against an MQTT broker started in-process by mochi-mqtt, so they
// need no broker installed and no container.

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user/created"

// broker is how a test wants its broker: in the clear and open by default.
type broker struct {
	tls    *tls.Config
	ledger *auth.Ledger
}

// testBroker starts a broker on a free port and returns its address.
//
// The port comes from mochi rather than being reserved here. AddListener calls
// the listener's Init, which binds the socket synchronously, and TCP.Address
// then reports the port the kernel chose -- so by the time this returns the
// socket is bound and the backlog is accepting, whether or not Serve's accept
// loop has been scheduled yet.
//
// The two things that replaced were both races. Reserving a port with our own
// listener and closing it before handing mochi the address left a window for
// anything else on the machine to take it. Dialing afterwards to check the
// broker was up was worse: it left a connection that sent no CONNECT packet,
// so mochi was still inside EstablishConnection for it when the cleanup below
// called Close, and those two race on the server's own fields inside the
// library (server.go:401 against server.go:1499 in v2.7.9). That surfaced as
// an intermittent "race detected during execution of test" in CI, on
// whichever test happened to finish fastest.
func testBroker(t *testing.T, b broker) (string, *mochi.Server) {
	t.Helper()

	srv := mochi.New(&mochi.Options{InlineClient: true})
	var err error
	if b.ledger != nil {
		err = srv.AddHook(new(auth.Hook), &auth.Options{Ledger: b.ledger})
	} else {
		err = srv.AddHook(new(auth.AllowHook), nil)
	}
	if err != nil {
		t.Fatalf("AddHook: %v", err)
	}
	ln := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0", TLSConfig: b.tls})
	if err := srv.AddListener(ln); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })

	return ln.Address(), srv
}

func testStreams(t *testing.T, opts ...streamsmqtt.Option) streams.Streams {
	t.Helper()
	addr, _ := testBroker(t, broker{})
	return connect(t, addr, opts...)
}

func connect(t *testing.T, addr string, opts ...streamsmqtt.Option) streams.Streams {
	t.Helper()

	s, err := streamsmqtt.Connect(t.Context(), addr, opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	return s
}

func declare(t *testing.T, s streams.Streams, subjects ...string) (streams.Stream, streams.Manager) {
	t.Helper()

	stream, err := s.Create(t.Context(), streams.Stream{Name: "test", Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return stream, m
}

// drop cuts every client whose id starts with prefix from the broker's side,
// which is what a network failure looks like to the client.
func drop(t *testing.T, srv *mochi.Server, prefix string) {
	t.Helper()

	n := 0
	for id, cl := range srv.Clients.GetAll() {
		if strings.HasPrefix(id, prefix) {
			cl.Stop(errors.New("dropped by the test"))
			n++
		}
	}
	if n == 0 {
		t.Fatalf("no client with an id starting %q to drop", prefix)
	}
}

func recv[T any](t *testing.T, ch <-chan T, within time.Duration) T {
	t.Helper()

	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before a message arrived")
		}
		return v
	case <-time.After(within):
		t.Fatalf("nothing arrived within %s", within)
		var zero T
		return zero
	}
}
