package mqtt_test

// These run against an MQTT broker started in-process by mochi-mqtt, so they
// need no broker installed and no container.

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
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

// declareWith creates stream as given and binds a manager to it.
func declareWith(t *testing.T, s streams.Streams, stream streams.Stream) (streams.Stream, streams.Manager) {
	t.Helper()

	created, err := s.Create(t.Context(), stream)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return created, m
}

// raw is a plain MQTT 5 client on the broker at addr, reading and writing the
// wire as another tool on the broker would, subscribed to filter when it is
// not empty.
func raw(t *testing.T, addr, filter string) (*autopaho.ConnectionManager, <-chan *paho.Publish) {
	t.Helper()

	server, err := url.Parse("mqtt://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *paho.Publish, 64)
	up := make(chan error, 1)
	cm, err := autopaho.NewConnection(t.Context(), autopaho.ClientConfig{
		ServerUrls: []*url.URL{server}, KeepAlive: 30, CleanStartOnInitialConnection: true,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			if filter == "" {
				up <- nil
				return
			}
			go func() {
				_, serr := cm.Subscribe(t.Context(), &paho.Subscribe{
					Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
				})
				up <- serr
			}()
		},
		ClientConfig: paho.ClientConfig{
			ClientID: "raw-" + time.Now().Format("150405.000000000"),
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
				select {
				case got <- pr.Packet:
				default:
				}
				return true, nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	t.Cleanup(func() { _ = cm.Disconnect(context.Background()) })
	select {
	case err := <-up:
		if err != nil {
			t.Fatalf("raw subscribe: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the raw client did not connect")
	}
	return cm, got
}

// quiet asserts nothing arrives on ch within window.
func quiet[T any](t *testing.T, ch <-chan T, window time.Duration, what string) {
	t.Helper()

	select {
	case v, ok := <-ch:
		if ok {
			t.Errorf("%s received %+v", what, v)
		}
	case <-time.After(window):
	}
}
