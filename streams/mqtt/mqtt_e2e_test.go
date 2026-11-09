//go:build live

package mqtt_test

// End to end against the secure Docker broker: mutual TLS, persistent sessions
// on disk, authenticated two ways, by the client certificate's identity on one
// listener, and by username and password, under ACLs, on another. Start it
// from streams/ with
//
//	docker/tls/generate.sh            # once, if docker/tls/out is empty
//	docker compose -p tpp-streams-e2e-mqtt -f docker/compose.mqtt.secure.yaml up -d --wait
//
// then run STREAMS_E2E_RESTART=1 go test -race -tags live ./mqtt -run E2E.
//
// MQTT_E2E_ADDR (default 127.0.0.1:18883, the certificate-identity listener),
// MQTT_E2E_PASSWORD_ADDR (default 127.0.0.1:18884, the password listener),
// STREAMS_E2E_TLS_DIR (default ../docker/tls/out) and MQTT_E2E_CONTAINER
// (default tpp-streams-mqtt-secure) point it elsewhere. The restart test runs
// only with STREAMS_E2E_RESTART=1, because it restarts that container. The
// tests skip when the broker is not up.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

// The test-only users of the password listener, from
// docker/compose.mqtt.secure.yaml. docker/compose.mqtt.secure.acl confines
// each to the topics under its own name.
var (
	app   = [2]string{"e2e-app", "e2e-app-secret"}
	other = [2]string{"e2e-other", "e2e-other-secret"}
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// e2eAddr is the listener where the certificate's common name is the
// username, or a skip when the broker is not up.
func e2eAddr(t *testing.T) string {
	t.Helper()
	addr := envOr("MQTT_E2E_ADDR", "127.0.0.1:18883")
	reachable(t, addr)
	return addr
}

// passwordAddr is the listener that wants a certificate and then a login.
func passwordAddr(t *testing.T) string {
	t.Helper()
	addr := envOr("MQTT_E2E_PASSWORD_ADDR", "127.0.0.1:18884")
	reachable(t, addr)
	return addr
}

func container() string { return envOr("MQTT_E2E_CONTAINER", "tpp-streams-mqtt-secure") }

// tlsConfig trusts the test CA when trustCA, and presents the test client
// certificate when clientCert.
func tlsConfig(t *testing.T, clientCert, trustCA bool) *tls.Config {
	t.Helper()

	dir := envOr("STREAMS_E2E_TLS_DIR", filepath.Join("..", "docker", "tls", "out"))
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: x509.NewCertPool()}
	if trustCA {
		pem, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
		if err != nil {
			t.Skipf("no test CA (run docker/tls/generate.sh): %v", err)
		}
		cfg.RootCAs.AppendCertsFromPEM(pem)
	}
	if clientCert {
		cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))
		if err != nil {
			t.Skipf("no test client certificate (run docker/tls/generate.sh): %v", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

// e2eOptions are QoS 1 under prefix, with sessions expiring after five
// minutes so leftovers do not pile up on the broker, and a quick reconnect.
func e2eOptions(prefix string, more ...streamsmqtt.Option) []streamsmqtt.Option {
	return append([]streamsmqtt.Option{
		streamsmqtt.WithPrefix(prefix),
		streamsmqtt.WithQoS(1),
		streamsmqtt.WithSessionExpiry(5 * time.Minute),
		streamsmqtt.WithReconnectDelay(500 * time.Millisecond),
	}, more...)
}

// connectCert connects on the certificate-identity listener over mutual TLS.
func connectCert(t *testing.T, prefix string, more ...streamsmqtt.Option) streams.Streams {
	t.Helper()
	return connect(t, e2eAddr(t), e2eOptions(prefix, append([]streamsmqtt.Option{streamsmqtt.WithTLS(tlsConfig(t, true, true))}, more...)...)...)
}

// connectAs logs in as user on the password listener over mutual TLS. Every
// connection the streams open (subscribers, durable sessions, shared
// subscriptions) logs in the same way.
func connectAs(t *testing.T, user [2]string, prefix string, more ...streamsmqtt.Option) streams.Streams {
	t.Helper()
	return connect(t, passwordAddr(t), e2eOptions(prefix, append([]streamsmqtt.Option{
		streamsmqtt.WithTLS(tlsConfig(t, true, true)), streamsmqtt.WithCredentials(user[0], user[1]),
	}, more...)...)...)
}

// freshFor is a fresh prefix inside the topic tree the ACLs give user.
func freshFor(user [2]string, tag string) string { return user[0] + "/" + fresh(tag) }

func TestE2ETLSIsRequiredAndMutual(t *testing.T) {
	addr := e2eAddr(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Plaintext MQTT to the TLS port: the broker hangs up.
	if s, err := streamsmqtt.Connect(ctx, "mqtt://"+addr, streamsmqtt.WithPrefix("plain")); err == nil {
		_ = s.(streams.Closer).Close()
		t.Error("a plaintext connection to the secure port succeeded")
	}
	// TLS without a client certificate: require_certificate refuses it.
	if s, err := streamsmqtt.Connect(ctx, addr, streamsmqtt.WithTLS(tlsConfig(t, false, true))); err == nil {
		_ = s.(streams.Closer).Close()
		t.Error("the broker accepted a TLS client without a certificate")
	}
	// TLS that does not trust the CA: the client refuses the broker.
	if s, err := streamsmqtt.Connect(ctx, addr, streamsmqtt.WithTLS(tlsConfig(t, true, false))); err == nil {
		_ = s.(streams.Closer).Close()
		t.Error("the client accepted a broker certificate from an untrusted CA")
	}

	// Mutual TLS works, end to end.
	_, m := declare(t, connectCert(t, fresh("tls")), subject)
	roundTrip(t, m)
}

// The password listener takes a certificate and then a login. The broker
// answers a bad login with CONNACK 0x87, not authorized (or 0x86, bad
// username or password), and the provider reports the broker's reason.
func TestE2EPasswordsAreChecked(t *testing.T) {
	addr := passwordAddr(t)
	for _, tc := range []struct {
		user, password, what string
	}{
		{app[0], "not-the-password", "a wrong password"},
		{app[0], "", "a missing password"},
		{app[0], other[1], "another user's password"},
		{"nobody", "anything", "an unknown user"},
		{"", "", "no credentials at all"},
	} {
		opts := []streamsmqtt.Option{streamsmqtt.WithTLS(tlsConfig(t, true, true))}
		if tc.user != "" {
			opts = append(opts, streamsmqtt.WithCredentials(tc.user, tc.password))
		}
		s, err := streamsmqtt.Connect(t.Context(), addr, opts...)
		if err == nil {
			_ = s.(streams.Closer).Close()
			t.Errorf("%s was accepted", tc.what)
			continue
		}
		var refused *autopaho.ConnackError
		if !errors.As(err, &refused) || (refused.ReasonCode != 0x86 && refused.ReasonCode != 0x87) {
			t.Errorf("%s was refused, but not as not authorized: %v", tc.what, err)
		}
	}

	// Without a client certificate the right password never gets that far.
	if s, err := streamsmqtt.Connect(t.Context(), addr, streamsmqtt.WithTLS(tlsConfig(t, false, true)), streamsmqtt.WithCredentials(app[0], app[1])); err == nil {
		_ = s.(streams.Closer).Close()
		t.Error("the password listener accepted a client without a certificate")
	}

	// The right password over mutual TLS works end to end.
	_, m := declare(t, connectAs(t, app, freshFor(app, "login")), subject)
	roundTrip(t, m)
}

// Each password user is confined to its own topic tree, and the ACLs are the
// password listener's alone: the certificate identity is not confined.
func TestE2EACLsConfineEachUser(t *testing.T) {
	theirs := freshFor(other, "acl")
	const silent = 1500 * time.Millisecond

	owner := connectAs(t, other, theirs)
	_, owned := declareWith(t, owner, streams.Stream{ID: "acl", Name: "acl", Subjects: []string{subject}})
	ownersView, err := owned.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe (owner): %v", err)
	}

	// e2e-app aimed at e2e-other's tree. Its durable session expires a second
	// after it leaves rather than lingering.
	intruder := connectAs(t, app, theirs, streamsmqtt.WithSessionExpiry(time.Second))
	_, trespass := declareWith(t, intruder, streams.Stream{ID: "acl", Name: "acl", Subjects: []string{subject}})

	// Publishing outside its tree is refused outright: PUBACK 0x87, and the
	// call fails with it rather than reporting it published.
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if _, perr := trespass.Publish(ctx, subject, event{User: "mallory"}); perr == nil || !strings.Contains(strings.ToLower(perr.Error()), "authori") {
		t.Errorf("a publish outside the user's topics = %v, want not authorized", perr)
	}
	quiet(t, ownersView, silent, "the owner, of a refused publish")

	// Subscribing outside its tree is granted in the SUBACK, and every message
	// fails the read check instead: plain, shared and durable alike hear
	// nothing.
	sctx, scancel := context.WithCancel(t.Context())
	plain, err := trespass.Subscribe(sctx, subject)
	if err != nil {
		t.Fatalf("Subscribe (intruder): %v", err)
	}
	shared, err := trespass.Subscribe(sctx, subject, streams.Group(fresh("spies")))
	if err != nil {
		t.Fatalf("Subscribe (shared intruder): %v", err)
	}
	durable, err := mustDurable(t, trespass).Consume(sctx, subject, fresh("spy"))
	if err != nil {
		t.Fatalf("Consume (intruder): %v", err)
	}
	id, err := owned.Publish(t.Context(), subject, event{User: "ada"})
	if err != nil {
		t.Fatalf("Publish (owner): %v", err)
	}
	if got := recv(t, ownersView, within); got.ID != id {
		t.Errorf("owner heard %q, want %q", got.ID, id)
	}
	quiet(t, plain, silent, "a subscription outside the user's topics")
	quiet(t, shared, silent, "a shared subscription outside them")
	quiet(t, durable, silent, "a durable consumer outside them")
	scancel()

	// The certificate identity is under no ACL: the same topics are open to it.
	_, unconfined := declareWith(t, connectCert(t, theirs), streams.Stream{ID: "acl", Name: "acl", Subjects: []string{subject}})
	view, err := unconfined.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe (identity): %v", err)
	}
	id, err = unconfined.Publish(t.Context(), subject, event{User: "cert"})
	if err != nil {
		t.Fatalf("the certificate identity is confined: %v", err)
	}
	if got := recv(t, view, within); got.ID != id {
		t.Errorf("identity heard %q, want %q", got.ID, id)
	}
	if got := recv(t, ownersView, within); got.ID != id {
		t.Errorf("owner heard %q, want %q", got.ID, id)
	}
}

func TestE2ELifecycle(t *testing.T) {
	s := connectAs(t, app, freshFor(app, "life"))
	ctx := t.Context()

	explicit, err := s.Create(ctx, streams.Stream{ID: "orders", Name: "orders", Description: "before", Subjects: []string{subject}})
	if err != nil || explicit.ID != "orders" || !explicit.Active {
		t.Fatalf("Create = %+v, %v", explicit, err)
	}
	generated, err := s.Create(ctx, streams.Stream{Name: "with/slashes+and#hashes", Subjects: []string{subject}})
	if err != nil || generated.ID == "" || strings.ContainsAny(generated.ID, "/+#") {
		t.Fatalf("Create (generated) = %q, %v", generated.ID, err)
	}
	list, _ := s.List(ctx)
	if len(list) != 2 {
		t.Errorf("List = %d streams, want 2", len(list))
	}
	if _, uerr := s.Update(ctx, "orders", streams.Stream{Name: "orders", Description: "after", Subjects: []string{subject, "user/deleted"}}); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}
	if got, _ := s.Get(ctx, "orders"); got.Description != "after" {
		t.Errorf("Update did not take: %q", got.Description)
	}

	// The update is what a later bind sees.
	m, err := s.Bind(ctx, "orders")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ch, err := m.Subscribe(ctx, "user/deleted")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := m.Publish(ctx, "user/deleted", event{User: "ada"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recv(t, ch, within); got.Subject != "user/deleted" {
		t.Errorf("Subject = %q", got.Subject)
	}

	for _, id := range []string{explicit.ID, generated.ID} {
		if err := s.Delete(ctx, id); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := s.Get(ctx, id); !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("Get after Delete = %v", err)
		}
		if _, err := s.Bind(ctx, id); !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("Bind after Delete = %v", err)
		}
		if _, err := s.Update(ctx, id, streams.Stream{}); !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("Update after Delete = %v", err)
		}
		if err := s.Delete(ctx, id); !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("Delete twice = %v", err)
		}
	}
}

func TestE2EPublishSubscribeOptionsAndCapabilities(t *testing.T) {
	s := connectCert(t, fresh("pubsub"))
	_, m := declareWith(t, s, streams.Stream{ID: "users", Name: "users", Subjects: []string{subject, "device/+"}})

	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable: %v", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v, want ErrUnsupported", err)
	}
	b, err := streams.AsBatch(m)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}

	ch, err := m.Subscribe(t.Context(), subject, streams.Prefetch(1))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "ada", Action: "created"}, streams.ID("chosen-id"), streams.PartitionKey("ada"))
	if err != nil || id != "chosen-id" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	got := recv(t, ch, within)
	var e event
	if derr := got.Decode(&e); derr != nil || got.ID != "chosen-id" || got.Subject != subject || e.User != "ada" {
		t.Errorf("delivered %q on %q: %+v (%v)", got.ID, got.Subject, e, derr)
	}
	for i := range 5 {
		_, _ = m.Publish(t.Context(), subject, event{Action: strconv.Itoa(i)})
	}
	for i := range 5 {
		var e event
		_ = recv(t, ch, within).Decode(&e)
		if e.Action != strconv.Itoa(i) {
			t.Errorf("message %d is %q, out of order", i, e.Action)
		}
	}
	// A zero TTL is "no delay".
	if _, perr := m.Publish(t.Context(), subject, event{}, streams.TTL(0)); perr != nil {
		t.Errorf("Publish with a zero TTL: %v", perr)
	}
	recv(t, ch, within)

	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}}, streams.PartitionKey("k"))
	if err != nil || len(ids) != 3 {
		t.Fatalf("PublishBatch = %v, %v", ids, err)
	}
	for _, id := range ids {
		if got := recv(t, ch, within); got.ID != id {
			t.Errorf("batch delivered %q, want %q", got.ID, id)
		}
	}

	// Refusals.
	if _, err := m.Publish(t.Context(), "device/+", event{}); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish to a filter = %v, want ErrUnsupported", err)
	}
	if _, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
	if _, err := m.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish on typo = %v", err)
	}
	if _, err := m.Subscribe(t.Context(), "typo"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe on typo = %v", err)
	}
	if _, err := mustDurable(t, m).Consume(t.Context(), "typo", "c"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Consume on typo = %v", err)
	}
	if _, err := mustDurable(t, m).Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", err)
	}
	if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an id = %v", err)
	}
	if _, err := b.PublishBatch(t.Context(), "typo", []any{event{}}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("PublishBatch on typo = %v", err)
	}

	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := m.Publish(ctx, subject, event{}); err == nil {
		t.Error("publishing after Close succeeded")
	}
}

// Ack settles; nak leaves the message with the session, and it comes back when
// the consumer does. The session is named by consumer and subject, not stream,
// so the same name on another stream resumes it, and must still hear it.
func TestE2EDurableAckNakAndRedelivery(t *testing.T) {
	s := connectCert(t, fresh("durable"))
	_, m := declareWith(t, s, streams.Stream{ID: "durable", Name: "durable", Subjects: []string{subject}})
	consumer := fresh("billing")
	prefetch := streams.Prefetch(1)

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := mustDurable(t, m).Consume(ctx, subject, consumer, prefetch)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	acked, _ := m.Publish(t.Context(), subject, event{User: "acked"})
	naked, _ := m.Publish(t.Context(), subject, event{User: "naked"})
	first := recv(t, ch, within)
	if first.ID != acked || first.Attempt != 0 {
		t.Errorf("first = %q attempt %d", first.ID, first.Attempt)
	}
	_ = first.Ack(ctx)
	second := recv(t, ch, within)
	if second.ID != naked {
		t.Errorf("second = %q, want %q", second.ID, naked)
	}
	_ = second.Nak(ctx)
	settle()
	cancel()
	settle()

	away, _ := m.Publish(t.Context(), subject, event{User: "away"})
	actx, acancel := context.WithCancel(t.Context())
	again, err := mustDurable(t, m).Consume(actx, subject, consumer, prefetch)
	if err != nil {
		t.Fatalf("Consume (again): %v", err)
	}
	if got := ids(drain(t, again, 3*time.Second)); fmt.Sprint(got) != fmt.Sprint([]string{naked, away}) {
		t.Errorf("resumed with %v, want [%s %s]", got, naked, away)
	}
	settle()
	acancel()
	settle()

	_, moved := declareWith(t, s, streams.Stream{ID: "durable-other", Name: "other", Subjects: []string{subject}})
	mch, err := mustDurable(t, moved).Consume(t.Context(), subject, consumer, prefetch)
	if err != nil {
		t.Fatalf("Consume (other stream): %v", err)
	}
	id, _ := moved.Publish(t.Context(), subject, event{User: "moved"})
	if got := ids(drain(t, mch, 3*time.Second)); len(got) != 1 || got[0] != id {
		t.Errorf("the resumed session heard %v, want [%s]", got, id)
	}
}

// A group is a shared subscription: each message goes to exactly one member,
// durable or not, every member logging in.
func TestE2EGroupsShareTheSubject(t *testing.T) {
	s := connectAs(t, app, freshFor(app, "group"))
	_, m := declare(t, s, subject)
	const total = 8

	group := streams.Group(fresh("workers"))
	a, _ := mustDurable(t, m).Consume(t.Context(), subject, "worker", group)
	b, _ := mustDurable(t, m).Consume(t.Context(), subject, "worker", group)
	sent := map[string]bool{}
	for i := range total {
		id, _ := m.Publish(t.Context(), subject, event{Action: strconv.Itoa(i)})
		sent[id] = true
	}
	for range total {
		var got streams.Delivery
		select {
		case got = <-a:
		case got = <-b:
		case <-time.After(within):
			t.Fatal("the group stopped short")
		}
		if !sent[got.ID] {
			t.Errorf("%q arrived twice", got.ID)
		}
		delete(sent, got.ID)
		_ = got.Ack(t.Context())
	}
	quiet(t, a, 500*time.Millisecond, "a durable member, after the subject was shared out")
	quiet(t, b, 500*time.Millisecond, "a durable member, after the subject was shared out")

	readers := streams.Group(fresh("readers"))
	ra, _ := m.Subscribe(t.Context(), subject, readers)
	rb, _ := m.Subscribe(t.Context(), subject, readers)
	for i := range total {
		_, _ = m.Publish(t.Context(), subject, event{Action: strconv.Itoa(i)})
	}
	for count := 0; count < total; count++ {
		select {
		case <-ra:
		case <-rb:
		case <-time.After(within):
			t.Fatalf("saw %d of %d across the readers", count, total)
		}
	}
	quiet(t, ra, 500*time.Millisecond, "a reader, after the subject was shared out")
	quiet(t, rb, 500*time.Millisecond, "a reader, after the subject was shared out")
}

// sysWatch keeps the broker's own $SYS counters, read over a mutual-TLS
// connection of its own.
type sysWatch struct {
	mu     sync.Mutex
	latest map[string]uint64
}

const (
	sysConnected     = "$SYS/broker/clients/connected"
	sysSubscriptions = "$SYS/broker/subscriptions/count"
)

func watchSys(t *testing.T) *sysWatch {
	t.Helper()

	w := &sysWatch{latest: map[string]uint64{}}
	server, _ := url.Parse("tls://" + e2eAddr(t))
	up := make(chan error, 1)
	cm, err := autopaho.NewConnection(t.Context(), autopaho.ClientConfig{
		ServerUrls: []*url.URL{server}, TlsCfg: tlsConfig(t, true, true), KeepAlive: 30, CleanStartOnInitialConnection: true,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			go func() {
				_, serr := cm.Subscribe(t.Context(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
					{Topic: sysConnected}, {Topic: sysSubscriptions},
				}})
				up <- serr
			}()
		},
		ClientConfig: paho.ClientConfig{
			ClientID: "e2e-sys-" + fresh("w"),
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
				if n, perr := strconv.ParseUint(strings.TrimSpace(string(pr.Packet.Payload)), 10, 64); perr == nil {
					w.mu.Lock()
					w.latest[pr.Packet.Topic] = n
					w.mu.Unlock()
				}
				return true, nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("$SYS watcher: %v", err)
	}
	t.Cleanup(func() { _ = cm.Disconnect(context.Background()) })
	if err := <-up; err != nil {
		t.Fatalf("$SYS subscribe: %v", err)
	}
	return w
}

func (w *sysWatch) get(topic string) (uint64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, ok := w.latest[topic]
	return n, ok
}

// baseline is a counter's steady value: the least seen over three seconds, so
// a healthcheck client connecting for a moment is not mistaken for ours.
func (w *sysWatch) baseline(t *testing.T, topic string) uint64 {
	t.Helper()
	var least uint64
	seen := false
	for range 30 {
		if n, ok := w.get(topic); ok && (!seen || n < least) {
			least, seen = n, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("the broker never published %s", topic)
	}
	return least
}

// reaches waits up to 15s for topic to read want.
func (w *sysWatch) reaches(topic string, want uint64) error {
	var last uint64
	for range 150 {
		if n, ok := w.get(topic); ok {
			last = n
			if n == want {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s stayed at %d, expected %d", topic, last, want)
}

// Canceling a subscription unsubscribes and disconnects, so the broker holds
// nothing for it; canceling a durable consumer disconnects but keeps its
// session's subscription, as promised. Closing releases the publishing
// connection.
func TestE2ECancelingLeavesNothingOnTheBroker(t *testing.T) {
	s := connectCert(t, fresh("cleanup"))
	w := watchSys(t)
	_, m := declare(t, s, subject)

	// The counts are the whole broker's, and sessions earlier runs left behind
	// expire when they will, so a measurement one lands in is taken again.
	var failures []error
	for range 3 {
		err := comeAndGo(t, w, m)
		if err == nil {
			break
		}
		failures = append(failures, err)
	}
	if len(failures) == 3 {
		t.Fatalf("every measurement failed: %v", failures)
	}

	open := w.baseline(t, sysConnected)
	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for range 150 {
		if n, _ := w.get(sysConnected); n <= open-1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("closing did not release the publishing connection")
}

func comeAndGo(t *testing.T, w *sysWatch, m streams.Manager) error {
	clients := w.baseline(t, sysConnected)
	subs := w.baseline(t, sysSubscriptions)

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := m.Subscribe(ctx, subject); err != nil {
		cancel()
		return err
	}
	if err := w.reaches(sysConnected, clients+1); err != nil {
		cancel()
		return err
	}
	if err := w.reaches(sysSubscriptions, subs+1); err != nil {
		cancel()
		return err
	}
	cancel()
	if err := w.reaches(sysConnected, clients); err != nil {
		return err
	}
	if err := w.reaches(sysSubscriptions, subs); err != nil {
		return err
	}

	dctx, dcancel := context.WithCancel(t.Context())
	if _, err := mustDurable(t, m).Consume(dctx, subject, fresh("kept")); err != nil {
		dcancel()
		return err
	}
	if err := w.reaches(sysConnected, clients+1); err != nil {
		dcancel()
		return err
	}
	if err := w.reaches(sysSubscriptions, subs+1); err != nil {
		dcancel()
		return err
	}
	dcancel()
	if err := w.reaches(sysConnected, clients); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	if kept := w.baseline(t, sysSubscriptions); kept != subs+1 {
		return fmt.Errorf("%s is %d after a durable consumer left, expected %d", sysSubscriptions, kept, subs+1)
	}
	return nil
}

// restartBroker restarts the broker container and waits for it to report
// healthy again.
func restartBroker(t *testing.T) {
	t.Helper()

	if out, err := exec.CommandContext(t.Context(), "docker", "restart", container()).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v: %s", err, out)
	}
	for range 120 {
		out, _ := exec.CommandContext(t.Context(), "docker", "inspect", "--format", "{{.State.Health.Status}}", container()).Output()
		if strings.TrimSpace(string(out)) == "healthy" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s did not become healthy after a restart", container())
}

// A persistent session's queued and unacknowledged QoS 1 messages are on disk:
// the broker restarts, the consumer comes back, and gets exactly what it had
// not acknowledged, headers and all.
func TestE2EASessionSurvivesABrokerRestart(t *testing.T) {
	if os.Getenv("STREAMS_E2E_RESTART") != "1" {
		t.Skipf("set STREAMS_E2E_RESTART=1 to restart %s", container())
	}
	prefix := freshFor(app, "restart")
	consumer := fresh("ledger")
	s := connectAs(t, app, prefix)
	_, m := declareWith(t, s, streams.Stream{ID: "ledger", Name: "ledger", Subjects: []string{subject}})

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := mustDurable(t, m).Consume(ctx, subject, consumer)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	acked, _ := m.Publish(t.Context(), subject, event{User: "acked"})
	unacked, _ := m.Publish(t.Context(), subject, event{User: "unacked"}, streams.Header("phase", "in-flight"))
	if got := recv(t, ch, within); got.ID != acked {
		t.Fatalf("first = %q, want %q", got.ID, acked)
	} else {
		_ = got.Ack(ctx)
	}
	if got := recv(t, ch, within); got.ID != unacked {
		t.Fatalf("second = %q, want %q", got.ID, unacked)
	} // Neither acked nor nak'd: still in flight on the session.
	settle()
	cancel()
	settle()

	queued, _ := m.Publish(t.Context(), subject, event{User: "queued"}, streams.Header("phase", "queued"))
	// Past the autosave interval, though shutdown saves too.
	time.Sleep(1500 * time.Millisecond)

	restartBroker(t)

	// The publishing connection redials on its own, over the same TLS, and a
	// publish waits for it as long as ctx allows.
	pctx, pcancel := context.WithTimeout(t.Context(), 30*time.Second)
	after, err := m.Publish(pctx, subject, event{User: "after"})
	pcancel()
	if err != nil {
		t.Fatalf("the publisher never came back after the restart: %v", err)
	}
	_ = s.(streams.Closer).Close()

	// A new process: new streams, the same declaration, the same name.
	_, again := declareWith(t, connectAs(t, app, prefix), streams.Stream{ID: "ledger", Name: "ledger", Subjects: []string{subject}})
	resumed, err := mustDurable(t, again).Consume(t.Context(), subject, consumer)
	if err != nil {
		t.Fatalf("Consume (resumed): %v", err)
	}
	got := drain(t, resumed, 3*time.Second)
	if fmt.Sprint(ids(got)) != fmt.Sprint([]string{unacked, queued, after}) {
		t.Errorf("after the restart %v, want the in-flight %s, the queued %s and the later %s, in order (never the acked %s)",
			ids(got), unacked, queued, after, acked)
	}
	phases := make([]string, 0, len(got))
	for _, d := range got {
		phases = append(phases, d.Headers.Get("phase"))
	}
	if fmt.Sprint(phases) != fmt.Sprint([]string{"in-flight", "queued", ""}) {
		t.Errorf("headers after the restart = %q", phases)
	}
}

// mosquittoSub runs the broker's own subscriber inside its container, on the
// certificate-identity listener, as MQTT 5, printing each message's user
// properties and topic: another tool on the broker reading what was written.
func mosquittoSub(ctx context.Context, topic string) *exec.Cmd {
	return exec.CommandContext(ctx, "docker", "exec", container(), "mosquitto_sub", //nolint:misspell // the broker binary's name
		"-h", "localhost", "-p", "8883",
		"--cafile", "/certs/ca.crt", "--cert", "/certs/client.crt", "--key", "/certs/client.key",
		"-V", "mqttv5", "-F", "%P %t", "-t", topic, "-C", "1", "-W", "15")
}

func TestE2EBrokerToolsSeeTheHeaders(t *testing.T) {
	prefix := fresh("native")
	s := connectCert(t, prefix)
	stream, m := declareWith(t, s, streams.Stream{ID: "native", Name: "native", Subjects: []string{subject}}.WithHeader("lang", "go"))
	topic := prefix + "/native/" + subject

	// Retained, so the tool started afterwards is handed it at once.
	if _, err := m.Publish(t.Context(), subject, event{User: "ada"}, streams.Retain(), streams.Header("site", "plant-3")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	out, err := mosquittoSub(t.Context(), topic).Output()
	if err != nil {
		t.Fatalf("broker subscriber: %v", err)
	}
	if got, want := strings.TrimSpace(string(out)), "lang:go site:plant-3 "+topic; got != want {
		t.Errorf("broker subscriber printed %q, want %q", got, want)
	}
	if err := streamsmqtt.ClearRetained(t.Context(), s, stream.ID, subject); err != nil {
		t.Fatalf("ClearRetained: %v", err)
	}

	// Live, not retained: the tool subscribes first, and the provider publishes
	// until it has heard one.
	cmd := mosquittoSub(t.Context(), topic)
	var buf strings.Builder
	cmd.Stdout = &buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("broker subscriber: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
publishing:
	for range 50 {
		if _, err := m.Publish(t.Context(), subject, event{User: "grace"}, streams.Header("traceparent", "00-ab")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case <-exited:
			break publishing
		case <-time.After(300 * time.Millisecond):
		}
	}
	if got, want := strings.TrimSpace(buf.String()), "lang:go traceparent:00-ab "+topic; got != want {
		t.Errorf("broker subscriber printed %q, want %q", got, want)
	}
}

func TestE2EHeadersOverrideSurviveRedeliveryAndRefuseWhatIsInvalid(t *testing.T) {
	s := connectAs(t, app, freshFor(app, "headers"))
	_, m := declareWith(t, s, streams.Stream{ID: "h", Name: "h", Subjects: []string{subject}}.
		WithHeaders(streams.Headers{"lang": "go", "site": "plant-3"}))
	consumer := fresh("hdr")

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := mustDurable(t, m).Consume(ctx, subject, consumer)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	sent, _ := m.Publish(t.Context(), subject, event{User: "a"}, streams.Header("site", "plant-4"))
	got := recv(t, ch, within)
	if got.ID != sent || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-4" {
		t.Errorf("delivered %q with %v", got.ID, got.Headers)
	}
	if got.CanDelayRedelivery() {
		t.Error("CanDelayRedelivery = true on MQTT")
	}
	if nerr := got.NakAfter(ctx, time.Second); !errors.Is(nerr, streams.ErrUnsupported) {
		t.Errorf("NakAfter = %v, want ErrUnsupported", nerr)
	}
	settle()
	cancel()
	settle()

	// NakAfter left it outstanding: the session hands it back on resume,
	// headers and all.
	again, err := mustDurable(t, m).Consume(t.Context(), subject, consumer)
	if err != nil {
		t.Fatalf("Consume (again): %v", err)
	}
	re := recv(t, again, within)
	if re.ID != sent || re.Headers.Get("site") != "plant-4" {
		t.Errorf("redelivered %q with %v", re.ID, re.Headers)
	}
	_ = re.Ack(t.Context())

	// A message published without headers round-trips without any.
	_, plain := declareWith(t, s, streams.Stream{ID: "plain", Name: "plain", Subjects: []string{subject}})
	pch, _ := plain.Subscribe(t.Context(), subject)
	id, _ := plain.Publish(t.Context(), subject, event{User: "go"})
	if got := recv(t, pch, within); got.ID != id || got.Headers != nil {
		t.Errorf("plain delivered %q with %v", got.ID, got.Headers)
	}

	// Invalid headers fail at the call that set them.
	if _, err := s.Create(t.Context(), streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader("a:b", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create = %v", err)
	}
	if _, err := s.Update(t.Context(), "h", streams.Stream{Name: "h", Subjects: []string{subject}}.WithHeader("k", "a\rb")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update = %v", err)
	}
	if _, err := m.Publish(t.Context(), subject, event{}, streams.Header("", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish = %v", err)
	}
}

func TestE2EExpiryDropsWhatWaitedTooLong(t *testing.T) {
	s := connectAs(t, app, freshFor(app, "expiry"))
	_, m := declare(t, s, subject)
	d := mustDurable(t, m)
	consumer := fresh("exp")

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := d.Consume(ctx, subject, consumer); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	settle()
	cancel()
	settle()
	short, _ := m.Publish(t.Context(), subject, event{User: "short"}, streams.Expires(400*time.Millisecond))
	long, _ := m.Publish(t.Context(), subject, event{User: "long"}, streams.Expires(2*time.Minute))
	forever, _ := m.Publish(t.Context(), subject, event{User: "forever"})
	_, _ = m.Publish(t.Context(), subject, event{User: "retained"}, streams.Retain(), streams.Expires(time.Second))
	time.Sleep(2500 * time.Millisecond)

	resumed, err := d.Consume(t.Context(), subject, consumer)
	if err != nil {
		t.Fatalf("Consume (resumed): %v", err)
	}
	got := ids(drain(t, resumed, 2*time.Second))
	for _, id := range got {
		if id == short {
			t.Error("the expired message was delivered")
		}
	}
	if len(got) < 2 || got[0] != long || got[1] != forever {
		t.Errorf("resumed with %v, want %s then %s", got, long, forever)
	}
	late, _ := m.Subscribe(t.Context(), subject)
	quiet(t, late, time.Second, "a later subscriber, of an expired retained value")
}

func TestE2ERetainReachesLaterSubscribersOnceAndClears(t *testing.T) {
	prefix := freshFor(app, "retain")
	s := connectAs(t, app, prefix)
	_, m := declareWith(t, s, streams.Stream{ID: "r", Name: "r", Subjects: []string{subject}})
	sent, err := m.Publish(t.Context(), subject, event{User: "last"}, streams.Retain(), streams.Header("lang", "go"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	_, late := declareWith(t, connectAs(t, app, prefix), streams.Stream{ID: "r", Name: "r", Subjects: []string{subject}})
	sub, err := late.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	got := recv(t, sub, within)
	var e event
	if derr := got.Decode(&e); derr != nil || got.ID != sent || e.User != "last" || got.Headers.Get("lang") != "go" {
		t.Errorf("retained %q %+v %v (%v)", got.ID, e, got.Headers, derr)
	}

	consumer := fresh("ret")
	ctx, cancel := context.WithCancel(t.Context())
	first, _ := mustDurable(t, m).Consume(ctx, subject, consumer)
	if d := recv(t, first, within); d.ID != sent {
		t.Errorf("durable consumer handed %q, want %q", d.ID, sent)
	} else {
		_ = d.Ack(ctx)
	}
	settle()
	cancel()
	settle()
	resumed, _ := mustDurable(t, m).Consume(t.Context(), subject, consumer)
	if got := drain(t, resumed, time.Second); len(got) != 0 {
		t.Errorf("the retained value came back on resume: %v", ids(got))
	}

	if err := streamsmqtt.ClearRetained(t.Context(), s, "r", subject); err != nil {
		t.Fatalf("ClearRetained: %v", err)
	}
	quiet(t, sub, 500*time.Millisecond, "a live subscriber, of the clear")
	later, _ := late.Subscribe(t.Context(), subject)
	quiet(t, later, time.Second, "a subscriber after the clear")
}
