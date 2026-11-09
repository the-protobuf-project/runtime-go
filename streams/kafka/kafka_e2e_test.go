//go:build live

package kafka_test

// The Kafka provider end to end, over mutual TLS and over SASL/SCRAM on TLS,
// against a durable broker with ACLs on.
//
// Start one from streams/ with
//
//	docker compose -p tpp-streams-e2e-kafka -f docker/compose.kafka.secure.yaml up -d --wait
//
// (certificates from docker/tls/generate.sh), then
// go test -race -tags live ./kafka -run E2E.
//
// KAFKA_E2E_ADDR overrides the mutual-TLS listener (default 127.0.0.1:19093),
// KAFKA_E2E_SASL_ADDR the SASL_SSL one (default 127.0.0.1:19094), and
// STREAMS_E2E_TLS_DIR the certificate directory (default docker/tls/out). The
// SCRAM user, its password and the prefix its ACLs cover are read from
// docker/compose.kafka.secure.env, as the broker reads them. The restart test
// runs only with STREAMS_E2E_RESTART=1, because it restarts the
// tpp-streams-kafka-secure container. Tests skip when the broker is not
// reachable.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamskafka "github.com/the-protobuf-project/runtime-go/streams/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	other           = "user.deleted"
	secureContainer = "tpp-streams-kafka-secure"
)

// e2eAddr is the mutual-TLS listener.
func e2eAddr() string {
	if addr := os.Getenv("KAFKA_E2E_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:19093"
}

// saslAddr is the SASL_SSL listener.
func saslAddr() string {
	if addr := os.Getenv("KAFKA_E2E_SASL_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:19094"
}

// cert is the path of a test certificate, failing with how to generate it
// when it is missing.
func cert(t testing.TB, name string) string {
	t.Helper()
	dir := os.Getenv("STREAMS_E2E_TLS_DIR")
	if dir == "" {
		dir = filepath.Join("..", "docker", "tls", "out")
	}
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s is missing; run docker/tls/generate.sh", path)
	}
	return path
}

// tlsConfig verifies the broker against ca and, when mutual, presents the
// client certificate.
func tlsConfig(t testing.TB, ca string, mutual bool) *tls.Config {
	t.Helper()
	pem, err := os.ReadFile(cert(t, ca))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if mutual {
		pair, err := tls.LoadX509KeyPair(cert(t, "client.crt"), cert(t, "client.key"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg
}

// scramValue is one value from docker/compose.kafka.secure.env.
func scramValue(t testing.TB, key string) string {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "docker", "compose.kafka.secure.env"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if name, value, ok := strings.Cut(sc.Text(), "="); ok && strings.TrimSpace(name) == key {
			return strings.TrimSpace(value)
		}
	}
	t.Fatalf("%s is not set in compose.kafka.secure.env", key)
	return ""
}

// auth is who a test connects as.
type auth int

const (
	// mtls is the client certificate on the mutual-TLS listener, allowed
	// everything.
	mtls auth = iota
	// scramUser is the SCRAM user on the SASL_SSL listener, allowed only
	// names under its prefix.
	scramUser
)

func (a auth) String() string { return [...]string{"mtls", "scram"}[a] }

// addr is the listener a connects to.
func (a auth) addr() string {
	if a == scramUser {
		return saslAddr()
	}
	return e2eAddr()
}

// client is the franz-go options for a's listener and credentials.
func (a auth) client(t testing.TB) []kgo.Opt {
	if a == scramUser {
		return saslOpts(t, scramValue(t, "E2E_SCRAM_PASSWORD"))
	}
	return []kgo.Opt{kgo.DialTLSConfig(tlsConfig(t, "ca.crt", true))}
}

// saslOpts is TLS verifying the broker, no client certificate, and
// SCRAM-SHA-512 with password.
func saslOpts(t testing.TB, password string) []kgo.Opt {
	return []kgo.Opt{
		kgo.DialTLSConfig(tlsConfig(t, "ca.crt", false)),
		kgo.SASL(scram.Auth{User: scramValue(t, "E2E_SCRAM_USER"), Pass: password}.AsSha512Mechanism()),
	}
}

// freshPrefixFor is a topic prefix no other test uses, inside a's ACLs.
func (a auth) freshPrefix(t testing.TB) string {
	if a == scramUser {
		return scramValue(t, "E2E_SCRAM_PREFIX") + strings.TrimPrefix(freshPrefix("e2e"), "e2e-")
	}
	return freshPrefix("e2e")
}

// consumer is a consumer name a may use as a group.
func (a auth) consumer(t testing.TB, name string) string {
	if a == scramUser {
		return scramValue(t, "E2E_SCRAM_PREFIX") + name
	}
	return name
}

// connectAs connects as a under prefix with opts, without failing.
func connectAs(t testing.TB, a auth, prefix string, partitions int32, opts ...kgo.Opt) (streams.Streams, error) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return streamskafka.Connect(ctx, []string{a.addr()},
		streamskafka.WithPrefix(prefix), streamskafka.WithPartitions(partitions),
		streamskafka.WithClientOptions(append(a.client(t), opts...)...))
}

// providerAt connects as a under prefix, closed when the test ends.
func providerAt(t testing.TB, a auth, prefix string, partitions int32) streams.Streams {
	t.Helper()
	reachable(t, a.addr())
	s, err := connectAs(t, a, prefix, partitions)
	if err != nil {
		t.Fatalf("secure Kafka is not usable as %s: %v", a, err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	return s
}

// providerAs connects as a under a fresh prefix inside its ACLs.
func providerAs(t testing.TB, a auth, partitions int32) (streams.Streams, string) {
	t.Helper()
	prefix := a.freshPrefix(t)
	return providerAt(t, a, prefix, partitions), prefix
}

// refused asserts a connection with opts to addr fails, after checking the
// broker is up for a good client, so the refusal is the listener's doing.
func refused(t *testing.T, good auth, addr string, opts ...kgo.Opt) {
	t.Helper()
	providerAs(t, good, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	s, err := streamskafka.Connect(ctx, []string{addr}, streamskafka.WithClientOptions(opts...))
	if err == nil {
		_ = s.(streams.Closer).Close()
		t.Fatal("the broker accepted a client it should have refused")
	}
}

// admin is a kadm client over mutual TLS, closed when the test ends.
func admin(t testing.TB) *kadm.Client {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(e2eAddr())}, mtls.client(t)...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

// topicsGone waits until no topic carries stream's name under prefix. Topic
// deletion is asynchronous on the broker.
func topicsGone(t testing.TB, prefix, stream string) {
	t.Helper()
	marker := prefix + "." + stream + "."
	deadline := time.Now().Add(within)
	for {
		listed, err := admin(t).ListTopics(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var left []string
		for _, name := range listed.Names() {
			if strings.HasPrefix(name, marker) {
				left = append(left, name)
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("topics left behind after delete: %v", left)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// TLS is really in use
// ---------------------------------------------------------------------------

func TestE2EPlaintextIsRefusedOnTheSecurePort(t *testing.T) {
	refused(t, mtls, e2eAddr())
}

func TestE2ETLSWithoutAClientCertificateIsRefused(t *testing.T) {
	refused(t, mtls, e2eAddr(), kgo.DialTLSConfig(tlsConfig(t, "ca.crt", false)))
}

// The client certificate is not a CA, so verifying the broker against it must
// fail: the client really checks who it is talking to.
func TestE2ETLSWithAnUntrustedBrokerCAIsRefused(t *testing.T) {
	refused(t, mtls, e2eAddr(), kgo.DialTLSConfig(tlsConfig(t, "client.crt", true)))
}

// ---------------------------------------------------------------------------
// SASL/SCRAM over TLS, and ACLs
// ---------------------------------------------------------------------------

func TestE2EAWrongSCRAMPasswordIsRefused(t *testing.T) {
	refused(t, scramUser, saslAddr(), saslOpts(t, "not-the-password")...)
}

// Right credentials, but in the clear: the listener speaks only TLS.
func TestE2ESCRAMWithoutTLSIsRefused(t *testing.T) {
	refused(t, scramUser, saslAddr(),
		kgo.SASL(scram.Auth{User: scramValue(t, "E2E_SCRAM_USER"), Pass: scramValue(t, "E2E_SCRAM_PASSWORD")}.AsSha512Mechanism()))
}

// assertTopicDenied asserts err is the broker refusing the topic, carried to
// the caller as TOPIC_AUTHORIZATION_FAILED.
func assertTopicDenied(t *testing.T, s streams.Streams, err error, what string) {
	t.Helper()
	if err == nil {
		_ = s.(streams.Closer).Close()
		t.Fatalf("%s: the SCRAM user reached a topic outside its ACLs", what)
	}
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("%s: refused, but not for authorization: %v", what, err)
	}
}

// The SCRAM user's ACLs cover topics under its prefix and nothing else, whether
// the topics are new or already there.
func TestE2ETheSCRAMUserIsDeniedTopicsOutsideItsACLs(t *testing.T) {
	providerAs(t, scramUser, 1)

	s, err := connectAs(t, scramUser, freshPrefix("denied"), 1)
	assertTopicDenied(t, s, err, "a fresh prefix")

	owner, prefix := providerAs(t, mtls, 1)
	stream, _ := declare(t, owner, subject)
	s, err = connectAs(t, scramUser, prefix, 1)
	assertTopicDenied(t, s, err, "another principal's prefix")
	if err := owner.Delete(t.Context(), stream.ID); err != nil {
		t.Fatal(err)
	}
	topicsGone(t, prefix, stream.ID)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func TestE2ELifecycleOverTLS(t *testing.T)     { lifecycle(t, mtls) }
func TestE2ELifecycleOverSASLSSL(t *testing.T) { lifecycle(t, scramUser) }

// lifecycle declares, reads, lists, updates, binds, publishes, subscribes and
// deletes, as a.
func lifecycle(t *testing.T, a auth) {
	s, prefix := providerAs(t, a, 1)
	ctx := t.Context()

	// Explicit id, sanitized.
	explicit, err := s.Create(ctx, streams.Stream{
		ID: "orders:eu", Name: "orders", Description: "before",
		Subjects: []string{subject}, UserID: "u-1",
	}.WithHeader("lang", "go"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if explicit.ID != "orders-eu" || !explicit.Active || explicit.Headers.Get("lang") != "go" {
		t.Errorf("Create = %+v", explicit)
	}
	generated, err := s.Create(ctx, streams.Stream{Name: "users", Subjects: []string{subject}})
	if err != nil || generated.ID == "" || generated.ID == explicit.ID {
		t.Fatalf("Create (generated) = %+v, %v", generated, err)
	}
	if got, gerr := s.Get(ctx, "orders:eu"); gerr != nil || got.ID != explicit.ID || !maps.Equal(got.Headers, explicit.Headers) {
		t.Errorf("Get = %+v, %v", got, gerr)
	}
	list, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(list))
	for _, st := range list {
		ids = append(ids, st.ID)
		if st.ID == explicit.ID && !maps.Equal(st.Headers, explicit.Headers) {
			t.Error("List lost the default headers")
		}
	}
	want := []string{explicit.ID, generated.ID}
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Errorf("List = %v, want %v ordered by id", ids, want)
	}

	// Update adds a subject, which creates its topic.
	if _, ierr := s.Update(ctx, explicit.ID, streams.Stream{
		Name: "orders", Description: "after", Subjects: []string{subject, other}, UserID: "u-2",
	}.WithHeader("lang", "rust")); ierr != nil {
		t.Fatalf("Update: %v", ierr)
	}
	for _, bad := range []streams.Stream{
		streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader("has space", "x"),
		streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader("lang", "two\nlines"),
	} {
		if _, ierr := s.Create(ctx, bad); !errors.Is(ierr, streams.ErrInvalidHeader) {
			t.Errorf("Create = %v", ierr)
		}
		if _, ierr := s.Update(ctx, explicit.ID, bad); !errors.Is(ierr, streams.ErrInvalidHeader) {
			t.Errorf("Update = %v", ierr)
		}
	}
	got, _ := s.Get(ctx, explicit.ID)
	if got.Description != "after" || got.UserID != "u-2" || len(got.Subjects) != 2 || got.Headers.Get("lang") != "rust" {
		t.Errorf("after Update: %+v", got)
	}
	topics, err := admin(t).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{subject, other} {
		if name := prefix + "." + explicit.ID + "." + sub; !topics.Has(name) {
			t.Errorf("%s was not created", name)
		}
	}

	// Bind, publish, subscribe on the added subject.
	m, err := s.Bind(ctx, explicit.ID)
	if err != nil {
		t.Fatal(err)
	}
	subCtx, cancel := context.WithCancel(ctx)
	ch, err := m.Subscribe(subCtx, other)
	if err != nil {
		t.Fatal(err)
	}
	id, err := m.Publish(ctx, other, event{User: "ada", Action: "deleted"})
	if err != nil {
		t.Fatal(err)
	}
	msg := recvMessage(t, ch, within)
	if msg.ID != id || msg.Subject != other || msg.Headers.Get("lang") != "rust" {
		t.Errorf("delivered %q on %q with %v", msg.ID, msg.Subject, msg.Headers)
	}
	cancel()
	drain(ch)

	for _, st := range []streams.Stream{explicit, generated} {
		if ierr := s.Delete(ctx, st.ID); ierr != nil {
			t.Fatalf("Delete: %v", ierr)
		}
		if _, ierr := s.Get(ctx, st.ID); !errors.Is(ierr, streams.ErrNotFound) {
			t.Errorf("Get after Delete = %v", ierr)
		}
		topicsGone(t, prefix, st.ID)
	}
	if list, _ := s.List(ctx); len(list) != 0 {
		t.Errorf("List after deleting everything = %v", list)
	}
	if _, ierr := s.Bind(ctx, explicit.ID); !errors.Is(ierr, streams.ErrNotFound) {
		t.Errorf("Bind = %v", ierr)
	}
	if _, ierr := s.Update(ctx, explicit.ID, streams.Stream{}); !errors.Is(ierr, streams.ErrNotFound) {
		t.Errorf("Update = %v", ierr)
	}
	if ierr := s.Delete(ctx, explicit.ID); !errors.Is(ierr, streams.ErrNotFound) {
		t.Errorf("Delete = %v", ierr)
	}

	// Tails and metadata reads read without a group, so none of them leaves
	// a group behind on the broker.
	groups, err := admin(t).ListGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups.Groups() {
		if strings.HasPrefix(g, "streams-reader-") || strings.HasPrefix(g, prefix) {
			t.Errorf("a reader left group %q behind", g)
		}
	}
}

func TestE2EUndeclaredSubjectsAreRefusedEverywhere(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	ctx := t.Context()
	_, perr := m.Publish(ctx, "typo", event{})
	_, serr := m.Subscribe(ctx, "typo")
	d, _ := streams.AsDurable(m)
	_, cerr := d.Consume(ctx, "typo", "c")
	p, _ := streams.AsPositioned(m)
	_, ferr := p.ConsumeFrom(ctx, "typo", "c", streams.FromEarliest)
	b, _ := streams.AsBatch(m)
	_, berr := b.PublishBatch(ctx, "typo", batchOf(1))
	for _, err := range []error{perr, serr, cerr, ferr, berr} {
		if !errors.Is(err, streams.ErrUnknownSubject) {
			t.Errorf("%v, want ErrUnknownSubject", err)
		}
	}
	_ = s.Delete(ctx, stream.ID)
}

func TestE2EATypedViewRoundTripsOverTLS(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	typed := streams.For[event](m)
	ch, err := typed.Subscribe(ctx, subject)
	if err != nil {
		t.Fatal(err)
	}
	sent := event{User: "grace", Action: "typed"}
	if _, err := typed.Publish(ctx, subject, sent); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != sent {
			t.Errorf("received %+v, want %+v", got, sent)
		}
	case <-time.After(within):
		t.Fatal("nothing typed arrived")
	}
	cancel()
	_ = s.Delete(t.Context(), stream.ID)
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

func TestE2EOptionsAreHonouredOrRefused(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	ctx := t.Context()
	b, _ := streams.AsBatch(m)

	// id: published under the caller's id.
	subCtx, cancel := context.WithCancel(ctx)
	ch, err := m.Subscribe(subCtx, subject)
	if err != nil {
		t.Fatal(err)
	}
	id, err := m.Publish(ctx, subject, event{User: "ada", Action: "id"}, streams.ID("order-42"))
	if err != nil || id != "order-42" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	if got := recvMessage(t, ch, within); got.ID != "order-42" {
		t.Errorf("delivered %q", got.ID)
	}
	cancel()

	// ttl: Kafka cannot schedule, so it is refused rather than dropped.
	if _, ierr := m.Publish(ctx, subject, event{}, streams.TTL(time.Second)); !errors.Is(ierr, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", ierr)
	}
	if _, ierr := b.PublishBatch(ctx, subject, batchOf(1), streams.TTL(time.Second)); !errors.Is(ierr, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with a TTL = %v", ierr)
	}
	// A zero ttl means none.
	if _, ierr := m.Publish(ctx, subject, event{Action: "zero-ttl"}, streams.TTL(0)); ierr != nil {
		t.Fatal(ierr)
	}

	// expiry and retain are refused by name; so is a refused header.
	for says, opt := range map[string]streams.Option{"expiry": streams.Expires(5 * time.Second), "retain": streams.Retain()} {
		_, perr := m.Publish(ctx, subject, event{}, opt)
		_, berr := b.PublishBatch(ctx, subject, batchOf(1), opt)
		for _, err := range []error{perr, berr} {
			if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), says) {
				t.Errorf("%s: %v", says, err)
			}
		}
	}
	bad := streams.Header("has:colon", "x")
	if _, ierr := m.Publish(ctx, subject, event{}, bad); !errors.Is(ierr, streams.ErrInvalidHeader) {
		t.Errorf("Publish = %v", ierr)
	}
	if _, ierr := b.PublishBatch(ctx, subject, batchOf(1), bad); !errors.Is(ierr, streams.ErrInvalidHeader) {
		t.Errorf("PublishBatch = %v", ierr)
	}
	// id on a batch: one id cannot name several messages.
	if _, ierr := b.PublishBatch(ctx, subject, batchOf(2), streams.ID("x")); !errors.Is(ierr, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an ID = %v", ierr)
	}
	d, _ := streams.AsDurable(m)
	if _, ierr := d.Consume(ctx, subject, "billing", streams.Group("g")); !errors.Is(ierr, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v", ierr)
	}
	if _, ierr := d.Consume(ctx, subject, ""); !errors.Is(ierr, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", ierr)
	}

	// prefetch: a window of one still delivers everything, in order.
	for n := range 3 {
		if _, ierr := m.Publish(ctx, subject, event{User: "p", Action: fmt.Sprint(n)}); ierr != nil {
			t.Fatal(ierr)
		}
	}
	p, _ := streams.AsPositioned(m)
	deliveries, err := p.ConsumeFrom(ctx, subject, "prefetch", streams.FromEarliest, streams.Prefetch(1))
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for len(seen) < 5 {
		dl := recvDelivery(t, deliveries, within)
		var e event
		_ = dl.Decode(&e)
		seen = append(seen, e.Action)
		_ = dl.Ack(ctx)
	}
	if !slices.Equal(seen, []string{"id", "zero-ttl", "0", "1", "2"}) {
		t.Errorf("delivered %v", seen)
	}
	_ = s.Delete(ctx, stream.ID)
}

// Messages sharing a key keep their order across a four-partition topic.
func TestE2EAPartitionKeyKeepsOrder(t *testing.T) {
	s, _ := providerAs(t, mtls, 4)
	stream, m := declare(t, s, subject)
	for n := range 10 {
		if _, err := m.Publish(t.Context(), subject, event{User: "ada", Action: fmt.Sprint(n)},
			streams.PartitionKey("ada")); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := streams.AsPositioned(m)
	ch, err := p.ConsumeFrom(t.Context(), subject, "ordering", streams.FromEarliest)
	if err != nil {
		t.Fatal(err)
	}
	for n := range 10 {
		d := recvDelivery(t, ch, within)
		var e event
		_ = d.Decode(&e)
		if e.Action != fmt.Sprint(n) {
			t.Fatalf("received %q, want %d", e.Action, n)
		}
		_ = d.Ack(t.Context())
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// With a group, members share the subject and each message reaches one of
// them; at-least-once may hand one to both during a rebalance, but the
// subject is not fanned out.
func TestE2EAGroupSharesASubject(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	a, err := m.Subscribe(ctx, subject, streams.Group("workers"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Subscribe(ctx, subject, streams.Group("workers"))
	if err != nil {
		t.Fatal(err)
	}

	// The second member's join can rebalance the first, so probe until three
	// probes in a row arrive promptly: by then both members have joined.
	deadline := time.Now().Add(60 * time.Second)
	for streak := 0; streak < 3; {
		if time.Now().After(deadline) {
			t.Fatal("the group never settled")
		}
		probe, err := m.Publish(ctx, subject, event{User: "probe"})
		if err != nil {
			t.Fatal(err)
		}
		landed := false
		wait := time.After(time.Second)
	probing:
		for {
			select {
			case msg := <-a:
				if msg.ID == probe {
					landed = true
					break probing
				}
			case msg := <-b:
				if msg.ID == probe {
					landed = true
					break probing
				}
			case <-wait:
				break probing
			}
		}
		if landed {
			streak++
		} else {
			streak = 0
		}
	}

	const total = 10
	want := map[string]bool{}
	for n := range total {
		id, err := m.Publish(ctx, subject, event{User: "w", Action: fmt.Sprint(n)})
		if err != nil {
			t.Fatal(err)
		}
		want[id] = true
	}
	seenA, seenB := map[string]bool{}, map[string]bool{}
	collect := time.After(within)
	missing := maps.Clone(want)
	for len(missing) > 0 {
		select {
		case msg := <-a:
			seenA[msg.ID] = true
			delete(missing, msg.ID)
		case msg := <-b:
			seenB[msg.ID] = true
			delete(missing, msg.ID)
		case <-collect:
			t.Fatalf("grouped messages never arrived: %v", missing)
		}
	}
	all := func(seen map[string]bool) bool {
		for id := range want {
			if !seen[id] {
				return false
			}
		}
		return true
	}
	if all(seenA) && all(seenB) {
		t.Error("both members received every message: a group fanned out")
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

func TestE2EEveryCapabilityIsPresent(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	if _, err := streams.AsDurable(m); err != nil {
		t.Error(err)
	}
	if _, err := streams.AsPositioned(m); err != nil {
		t.Error(err)
	}
	if _, err := streams.AsBatch(m); err != nil {
		t.Error(err)
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// Ack commits; nak leaves the offset where it was, so the name reads the
// message again when it next attaches, with an attempt of zero both times.
func TestE2EAckCommitsAndNakRedelivers(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	for _, who := range []string{"handled", "retried"} {
		if _, err := m.Publish(t.Context(), subject, event{User: who}); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := streams.AsPositioned(m)
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := p.ConsumeFrom(ctx, subject, "billing", streams.FromEarliest)
	if err != nil {
		t.Fatal(err)
	}
	first := recvDelivery(t, ch, within)
	var e event
	_ = first.Decode(&e)
	if e.User != "handled" || first.Attempt != 0 {
		t.Errorf("first = %+v attempt %d", e, first.Attempt)
	}
	_ = first.Ack(ctx)
	second := recvDelivery(t, ch, within)
	_ = second.Nak(ctx)
	cancel()
	drain(ch)

	d, _ := streams.AsDurable(m)
	again, err := d.Consume(t.Context(), subject, "billing")
	if err != nil {
		t.Fatal(err)
	}
	redelivered := recvDelivery(t, again, within)
	if redelivered.ID != second.ID || redelivered.Attempt != 0 {
		t.Errorf("redelivered %q attempt %d, want %q", redelivered.ID, redelivered.Attempt, second.ID)
	}
	_ = redelivered.Ack(t.Context())
	_ = s.Delete(t.Context(), stream.ID)
}

// Earliest replays history to a fresh name; New does not.
func TestE2EPositionedReplay(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	old, err := m.Publish(t.Context(), subject, event{User: "old"})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := streams.AsPositioned(m)
	ctx, cancel := context.WithCancel(t.Context())
	replay, err := p.ConsumeFrom(ctx, subject, "replay", streams.FromEarliest)
	if err != nil {
		t.Fatal(err)
	}
	if first := recvDelivery(t, replay, within); first.ID != old {
		t.Errorf("replayed %q, want %q", first.ID, old)
	}
	cancel()
	drain(replay)

	// A fresh name at New starts at the end. Its group joins after
	// ConsumeFrom returns, so probe until it reads.
	tail, err := p.ConsumeFrom(t.Context(), subject, "tail", streams.FromNew)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(within)
	for {
		if time.Now().After(deadline) {
			t.Fatal("the New consumer never read")
		}
		if _, err := m.Publish(t.Context(), subject, event{User: "new"}); err != nil {
			t.Fatal(err)
		}
		select {
		case d := <-tail:
			if d.ID == old {
				t.Fatal("New replayed history")
			}
			_ = d.Ack(t.Context())
			_ = s.Delete(t.Context(), stream.ID)
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func TestE2EANativeBatchDeliversEveryEntry(t *testing.T) {
	s, _ := providerAs(t, mtls, 1)
	stream, m := declare(t, s, subject)
	b, _ := streams.AsBatch(m)
	const total = 100
	ids, err := b.PublishBatch(t.Context(), subject, batchOf(total))
	if err != nil {
		t.Fatal(err)
	}
	unique := map[string]bool{}
	for _, id := range ids {
		unique[id] = true
	}
	if len(unique) != total {
		t.Fatalf("%d unique ids", len(unique))
	}
	p, _ := streams.AsPositioned(m)
	ch, err := p.ConsumeFrom(t.Context(), subject, "batch", streams.FromEarliest)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for range total {
		d := recvDelivery(t, ch, within)
		if !unique[d.ID] {
			t.Errorf("an id nobody published: %q", d.ID)
		}
		seen[d.ID] = true
		_ = d.Ack(t.Context())
	}
	if !maps.Equal(seen, unique) {
		t.Error("not every published id arrived")
	}
	if ids, err := b.PublishBatch(t.Context(), subject, nil); err != nil || len(ids) != 0 {
		t.Errorf("an empty batch = %v, %v", ids, err)
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

// Over both listeners: the stream's defaults and a publish's own arrive on a
// tail, a durable consumer and a batch, as record headers Kafka's own tools
// print; a publish without headers writes none.
func TestE2EHeadersTravelAsRecordHeaders(t *testing.T) {
	for _, a := range []auth{mtls, scramUser} {
		t.Run(a.String(), func(t *testing.T) {
			s, prefix := providerAs(t, a, 1)
			stream, err := s.Create(t.Context(), streams.Stream{Name: "headers", Subjects: []string{subject}}.
				WithHeaders(streams.Headers{"lang": "go", "site": "plant-3"}))
			if err != nil {
				t.Fatal(err)
			}
			m, err := s.Bind(t.Context(), stream.ID)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			tail, err := m.Subscribe(ctx, subject)
			if err != nil {
				t.Fatal(err)
			}
			if _, ierr := m.Publish(ctx, subject, event{User: "ada", Action: "one"}, streams.Header("site", "plant-4")); ierr != nil {
				t.Fatal(ierr)
			}
			if got := recvMessage(t, tail, within); got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-4" {
				t.Errorf("tail headers %v", got.Headers)
			}
			cancel()
			drain(tail)
			b, _ := streams.AsBatch(m)
			if _, ierr := b.PublishBatch(t.Context(), subject, batchOf(2), streams.Header("traceparent", "00-ab-cd-01")); ierr != nil {
				t.Fatal(ierr)
			}

			p, _ := streams.AsPositioned(m)
			cctx, ccancel := context.WithCancel(t.Context())
			ch, err := p.ConsumeFrom(cctx, subject, a.consumer(t, "headers"), streams.FromEarliest)
			if err != nil {
				t.Fatal(err)
			}
			first := recvDelivery(t, ch, within)
			if first.Headers.Get("site") != "plant-4" || first.CanDelayRedelivery() {
				t.Errorf("first headers %v", first.Headers)
			}
			_ = first.Ack(cctx)
			for range 2 {
				d := recvDelivery(t, ch, within)
				if d.Headers.Get("traceparent") != "00-ab-cd-01" || d.Headers.Get("site") != "plant-3" {
					t.Errorf("batch headers %v", d.Headers)
				}
				_ = d.Ack(cctx)
			}
			ccancel()
			drain(ch)

			// What a header-less stream and publish write: no header block.
			plain, _ := declare(t, s, subject)
			pm, _ := s.Bind(t.Context(), plain.ID)
			pctx, pcancel := context.WithCancel(t.Context())
			ptail, err := pm.Subscribe(pctx, subject)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pm.Publish(pctx, subject, event{User: "go"}); err != nil {
				t.Fatal(err)
			}
			if got := recvMessage(t, ptail, within); len(got.Headers) != 0 {
				t.Errorf("a header-less publish arrived with %v", got.Headers)
			}
			pcancel()
			drain(ptail)

			printed := consoleHeaders(t, secureContainer, "127.0.0.1:29092", prefix+"."+stream.ID+"."+subject, 3)
			if !slices.Equal(printed, []string{
				"lang:go,site:plant-4",
				"lang:go,site:plant-3,traceparent:00-ab-cd-01",
				"lang:go,site:plant-3,traceparent:00-ab-cd-01",
			}) {
				t.Errorf("console consumer printed %q", printed)
			}
			if printed := consoleHeaders(t, secureContainer, "127.0.0.1:29092", prefix+"."+plain.ID+"."+subject, 1); !slices.Equal(printed, []string{"NO_HEADERS"}) {
				t.Errorf("console consumer printed %q for a header-less record", printed)
			}
			for _, id := range []string{stream.ID, plain.ID} {
				_ = s.Delete(t.Context(), id)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Durability across a broker restart
// ---------------------------------------------------------------------------

// health is the broker container's docker health status.
func health(t testing.TB) string {
	out, _ := exec.CommandContext(t.Context(), "docker", "inspect", "-f", "{{.State.Health.Status}}", secureContainer).Output()
	return strings.TrimSpace(string(out))
}

// restartBroker restarts the broker container and waits, up to 180s, until it
// is healthy and both client listeners answer three metadata reads in a row.
func restartBroker(t *testing.T) {
	if out, err := exec.CommandContext(t.Context(), "docker", "restart", "-t", "30", secureContainer).CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s: %v: %s", secureContainer, err, out)
	}
	deadline := time.Now().Add(180 * time.Second)
	for health(t) != "healthy" {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become healthy after restart", secureContainer)
		}
		time.Sleep(time.Second)
	}
	// The healthcheck speaks to the in-container listener; wait until both
	// client listeners also answer, so later tests do not meet a broker still
	// settling.
	for streak := 0; streak < 3; {
		ok := true
		for _, a := range []auth{mtls, scramUser} {
			cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(a.addr())}, a.client(t)...)...)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			if err := cl.Ping(ctx); err != nil {
				ok = false
			}
			cancel()
			cl.Close()
		}
		if ok {
			streak++
		} else {
			streak = 0
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s client listeners never settled after restart", secureContainer)
		}
		time.Sleep(time.Second)
	}
}

// seeded is what one principal left on the broker before the restart.
type seeded struct {
	auth           auth
	prefix, stream string
	acked, pending string
}

// seed declares a stream as a, publishes two messages, and acks only the first
// under a named consumer.
func seed(t *testing.T, a auth) seeded {
	prefix := a.freshPrefix(t)
	reachable(t, a.addr())
	s, err := connectAs(t, a, prefix, 1)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := s.Create(t.Context(), streams.Stream{Name: "e2e", Subjects: []string{subject}}.WithHeader("lang", "go"))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := s.Bind(t.Context(), stream.ID)
	acked, err := m.Publish(t.Context(), subject, event{User: "acked"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := m.Publish(t.Context(), subject, event{User: "pending"}, streams.Header("attempt", "first"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := streams.AsPositioned(m)
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := p.ConsumeFrom(ctx, subject, a.consumer(t, "survivor"), streams.FromEarliest)
	if err != nil {
		t.Fatal(err)
	}
	first := recvDelivery(t, ch, within)
	if first.ID != acked {
		t.Fatalf("first = %q, want %q", first.ID, acked)
	}
	_ = first.Ack(ctx)
	if second := recvDelivery(t, ch, within); second.ID != pending {
		t.Fatalf("second = %q, want %q", second.ID, pending)
	}
	// Left unsettled on purpose.
	cancel()
	drain(ch)
	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatal(err)
	}
	return seeded{auth: a, prefix: prefix, stream: stream.ID, acked: acked, pending: pending}
}

// verify checks, as a freshly built provider sees it after the restart, that
// the declaration, the committed offset and the whole log survived, and that
// the broker takes new publishes; then cleans up.
func verify(t *testing.T, sd seeded) {
	a := sd.auth
	s := providerAt(t, a, sd.prefix, 1)
	stream, err := s.Get(t.Context(), sd.stream)
	if err != nil {
		t.Fatalf("%s: the declaration did not survive: %v", a, err)
	}
	if !slices.Equal(stream.Subjects, []string{subject}) || stream.Headers.Get("lang") != "go" {
		t.Errorf("%s: declaration after restart = %+v", a, stream)
	}
	m, _ := s.Bind(t.Context(), sd.stream)

	// The committed offset survived: the name resumes at the unacked message.
	d, _ := streams.AsDurable(m)
	ctx, cancel := context.WithCancel(t.Context())
	resumed, err := d.Consume(ctx, subject, a.consumer(t, "survivor"))
	if err != nil {
		t.Fatal(err)
	}
	got := recvDelivery(t, resumed, within)
	if got.ID != sd.pending {
		t.Errorf("%s: resumed at %q, want the unacked %q", a, got.ID, sd.pending)
	}
	if got.Headers.Get("lang") != "go" || got.Headers.Get("attempt") != "first" {
		t.Errorf("%s: the redelivery lost its headers: %v", a, got.Headers)
	}
	_ = got.Ack(ctx)
	cancel()
	drain(resumed)

	// The log survived whole: a fresh name replays both from the start.
	p, _ := streams.AsPositioned(m)
	rctx, rcancel := context.WithCancel(t.Context())
	replay, err := p.ConsumeFrom(rctx, subject, a.consumer(t, "auditor"), streams.FromEarliest)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{sd.acked, sd.pending} {
		dl := recvDelivery(t, replay, within)
		if dl.ID != want || dl.Headers.Get("lang") != "go" {
			t.Errorf("%s: replayed %q %v, want %q", a, dl.ID, dl.Headers, want)
		}
		_ = dl.Ack(rctx)
	}
	rcancel()
	drain(replay)

	// And the restarted broker takes new publishes.
	tctx, tcancel := context.WithCancel(t.Context())
	tail, err := m.Subscribe(tctx, subject)
	if err != nil {
		t.Fatal(err)
	}
	id, err := m.Publish(tctx, subject, event{User: "after"})
	if err != nil {
		t.Fatal(err)
	}
	if msg := recvMessage(t, tail, within); msg.ID != id {
		t.Errorf("%s: delivered %q, want %q", a, msg.ID, id)
	}
	tcancel()
	drain(tail)

	if err := s.Delete(t.Context(), sd.stream); err != nil {
		t.Fatal(err)
	}
	topicsGone(t, sd.prefix, sd.stream)
}

// An acked message stays acked and an unacked one is redelivered after the
// broker restarts; the declaration and the log survive too. Checked for both
// principals across one restart: the SCRAM user and its ACLs live in the
// metadata log, so they must survive with everything else.
func TestE2EAcknowledgedStateSurvivesABrokerRestart(t *testing.T) {
	if os.Getenv("STREAMS_E2E_RESTART") != "1" {
		t.Skipf("set STREAMS_E2E_RESTART=1 to restart %s", secureContainer)
	}
	seededAll := []seeded{seed(t, mtls), seed(t, scramUser)}
	restartBroker(t)
	for _, sd := range seededAll {
		verify(t, sd)
	}
}
