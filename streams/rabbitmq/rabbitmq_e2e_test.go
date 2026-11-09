//go:build live

package rabbitmq_test

// The provider end to end over mutual TLS, against a broker that keeps its
// data across a restart, ported from runtime-rs's tests/rabbitmq_e2e.rs. The
// client certificate is also the login: the broker offers only SASL EXTERNAL
// and maps the certificate's CN, tpp-streams-e2e, to the one user it has.
//
//	docker/tls/generate.sh   # once, if docker/tls/out is empty
//	docker compose -p tpp-streams-e2e-rabbitmq -f ../docker/compose.rabbitmq.secure.yaml up -d --wait
//	STREAMS_E2E_RESTART=1 go test -race -tags live ./rabbitmq/... -run E2E
//
// RABBITMQ_E2E_ADDR (default 127.0.0.1:15671) and STREAMS_E2E_TLS_DIR
// (default docker/tls/out) point it elsewhere; RABBITMQ_E2E_CONTAINER names
// the container inspected for leftovers and restarted. The restart test runs
// only with STREAMS_E2E_RESTART=1, so it never bounces a broker someone else
// is using by accident. Everything skips when nothing is listening.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

// certUser is the user the broker derives from the client certificate's CN.
const certUser = "tpp-streams-e2e"

// e2eQuiet is how long silence must last before a test calls a message absent.
const e2eQuiet = 700 * time.Millisecond

// e2eAddr is the AMQPS listener.
func e2eAddr() string {
	if a := os.Getenv("RABBITMQ_E2E_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:15671"
}

// e2eContainer is the secure broker's container, for rabbitmqctl and the
// restart test.
func e2eContainer() string {
	if c := os.Getenv("RABBITMQ_E2E_CONTAINER"); c != "" {
		return c
	}
	return "tpp-streams-rabbitmq-secure"
}

// tlsDir is the test certificate directory.
func tlsDir() string {
	if d := os.Getenv("STREAMS_E2E_TLS_DIR"); d != "" {
		return d
	}
	return filepath.Join("..", "docker", "tls", "out")
}

// amqpsExternal is AMQPS with SASL EXTERNAL: no username or password, the
// certificate logs in.
func amqpsExternal() string { return "amqps://" + e2eAddr() + "/?auth_mechanism=external" }

// mutualTLS trusts the test CA and, with client, presents the client
// certificate.
func mutualTLS(t *testing.T, ca, client bool) *tls.Config {
	t.Helper()
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca {
		pem, err := os.ReadFile(filepath.Join(tlsDir(), "ca.crt"))
		if err != nil {
			t.Skipf("cannot read the test CA (%v); run docker/tls/generate.sh", err)
		}
		cfg.RootCAs = x509.NewCertPool()
		cfg.RootCAs.AppendCertsFromPEM(pem)
	}
	if client {
		pair, err := tls.LoadX509KeyPair(filepath.Join(tlsDir(), "client.crt"), filepath.Join(tlsDir(), "client.key"))
		if err != nil {
			t.Skipf("cannot read the client certificate (%v); run docker/tls/generate.sh", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg
}

// requireBroker skips when the secure broker is not listening.
func requireBroker(t *testing.T) {
	t.Helper()
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(t.Context(), "tcp", e2eAddr())
	if err != nil {
		t.Skipf("no RabbitMQ over mTLS at %s (%v); see docker/compose.rabbitmq.secure.yaml", e2eAddr(), err)
	}
	_ = conn.Close()
}

// secureStreams connects over mutual TLS, logging in by certificate, under a
// fresh prefix.
func secureStreams(t *testing.T, prefix string, opts ...streamsrabbit.Option) streams.Streams {
	t.Helper()
	requireBroker(t)
	opts = append([]streamsrabbit.Option{
		streamsrabbit.WithPrefix(prefix), streamsrabbit.WithTLS(mutualTLS(t, true, true)),
	}, opts...)
	s, err := streamsrabbit.Connect(t.Context(), amqpsExternal(), opts...)
	if err != nil {
		t.Fatalf("no RabbitMQ over mTLS at %s: %v", e2eAddr(), err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	return s
}

// secureRaw is a raw mutual-TLS connection, for reading the broker as another
// client would.
func secureRaw(t *testing.T) *amqp.Connection {
	t.Helper()
	conn, err := amqp.DialTLS(amqpsExternal(), mutualTLS(t, true, true))
	if err != nil {
		t.Fatalf("DialTLS: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// e2ePrefix is unique to this process and call.
func e2ePrefix() string { return "e2e" + prefix() }

// rabbitmqctl runs rabbitmqctl -q args inside the broker container.
func rabbitmqctl(t *testing.T, args ...string) []string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "docker",
		append([]string{"exec", e2eContainer(), "rabbitmqctl", "-q"}, args...)...).Output()
	if err != nil {
		t.Fatalf("rabbitmqctl %v: %v", args, err)
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// onBroker names the broker's exchanges or queues starting with prefix.
func onBroker(t *testing.T, what, prefix string) []string {
	t.Helper()
	var names []string
	for _, n := range rabbitmqctl(t, what, "name") {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return names
}

// deleteQueues removes the queues under prefix, which a durable consumer
// leaves behind by design.
func deleteQueues(t *testing.T, conn *amqp.Connection, prefix string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	for _, q := range onBroker(t, "list_queues", prefix) {
		if _, err := ch.QueueDelete(q, false, false, false); err != nil {
			t.Fatalf("QueueDelete %s: %v", q, err)
		}
	}
}

// refused fails with accepted unless dialing url with cfg is refused.
func refused(t *testing.T, url string, cfg *tls.Config, accepted string) {
	t.Helper()
	requireBroker(t)
	done := make(chan error, 1)
	go func() {
		conn, err := amqp.DialConfig(url, amqp.Config{TLSClientConfig: cfg, Dial: amqp.DefaultDial(5 * time.Second)})
		if err == nil {
			_ = conn.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal(accepted)
		}
		t.Logf("refused as expected: %v", err)
	case <-time.After(liveWait):
		t.Log("refused as expected: the handshake hung")
	}
}

func TestE2EPlaintextIsRefused(t *testing.T) {
	refused(t, "amqp://guest:guest@"+e2eAddr()+"/", nil, "a plaintext AMQP connection was accepted")
}

// A trusted certificate on the wire, but asking to log in as guest/guest with
// PLAIN: the broker offers only EXTERNAL, and has no guest.
func TestE2EPlainGuestIsRefused(t *testing.T) {
	refused(t, "amqps://guest:guest@"+e2eAddr()+"/", mutualTLS(t, true, true), "a PLAIN guest/guest login was accepted")
	rows := rabbitmqctl(t, "list_users", "--no-table-headers")
	users := make([]string, 0, len(rows))
	for _, row := range rows {
		users = append(users, strings.Fields(row)[0])
	}
	if !reflect.DeepEqual(users, []string{certUser}) {
		t.Errorf("the broker's users: %v", users)
	}
}

// The broker lists the store's connection under the user the CN names, logged
// in by EXTERNAL.
func TestE2ETheCertificateIsTheLogin(t *testing.T) {
	s := secureStreams(t, e2ePrefix())
	name := streamsrabbit.ConnectionName(s)
	var got []string
	for range 20 {
		for _, row := range rabbitmqctl(t, "list_connections", "--no-table-headers",
			"user", "auth_mechanism", "peer_cert_subject", "client_properties") {
			if strings.Contains(row, name) {
				cols := strings.Split(row, "\t")
				for i := range cols {
					cols[i] = strings.TrimSpace(cols[i])
				}
				got = cols[:3]
			}
		}
		if got != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	want := []string{certUser, "EXTERNAL", "CN=" + certUser + ",O=tpp-streams e2e"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the broker's view of connection %s: %q, want %q", name, got, want)
	}
}

// EXTERNAL has nothing to log in with; the TLS handshake already fails, since
// the broker requires a client certificate.
func TestE2EExternalWithoutAClientCertificateIsRefused(t *testing.T) {
	refused(t, amqpsExternal(), mutualTLS(t, true, false), "a TLS connection without a client certificate was accepted")
}

// The client certificate but not the CA: the broker's certificate cannot be
// verified, so the client must refuse it.
func TestE2EAnUntrustedServerIsRefused(t *testing.T) {
	refused(t, amqpsExternal(), mutualTLS(t, false, true), "an unverifiable server was trusted")
}

func TestE2ELifecycleOverMutualTLS(t *testing.T) {
	prefix := e2ePrefix()
	s := secureStreams(t, prefix)
	raw := secureRaw(t)
	ctx := t.Context()
	stream := func(id string, subjects ...string) streams.Stream {
		return streams.Stream{ID: id, Name: "e2e", Subjects: subjects}
	}

	// Create with an explicit id and with a generated one.
	explicit, err := s.Create(ctx, stream("orders", subject))
	if err != nil || explicit.ID != "orders" || !explicit.Active {
		t.Fatalf("Create(orders) = %+v, %v", explicit, err)
	}
	generated, err := s.Create(ctx, stream("", subject))
	if err != nil || generated.ID == "" || generated.ID == explicit.ID {
		t.Fatalf("Create(generated) = %+v, %v", generated, err)
	}
	want := []string{prefix + ".orders", prefix + "." + generated.ID}
	slices.Sort(want)
	if got := onBroker(t, "list_exchanges", prefix); !reflect.DeepEqual(got, want) {
		t.Errorf("exchanges %v, want %v", got, want)
	}

	// Get, list, update.
	if got, _ := s.Get(ctx, "orders"); !reflect.DeepEqual(got, explicit) {
		t.Errorf("Get = %+v, want %+v", got, explicit)
	}
	if list, _ := s.List(ctx); len(list) != 2 {
		t.Errorf("List = %v", list)
	}
	in := stream("ignored", subject, "user.deleted")
	in.Description = "after"
	if updated, xerr := s.Update(ctx, "orders", in); xerr != nil || updated.ID != "orders" {
		t.Fatalf("Update = %+v, %v", updated, xerr)
	}
	if got, _ := s.Get(ctx, "orders"); got.Description != "after" {
		t.Errorf("Update did not take: %+v", got)
	}
	if _, xerr := s.Update(ctx, "missing", stream("", subject)); !errors.Is(xerr, streams.ErrNotFound) {
		t.Errorf("Update(missing) = %v", xerr)
	}

	// Bind, publish, subscribe, under a chosen and a generated id.
	m, err := s.Bind(ctx, "orders")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	subCtx, endSub := context.WithCancel(ctx)
	sub, err := m.Subscribe(subCtx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if id, xerr := m.Publish(ctx, subject, event{User: "ada", Action: "created"}, streams.ID("order-1")); xerr != nil || id != "order-1" {
		t.Fatalf("Publish = %q, %v", id, xerr)
	}
	got := recv(t, sub, liveWait)
	var ev event
	if got.ID != "order-1" || got.Subject != subject || got.Decode(&ev) != nil || ev.User != "ada" {
		t.Errorf("got %+v (%+v)", got, ev)
	}
	// A partition key is ignored: RabbitMQ has no partitions to choose from.
	anyID, err := m.Publish(ctx, subject, event{User: "grace"}, streams.PartitionKey("k"))
	if err != nil {
		t.Fatalf("Publish with a partition key: %v", err)
	}
	if got := recv(t, sub, liveWait); got.ID != anyID {
		t.Errorf("delivered %q, want %q", got.ID, anyID)
	}

	// The typed view over the same manager.
	typed := streams.For[event](m)
	typedCtx, endTyped := context.WithCancel(ctx)
	events, err := typed.Subscribe(typedCtx, subject)
	if err != nil {
		t.Fatalf("typed Subscribe: %v", err)
	}
	evt := event{User: "linus", Action: "typed"}
	if _, xerr := typed.Publish(ctx, subject, evt); xerr != nil {
		t.Fatalf("typed Publish: %v", xerr)
	}
	if got := recv(t, events, liveWait); got != evt {
		t.Errorf("typed got %+v", got)
	}
	endTyped()

	// Batch, pipelined and confirmed, in order.
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil || len(ids) != 3 {
		t.Fatalf("PublishBatch = %v, %v", ids, err)
	}
	for _, id := range ids {
		// sub still sees everything, including the typed publish.
		got := recv(t, sub, liveWait)
		for got.Decode(&ev) == nil && ev.Action == "typed" {
			got = recv(t, sub, liveWait)
		}
		if got.ID != id {
			t.Errorf("batch delivered %q, want %q", got.ID, id)
		}
	}
	endSub()

	// Refusals: undeclared subjects, wildcards as a publish target, options
	// that change the meaning of a call, and the positioned capability.
	if _, xerr := m.Publish(ctx, "typo", event{}); !errors.Is(xerr, streams.ErrUnknownSubject) {
		t.Errorf("Publish(typo) = %v", xerr)
	}
	if _, xerr := m.Subscribe(ctx, "typo"); !errors.Is(xerr, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe(typo) = %v", xerr)
	}
	if _, xerr := b.PublishBatch(ctx, "typo", []any{event{}}); !errors.Is(xerr, streams.ErrUnknownSubject) {
		t.Errorf("PublishBatch(typo) = %v", xerr)
	}
	if _, xerr := b.PublishBatch(ctx, subject, []any{event{}}, streams.ID("x")); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an id = %v", xerr)
	}
	if _, xerr := m.Publish(ctx, subject, event{}, streams.TTL(time.Second)); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", xerr)
	}
	if _, xerr := m.Subscribe(ctx, subject, streams.Group("g")); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("Subscribe with a Group = %v", xerr)
	}
	d, _ := streams.AsDurable(m)
	if _, xerr := d.Consume(ctx, subject, "billing", streams.Group("g")); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v", xerr)
	}
	if _, xerr := d.Consume(ctx, subject, ""); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", xerr)
	}
	if _, xerr := streams.AsPositioned(m); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v", xerr)
	}

	// Ended subscriptions left no queues behind; only now declare durable ones.
	time.Sleep(500 * time.Millisecond)
	if q := onBroker(t, "list_queues", prefix); len(q) != 0 {
		t.Errorf("an ended subscription left its queue: %v", q)
	}

	durableConsumers(t, raw, m, prefix+".orders")

	// Delete both streams; NotFound afterwards, and their exchanges are gone.
	for _, id := range []string{"orders", generated.ID} {
		if xerr := s.Delete(ctx, id); xerr != nil {
			t.Fatalf("Delete(%s): %v", id, xerr)
		}
	}
	for _, id := range []string{"orders", generated.ID} {
		if _, xerr := s.Get(ctx, id); !errors.Is(xerr, streams.ErrNotFound) {
			t.Errorf("Get(%s) after Delete = %v", id, xerr)
		}
		if _, xerr := s.Bind(ctx, id); !errors.Is(xerr, streams.ErrNotFound) {
			t.Errorf("Bind(%s) after Delete = %v", id, xerr)
		}
		if xerr := s.Delete(ctx, id); !errors.Is(xerr, streams.ErrNotFound) {
			t.Errorf("Delete(%s) twice = %v", id, xerr)
		}
	}
	if list, _ := s.List(ctx); len(list) != 0 {
		t.Errorf("List after Delete = %v", list)
	}
	if ex := onBroker(t, "list_exchanges", prefix); len(ex) != 0 {
		t.Errorf("exchanges left: %v", ex)
	}

	// A durable consumer's queue outlives its stream by design: it may hold
	// work nobody has seen. The test owns these, so it removes them.
	if q := onBroker(t, "list_queues", prefix); !reflect.DeepEqual(q, []string{prefix + ".orders.billing.user-created"}) {
		t.Errorf("queues left %v, want only the durable consumer's", q)
	}
	deleteQueues(t, raw, prefix)

	// A publish on the deleted stream fails, and the broker closing its
	// publishing channel for it does not take down the other streams.
	other, err := s.Create(ctx, stream("other", subject))
	if err != nil {
		t.Fatalf("Create(other): %v", err)
	}
	om, _ := s.Bind(ctx, other.ID)
	if _, xerr := m.Publish(ctx, subject, event{}); xerr == nil {
		t.Error("a publish to a deleted stream succeeded")
	}
	otherSub, err := om.Subscribe(subCtxFor(t), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := om.Publish(ctx, subject, event{User: "after"})
	if err != nil {
		t.Fatalf("Publish after the broker closed a channel: %v", err)
	}
	if got := recv(t, otherSub, liveWait); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
	if err := s.Delete(ctx, other.ID); err != nil {
		t.Fatalf("Delete(other): %v", err)
	}
	if ex := onBroker(t, "list_exchanges", prefix); len(ex) != 0 {
		t.Errorf("exchanges left: %v", ex)
	}
}

// subCtxFor is a context ended with the test.
func subCtxFor(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return ctx
}

// durableConsumers checks ack, nak with an attempt count, prefetch,
// persistence, and one name splitting work.
func durableConsumers(t *testing.T, raw *amqp.Connection, m streams.Manager, exchange string) {
	t.Helper()
	ctx := t.Context()
	d, _ := streams.AsDurable(m)

	// Prefetch 1: the second message waits until the first is settled.
	billCtx, endBill := context.WithCancel(ctx)
	billing, err := d.Consume(billCtx, subject, "billing", streams.Prefetch(1))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	first, _ := m.Publish(ctx, subject, event{User: "one"})
	second, _ := m.Publish(ctx, subject, event{User: "two"})
	got := recvDelivery(t, billing, liveWait)
	if got.ID != first || got.Attempt != 1 {
		t.Fatalf("got %q attempt %d, want %q attempt 1", got.ID, got.Attempt, first)
	}
	quietly(t, billing, e2eQuiet, "a second message arrived past prefetch 1")

	// Nak hands it straight back, counted as a second attempt.
	if xerr := got.Nak(ctx); xerr != nil {
		t.Fatalf("Nak: %v", xerr)
	}
	again := recvDelivery(t, billing, liveWait)
	if again.ID != first || again.Attempt < 2 {
		t.Errorf("after nak: %q attempt %d", again.ID, again.Attempt)
	}
	_ = again.Ack(ctx)
	got = recvDelivery(t, billing, liveWait)
	if got.ID != second || got.Attempt != 1 {
		t.Errorf("got %q attempt %d, want %q attempt 1", got.ID, got.Attempt, second)
	}
	_ = got.Ack(ctx)
	quietly(t, billing, e2eQuiet, "more than was published")
	endBill()
	time.Sleep(300 * time.Millisecond)

	// What reaches the queue is persistent, in the shared properties.
	id, err := m.Publish(ctx, subject, event{User: "raw"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ch, err := raw.Channel()
	if err != nil {
		t.Fatal(err)
	}
	msg, ok, err := ch.Get(exchange+".billing.user-created", true)
	if err != nil || !ok {
		t.Fatalf("the durable queue did not hold the message: %v", err)
	}
	if msg.DeliveryMode != amqp.Persistent || msg.MessageId != id || msg.ContentType != "" {
		t.Errorf("properties: mode %d, id %q, type %q", msg.DeliveryMode, msg.MessageId, msg.ContentType)
	}
	_ = ch.Close()

	// Two consumers under one name split the work.
	splitCtx, endSplit := context.WithCancel(ctx)
	defer endSplit()
	a, _ := d.Consume(splitCtx, subject, "billing")
	b, _ := d.Consume(splitCtx, subject, "billing")
	for range 6 {
		if _, err := m.Publish(ctx, subject, event{User: "w"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	seen := map[string]bool{}
	for len(seen) < 6 {
		var got streams.Delivery
		select {
		case got = <-a:
		case got = <-b:
		case <-time.After(liveWait):
			t.Fatalf("only %d of 6 delivered", len(seen))
		}
		if seen[got.ID] {
			t.Errorf("%q delivered twice", got.ID)
		}
		seen[got.ID] = true
		_ = got.Ack(ctx)
	}
	select {
	case got := <-a:
		t.Errorf("more than was published: %q", got.ID)
	case got := <-b:
		t.Errorf("more than was published: %q", got.ID)
	case <-time.After(e2eQuiet):
	}
}

// ledger is the restart test's stream, with default headers a new process
// declares again, since stream metadata lives in the process.
func ledger() streams.Stream {
	return streams.Stream{ID: "ledger", Name: "e2e", Subjects: []string{subject}}.
		WithHeaders(streams.Headers{"lang": "go", "site": "plant-3"})
}

// An acked publish survives a broker restart; an unacked delivery comes back;
// an acked one does not; headers stay with the messages.
func TestE2EDurableAcrossABrokerRestart(t *testing.T) {
	if os.Getenv("STREAMS_E2E_RESTART") != "1" {
		t.Skipf("set STREAMS_E2E_RESTART=1 to restart %s", e2eContainer())
	}
	prefix := e2ePrefix()
	ctx := t.Context()

	// This store must not redial after the restart: its consumer would attach
	// again and take the redelivered messages the new process is checking for.
	before := secureStreams(t, prefix, streamsrabbit.WithReconnectDelay(time.Hour))
	if _, err := before.Create(ctx, ledger()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, _ := before.Bind(ctx, "ledger")
	d, _ := streams.AsDurable(m)
	consumeCtx, endConsume := context.WithCancel(ctx)
	defer endConsume()
	consumer, err := d.Consume(consumeCtx, subject, "billing", streams.Prefetch(2))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	// Each publish returns only once the broker has confirmed it.
	acked, _ := m.Publish(ctx, subject, event{User: "acked"})
	// Headers travel with the message, so a restart keeps them in the queue.
	unacked, _ := m.Publish(ctx, subject, event{User: "unacked"}, streams.Header("traceparent", "00-unacked"))
	waiting, err := m.Publish(ctx, subject, event{User: "waiting"}, streams.Header("site", "plant-4"), streams.Expires(10*time.Minute))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := recvDelivery(t, consumer, liveWait)
	if got.ID != acked {
		t.Fatalf("delivered %q, want %q", got.ID, acked)
	}
	_ = got.Ack(ctx)
	if held := recvDelivery(t, consumer, liveWait); held.ID != unacked {
		t.Fatalf("delivered %q, want %q", held.ID, unacked)
	}
	// Let the ack reach the broker before pulling the plug.
	time.Sleep(500 * time.Millisecond)

	restart(t)
	endConsume()
	_ = before.(streams.Closer).Close()

	after := secureStreams(t, prefix)
	// The exchange is durable; declaring the stream again, with its default
	// headers, is how a new process shares it.
	if _, xerr := after.Create(ctx, ledger()); xerr != nil {
		t.Fatalf("Create after the restart: %v", xerr)
	}
	m, _ = after.Bind(ctx, "ledger")
	d, _ = streams.AsDurable(m)
	afterCtx, endAfter := context.WithCancel(ctx)
	defer endAfter()
	consumer, err = d.Consume(afterCtx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume after the restart: %v", err)
	}
	back := map[string]int{}
	for range 2 {
		d := recvDelivery(t, consumer, liveWait)
		want := streams.Headers{"lang": "go", "site": "plant-4"}
		if d.ID == unacked {
			want = streams.Headers{"lang": "go", "site": "plant-3", "traceparent": "00-unacked"}
		}
		if !reflect.DeepEqual(d.Headers, want) {
			t.Errorf("%s: headers %v, want %v", d.ID, d.Headers, want)
		}
		back[d.ID] = d.Attempt
		_ = d.Ack(ctx)
	}
	if _, ok := back[unacked]; !ok || len(back) != 2 {
		t.Fatalf("after the restart got %v, want %q and %q", back, unacked, waiting)
	}
	if _, ok := back[waiting]; !ok {
		t.Fatalf("after the restart got %v, want %q and %q", back, unacked, waiting)
	}
	if back[unacked] < 2 {
		t.Errorf("the unacked message came back as attempt %d", back[unacked])
	}
	quietly(t, consumer, e2eQuiet, "the acked message came back")

	// The exchange and binding survived too: a fresh publish routes.
	fresh, err := m.Publish(ctx, subject, event{User: "fresh"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got = recvDelivery(t, consumer, liveWait)
	if got.ID != fresh || got.Headers.Get("lang") != "go" {
		t.Errorf("got %q %v", got.ID, got.Headers)
	}
	_ = got.Ack(ctx)
	// End the consumer first, or it attaches again to the queue deleted
	// under it.
	endAfter()
	time.Sleep(300 * time.Millisecond)

	if err := after.Delete(ctx, "ledger"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	deleteQueues(t, secureRaw(t), prefix)
	if q := onBroker(t, "list_queues", prefix); len(q) != 0 {
		t.Errorf("queues left: %v", q)
	}
	if ex := onBroker(t, "list_exchanges", prefix); len(ex) != 0 {
		t.Errorf("exchanges left: %v", ex)
	}
}

// Headers, expiry, retain and delayed nak over mutual TLS: defaults and
// overrides arrive on single publishes and batches, another client sees them
// as an AMQP table of strings with the expiration property, they survive a
// nak, bad keys are refused at Create and publish, an expired message is not
// delivered, and retain and a delayed nak are refused by name.
func TestE2EHeadersAndExpiryOverMutualTLS(t *testing.T) {
	prefix := e2ePrefix()
	s := secureStreams(t, prefix)
	raw := secureRaw(t)
	ctx := t.Context()
	stream := streams.Stream{ID: "h", Name: "e2e", Subjects: []string{subject}}

	for _, key := range []string{"x-death", "CC", "has space"} {
		bad := stream
		bad.ID = "bad"
		if _, err := s.Create(ctx, bad.WithHeader(key, "v")); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Create with %q = %v, want ErrInvalidHeader", key, err)
		}
	}
	if _, err := s.Create(ctx, stream.WithHeader("lang", "go")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, _ := s.Bind(ctx, "h")
	d, _ := streams.AsDurable(m)
	queue := prefix + ".h.audit.user-created"

	// Declare the queue, then publish while nobody consumes.
	park(t, m, "audit")
	value := event{User: "ada"}
	id, err := m.Publish(ctx, subject, value, streams.Header("traceparent", "00-ab"), streams.Expires(time.Minute))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Another client reads the queue directly and sees AMQP properties.
	ch, err := raw.Channel()
	if err != nil {
		t.Fatal(err)
	}
	msg, ok, err := ch.Get(queue, false)
	if err != nil || !ok {
		t.Fatalf("the durable queue did not hold the message: %v", err)
	}
	if msg.Headers["lang"] != "go" || msg.Headers["traceparent"] != "00-ab" || msg.Expiration != "60000" {
		t.Errorf("headers %v, expiration %q", msg.Headers, msg.Expiration)
	}
	if want, _ := core.Pack(streams.JSON, id, value); !bytes.Equal(msg.Body, want) {
		t.Errorf("body %q, want the version-1 frame %q", msg.Body, want)
	}
	_ = ch.Close()

	// The unsettled get went back; the consumer gets it with its headers, and
	// again after a nak.
	auditCtx, endAudit := context.WithCancel(ctx)
	defer endAudit()
	audit, err := d.Consume(auditCtx, subject, "audit")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got := recvDelivery(t, audit, liveWait)
	if got.ID != id || got.Headers.Get("traceparent") != "00-ab" || got.CanDelayRedelivery() {
		t.Errorf("got %q %v, can delay %v", got.ID, got.Headers, got.CanDelayRedelivery())
	}
	_ = got.Nak(ctx)
	got = recvDelivery(t, audit, liveWait)
	if got.Headers.Get("lang") != "go" || got.Headers.Get("traceparent") != "00-ab" {
		t.Errorf("after nak: headers %v", got.Headers)
	}
	// A refused delayed nak requeues at once rather than stranding it.
	if xerr := got.NakAfter(ctx, 30*time.Second); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("NakAfter = %v, want ErrUnsupported", xerr)
	}
	if got = recvDelivery(t, audit, liveWait); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
	_ = got.Ack(ctx)

	// Batches carry them too.
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, subject, []any{event{User: "a"}, event{User: "b"}}, streams.Header("lang", "rust"))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		got := recvDelivery(t, audit, liveWait)
		if got.ID != id || got.Headers.Get("lang") != "rust" {
			t.Errorf("got %q %v, want %q with lang rust", got.ID, got.Headers, id)
		}
		_ = got.Ack(ctx)
	}

	// An expired message is dropped by the broker.
	lateCtx, endLate := context.WithCancel(ctx)
	defer endLate()
	park(t, m, "late")
	if _, xerr := m.Publish(ctx, subject, event{User: "stale"}, streams.Expires(200*time.Millisecond)); xerr != nil {
		t.Fatalf("Publish: %v", xerr)
	}
	time.Sleep(800 * time.Millisecond)
	late, err := d.Consume(lateCtx, subject, "late")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	quietly(t, late, e2eQuiet, "an expired message was delivered")

	// Refusals at publish.
	for _, h := range [][2]string{{"x-delay", "1000"}, {"BCC", "user.deleted"}, {"lang", "two\nlines"}} {
		if _, err := m.Publish(ctx, subject, event{}, streams.Header(h[0], h[1])); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Publish with %q = %v, want ErrInvalidHeader", h[0], err)
		}
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with Retain = %v, want ErrUnsupported", err)
	}

	endAudit()
	endLate()
	time.Sleep(300 * time.Millisecond)
	if err := s.Delete(ctx, "h"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	deleteQueues(t, raw, prefix)
	if q := onBroker(t, "list_queues", prefix); len(q) != 0 {
		t.Errorf("queues left: %v", q)
	}
}

// restart restarts the broker container and waits until it reports healthy.
func restart(t *testing.T) {
	t.Helper()
	name := e2eContainer()
	if out, err := exec.CommandContext(t.Context(), "docker", "restart", name).CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s: %v: %s", name, err, out)
	}
	for range 120 {
		out, _ := exec.CommandContext(t.Context(), "docker", "inspect", "-f", "{{.State.Health.Status}}", name).Output()
		if strings.TrimSpace(string(out)) == "healthy" {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s did not become healthy after a restart", name)
}
