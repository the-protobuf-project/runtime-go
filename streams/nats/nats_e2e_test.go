//go:build live

package nats_test

// Both NATS providers end to end over mutual TLS with authentication, against
// a server that keeps JetStream on disk with every write fsynced.
//
// Start it, from the streams module, with
//
//	docker/tls/generate.sh
//	docker compose -p tpp-streams-e2e-nats -f docker/compose.nats.secure.yaml up -d --wait
//
// NATS_E2E_URL overrides the default tls://127.0.0.1:14223 (mTLS plus a
// password or NKey), NATS_E2E_CERTMAP_URL the default tls://127.0.0.1:14224
// (mTLS where the certificate is the user), and NATS_E2E_CERTS the certificate
// directory (default docker/tls/out). Without the server the tests skip.
//
// Every user is limited to e2e.> plus what JetStream needs, so every subject
// here starts with e2e.
//
// The broker-restart test runs only with STREAMS_E2E_RESTART=1, because it
// restarts the tpp-streams-nats-secure container. Go runs these tests one at a
// time, so no other test is mid-flight when it does.

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsnats "github.com/the-protobuf-project/runtime-go/streams/nats"
)

const (
	e2eContainer = "tpp-streams-nats-secure"

	// The test-only password user in docker/compose.nats.secure.conf.
	e2ePasswordUser = "e2e-password"
	e2ePassword     = "e2e-test-only-password"

	// A well-formed NKey seed that is no user of the server, generated for
	// this test and protecting nothing.
	e2eStrangerSeed = "SUAKWXM5G2VPFEV5HKIRI7FLLUX2EEDVKQP53XTCHEOLNBJDGVUQVZPIVM"

	// Shortened JetStream settings, so redelivery and cleanup happen within a
	// test rather than after the 30-second defaults.
	e2eAckWait  = time.Second
	e2eInactive = 2 * time.Second
)

func e2eEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func e2eURL() string        { return e2eEnv("NATS_E2E_URL", "tls://127.0.0.1:14223") }
func e2eCertmapURL() string { return e2eEnv("NATS_E2E_CERTMAP_URL", "tls://127.0.0.1:14224") }
func e2eCerts() string      { return e2eEnv("NATS_E2E_CERTS", filepath.Join("..", "docker", "tls", "out")) }

// e2eSeedFile is the test-only NKey seed file the server knows.
func e2eSeedFile() string { return filepath.Join("..", "docker", "compose.nats.secure.nkey") }

// e2eReachable skips the test when nothing listens on the secure port.
func e2eReachable(t *testing.T) {
	t.Helper()

	host := strings.TrimPrefix(e2eURL(), "tls://")
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", host)
	if err != nil {
		t.Skipf("no secure NATS at %s (%v); see docker/compose.nats.secure.yaml", host, err)
	}
	_ = conn.Close()
	if _, err := os.Stat(filepath.Join(e2eCerts(), "ca.crt")); err != nil {
		t.Skipf("no test certificates in %s (%v); run docker/tls/generate.sh", e2eCerts(), err)
	}
}

// trusting is TLS that trusts the test CA, without a client certificate.
func trusting() []gonats.Option {
	return []gonats.Option{
		gonats.RootCAs(filepath.Join(e2eCerts(), "ca.crt")),
		gonats.Timeout(within),
		gonats.MaxReconnects(-1),
	}
}

// mtls is mutual TLS: the test CA plus the client certificate the server
// requires, with no credentials yet.
func mtls(extra ...gonats.Option) []gonats.Option {
	dir := e2eCerts()
	return append(append(trusting(),
		gonats.ClientCert(filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))), extra...)
}

// withPassword adds the test-only password user's credentials.
func withPassword(opts []gonats.Option) []gonats.Option {
	return append(opts, gonats.UserInfo(e2ePasswordUser, e2ePassword))
}

// e2eConnect connects with opts to url, failing when it cannot.
func e2eConnect(t *testing.T, url string, opts []gonats.Option) *gonats.Conn {
	t.Helper()

	nc, err := gonats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("no NATS over mTLS at %s (%v); see docker/compose.nats.secure.yaml", url, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// passwordConn is mutual TLS as the password user.
func passwordConn(t *testing.T) *gonats.Conn {
	t.Helper()
	e2eReachable(t)
	return e2eConnect(t, e2eURL(), withPassword(mtls()))
}

// nkeyConn is mutual TLS as the NKey user.
func nkeyConn(t *testing.T) *gonats.Conn {
	t.Helper()
	e2eReachable(t)
	nkey, err := gonats.NkeyOptionFromSeed(e2eSeedFile())
	if err != nil {
		t.Fatalf("NkeyOptionFromSeed: %v", err)
	}
	return e2eConnect(t, e2eURL(), mtls(nkey))
}

// certmapConn is mutual TLS where the certificate is the user.
func certmapConn(t *testing.T) *gonats.Conn {
	t.Helper()
	e2eReachable(t)
	return e2eConnect(t, e2eCertmapURL(), mtls())
}

// unauthorized expects the server to refuse opts as unauthorized.
func unauthorized(t *testing.T, opts []gonats.Option) {
	t.Helper()

	nc, err := gonats.Connect(e2eURL(), append(opts, gonats.NoReconnect())...)
	if err == nil {
		nc.Close()
		t.Fatal("the server let the client in")
	}
	if !errors.Is(err, gonats.ErrAuthorization) && !strings.Contains(strings.ToLower(err.Error()), "authorization violation") {
		t.Errorf("connect = %v, want an authorization violation", err)
	}
}

// e2eUnique is a fresh subject or id under e2e., the only subjects the users
// may use.
func e2eUnique(prefix string) string { return "e2e." + prefix + "." + core.NewID() }

// rawStream is the server's handle on a stream, for checking its own view.
func rawStream(t *testing.T, nc *gonats.Conn, id string) jetstream.Stream {
	t.Helper()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	s, err := js.Stream(t.Context(), id)
	if err != nil {
		t.Fatalf("Stream(%s): %v", id, err)
	}
	return s
}

// consumers lists every consumer the server holds on s.
func consumers(t *testing.T, s jetstream.Stream) []*jetstream.ConsumerInfo {
	t.Helper()

	var out []*jetstream.ConsumerInfo
	lister := s.ListConsumers(t.Context())
	for info := range lister.Info() {
		out = append(out, info)
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("ListConsumers: %v", err)
	}
	return out
}

// onlyConsumer is the one consumer on s, failing if there are none or several.
func onlyConsumer(t *testing.T, s jetstream.Stream) *jetstream.ConsumerInfo {
	t.Helper()

	all := consumers(t, s)
	if len(all) != 1 {
		t.Fatalf("%d consumers, want one", len(all))
	}
	return all[0]
}

// ---------------------------------------------------------------- transport

// The client port speaks TLS only: the server announces it, and a plaintext
// CONNECT is answered with an error and a closed socket, never a PONG.
func TestE2EPlaintextIsRefused(t *testing.T) {
	e2eReachable(t)

	dialer := net.Dialer{Timeout: within}
	conn, err := dialer.DialContext(t.Context(), "tcp", strings.TrimPrefix(e2eURL(), "tls://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(within))
	info, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"INFO ", `"tls_required":true`, `"tls_verify":true`, `"auth_required":true`} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO lacks %s: %s", want, info)
		}
	}
	_, _ = conn.Write([]byte("CONNECT {\"verbose\":false,\"pedantic\":false}\r\nPING\r\n"))
	reply := make([]byte, 4096)
	n, _ := conn.Read(reply)
	if strings.Contains(string(reply[:n]), "PONG") {
		t.Errorf("a plaintext client was served: %s", reply[:n])
	}
}

// A client that does not trust the test CA cannot complete the handshake.
func TestE2EAnUnknownServerCertificateIsRefused(t *testing.T) {
	e2eReachable(t)

	nc, err := gonats.Connect(e2eURL(), gonats.Secure(), gonats.Timeout(within), gonats.NoReconnect())
	if err == nil {
		nc.Close()
		t.Fatal("connected without trusting the server CA")
	}
}

// Mutual TLS: trusting the server is not enough; the server wants a
// certificate from the client too.
func TestE2ETLSWithoutAClientCertificateIsRefused(t *testing.T) {
	e2eReachable(t)

	nc, err := gonats.Connect(e2eURL(), append(withPassword(trusting()), gonats.NoReconnect())...)
	if err == nil {
		defer nc.Close()
		if ferr := nc.FlushTimeout(within); ferr == nil {
			t.Fatal("a client without a certificate was served")
		}
	}
}

func TestE2EMutualTLSConnects(t *testing.T) {
	nc := passwordConn(t)
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if !nc.TLSRequired() || !nc.AuthRequired() {
		t.Errorf("TLSRequired %v, AuthRequired %v", nc.TLSRequired(), nc.AuthRequired())
	}
	if _, err := streamsnats.UseJetStream(nc); err != nil {
		t.Errorf("JetStream over mTLS: %v", err)
	}
}

// ---------------------------------------------------------- authentication

// The client certificate admits the connection; it is not a user on this
// port, so it still needs credentials.
func TestE2EACertificateWithoutCredentialsIsRefused(t *testing.T) {
	e2eReachable(t)
	unauthorized(t, mtls())
}

func TestE2EAWrongPasswordIsRefused(t *testing.T) {
	e2eReachable(t)
	unauthorized(t, mtls(gonats.UserInfo(e2ePasswordUser, "not-the-password")))
	unauthorized(t, mtls(gonats.UserInfo("nobody", e2ePassword)))
}

// A well-formed NKey that is not a user: it signs the nonce correctly, and the
// server still has no such key.
func TestE2EAnUnknownNKeyIsRefused(t *testing.T) {
	e2eReachable(t)

	seed := filepath.Join(t.TempDir(), "stranger.nk")
	if err := os.WriteFile(seed, []byte(e2eStrangerSeed+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stranger, err := gonats.NkeyOptionFromSeed(seed)
	if err != nil {
		t.Fatalf("NkeyOptionFromSeed: %v", err)
	}
	unauthorized(t, mtls(stranger))
}

func TestE2EAnNKeyUserConnects(t *testing.T) {
	nc := nkeyConn(t)
	if err := nc.Flush(); err != nil || !nc.AuthRequired() {
		t.Errorf("Flush %v, AuthRequired %v", err, nc.AuthRequired())
	}
}

// With verify_and_map the certificate's subject is the user.
func TestE2EACertificateMappedUserConnects(t *testing.T) {
	nc := certmapConn(t)
	if err := nc.Flush(); err != nil || !nc.TLSRequired() || !nc.AuthRequired() {
		t.Errorf("Flush %v, TLSRequired %v, AuthRequired %v", err, nc.TLSRequired(), nc.AuthRequired())
	}
	if _, err := streamsnats.UseJetStream(nc); err != nil {
		t.Errorf("JetStream as the mapped user: %v", err)
	}
}

// Every user may publish and subscribe under e2e. only. Outside it the server
// refuses: a core publish or subscribe is answered with a permissions
// violation, and a JetStream publish is never stored, so its ack never comes
// and the publish fails.
func TestE2EPermissionsRestrictWhatAUserMayPublish(t *testing.T) {
	e2eReachable(t)
	violations := make(chan string, 16)
	var mu sync.Mutex
	nc := e2eConnect(t, e2eURL(), withPassword(mtls(gonats.ErrorHandler(func(_ *gonats.Conn, _ *gonats.Subscription, err error) {
		mu.Lock()
		defer mu.Unlock()
		select {
		case violations <- strings.ToLower(err.Error()):
		default:
		}
	}))))
	violation := func() string {
		t.Helper()
		return next(t, violations, within)
	}
	ctx := t.Context()

	// Core NATS: the client sends, the server refuses.
	forbidden := "forbidden." + core.NewID()
	plain := streamsnats.Use(nc)
	_, m := liveDeclare(t, plain, streams.Stream{Subjects: []string{forbidden}})
	if _, err := m.Publish(ctx, forbidden, event{User: "x"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = nc.Flush()
	if said := violation(); !strings.Contains(said, "permissions violation for publish") || !strings.Contains(said, strings.ToLower(forbidden)) {
		t.Errorf("server said %q", said)
	}
	if _, err := m.Subscribe(ctx, forbidden); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = nc.Flush()
	if said := violation(); !strings.Contains(said, "permissions violation for subscription") {
		t.Errorf("server said %q", said)
	}

	// The same user is served under e2e., on the same connection.
	allowed := e2eUnique("allowed")
	_, m = liveDeclare(t, plain, streams.Stream{Subjects: []string{allowed}})
	ch, _ := m.Subscribe(ctx, allowed)
	id, _ := m.Publish(ctx, allowed, event{User: "ok"})
	if got := next(t, ch, within); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}

	// JetStream: declaring the stream is a call to the JetStream API, which the
	// user may make; publishing into it is not.
	forbidden = "forbidden." + core.NewID()
	js := liveJetStream(t, nc)
	stream, jm := liveDeclare(t, js, streams.Stream{Subjects: []string{forbidden}})
	if _, err := jm.Publish(ctx, forbidden, event{User: "x"}); err == nil {
		t.Error("a forbidden publish was acked")
	}
	if said := violation(); !strings.Contains(said, "permissions violation for publish") {
		t.Errorf("server said %q", said)
	}
	info, _ := rawStream(t, nc, stream.ID).Info(ctx)
	if info.State.Msgs != 0 {
		t.Errorf("stored %d, want none", info.State.Msgs)
	}
}

// ---------------------------------------------------------------- core NATS

func TestE2ECoreFullLifecycleOverTLS(t *testing.T) {
	nc := passwordConn(t)
	p := streamsnats.Use(nc)
	ctx := t.Context()
	root := e2eUnique("orders")
	placed, pattern := root+".placed", root+".*"

	// Create with an explicit id and with a generated one.
	explicitID := e2eUnique("core-explicit")
	explicit, err := p.Create(ctx, streams.Stream{ID: explicitID, Name: "explicit", Subjects: []string{placed}})
	if err != nil || explicit.ID != explicitID || !explicit.Active {
		t.Fatalf("Create = %+v, %v", explicit, err)
	}
	generated, err := p.Create(ctx, streams.Stream{Name: "orders", Description: "before", Subjects: []string{pattern}, UserID: "ada"})
	if err != nil || !strings.HasSuffix(generated.ID, ":orders") {
		t.Fatalf("Create = %+v, %v", generated, err)
	}

	// Get, list, update.
	if got, _ := p.Get(ctx, generated.ID); got.UserID != "ada" || got.Description != "before" {
		t.Errorf("Get = %+v", got)
	}
	list, _ := p.List(ctx)
	ids := make([]string, 0, len(list))
	for _, s := range list {
		ids = append(ids, s.ID)
	}
	if !slices.Contains(ids, explicit.ID) || !slices.Contains(ids, generated.ID) {
		t.Errorf("List = %v", ids)
	}
	after := generated
	after.Description = "after"
	if updated, perr := p.Update(ctx, generated.ID, after); perr != nil || updated.ID != generated.ID {
		t.Errorf("Update = %+v, %v", updated, perr)
	}
	if got, _ := p.Get(ctx, generated.ID); got.Description != "after" {
		t.Errorf("Get after Update = %q", got.Description)
	}

	// Bind, publish, subscribe; the id option is honored.
	m, _ := p.Bind(ctx, generated.ID)
	subCtx, cancelSubs := context.WithCancel(ctx)
	sub, _ := m.Subscribe(subCtx, pattern)
	id, err := m.Publish(ctx, placed, event{User: "ada"}, streams.ID("core-1"))
	if err != nil || id != "core-1" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	got := next(t, sub, within)
	var e event
	if got.ID != "core-1" || got.Subject != placed || got.Decode(&e) != nil || e.User != "ada" {
		t.Errorf("delivered %q on %q as %+v", got.ID, got.Subject, e)
	}

	// Prefetch and partition key only tune delivery here, so they are accepted.
	tunedCtx, cancelTuned := context.WithCancel(ctx)
	tuned, _ := m.Subscribe(tunedCtx, placed, streams.Prefetch(4))
	id, _ = m.Publish(ctx, placed, event{User: "bo"}, streams.PartitionKey("ada"))
	if next(t, tuned, within).ID != id || next(t, sub, within).ID != id {
		t.Error("a tuned publish was not delivered to both")
	}
	cancelTuned()

	// Typed view.
	orders := streams.For[event](m)
	typed, _ := orders.Subscribe(subCtx, placed)
	want := event{User: "cy", Action: "placed"}
	if _, perr := orders.Publish(ctx, placed, want); perr != nil {
		t.Fatal(perr)
	}
	if got := next(t, typed, within); got != want {
		t.Errorf("typed = %+v", got)
	}
	next(t, sub, within)

	// Undeclared and wildcard subjects, and the TTL it cannot honor.
	other := e2eUnique("elsewhere")
	if _, perr := m.Publish(ctx, other, event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish(other) = %v", perr)
	}
	if _, perr := m.Subscribe(ctx, other); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe(other) = %v", perr)
	}
	if _, perr := m.Publish(ctx, pattern, event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish(pattern) = %v", perr)
	}
	if _, perr := m.Publish(ctx, placed, event{}, streams.TTL(time.Second)); !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", perr)
	}

	// Capabilities: batch only.
	if _, perr := streams.AsDurable(m); !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("AsDurable = %v", perr)
	}
	if _, perr := streams.AsPositioned(m); !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v", perr)
	}
	b, _ := streams.AsBatch(m)
	batch, err := b.PublishBatch(ctx, placed, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil || len(batch) != 3 {
		t.Fatalf("PublishBatch = %v, %v", batch, err)
	}
	for _, id := range batch {
		if got := next(t, sub, within); got.ID != id {
			t.Errorf("batch entry %q arrived as %q", id, got.ID)
		}
	}
	if _, err := b.PublishBatch(ctx, placed, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an id = %v", err)
	}

	// Canceling the subscriptions releases the server-side interest.
	cancelSubs()
	deadline := time.Now().Add(within)
	for {
		_ = nc.Flush()
		if _, err := nc.Request(placed, []byte("ping"), time.Second); errors.Is(err, gonats.ErrNoResponders) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a canceled subscription still had interest")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Delete, then NotFound everywhere.
	for _, s := range []streams.Stream{explicit, generated} {
		if err := p.Delete(ctx, s.ID); err != nil {
			t.Fatal(err)
		}
		_, gerr := p.Get(ctx, s.ID)
		_, berr := p.Bind(ctx, s.ID)
		_, uerr := p.Update(ctx, s.ID, streams.Stream{})
		for _, err := range []error{gerr, berr, uerr, p.Delete(ctx, s.ID)} {
			if !errors.Is(err, streams.ErrNotFound) {
				t.Errorf("after Delete: %v", err)
			}
		}
	}
}

func TestE2ECoreQueueGroupsShareAndFanOut(t *testing.T) {
	nc := passwordConn(t)
	subj := e2eUnique("jobs")

	// The configured queue group: two members share twenty messages.
	p := streamsnats.Use(nc, streamsnats.WithQueueGroup("workers"))
	_, m := liveDeclare(t, p, streams.Stream{Subjects: []string{subj}})
	first, _ := m.Subscribe(t.Context(), subj)
	second, _ := m.Subscribe(t.Context(), subj)
	// A different group, named per call, receives its own copy of each.
	audit, _ := m.Subscribe(t.Context(), subj, streams.Group("audit"))
	for range 20 {
		if _, err := m.Publish(t.Context(), subj, event{User: "u"}); err != nil {
			t.Fatal(err)
		}
	}
	shared := merge(t.Context(), first, second)
	for range 20 {
		next(t, shared, within)
		next(t, audit, within)
	}
	nothing(t, shared, 300*time.Millisecond)
	nothing(t, audit, 100*time.Millisecond)
}

// ---------------------------------------------------------------- JetStream

func TestE2EJetStreamFullLifecycleAsAPasswordUser(t *testing.T) {
	jetstreamLifecycle(t, passwordConn(t))
}

func TestE2EJetStreamFullLifecycleAsAnNKeyUser(t *testing.T) {
	jetstreamLifecycle(t, nkeyConn(t))
}

func TestE2EJetStreamFullLifecycleAsACertificateMappedUser(t *testing.T) {
	jetstreamLifecycle(t, certmapConn(t))
}

// jetstreamLifecycle runs the full JetStream stream and consumer lifecycle,
// checked against the server's own view, once per way of authenticating.
func jetstreamLifecycle(t *testing.T, nc *gonats.Conn) {
	t.Helper()
	ctx := t.Context()
	p := liveJetStream(t, nc)
	subj := e2eUnique("user.created")
	other := e2eUnique("user.deleted")

	// Create with an explicit id and with a generated one.
	explicitID := e2eUnique("js-explicit")
	explicit, err := p.Create(ctx, streams.Stream{ID: explicitID, Name: "explicit", Subjects: []string{other}})
	if err != nil || explicit.ID != strings.ReplaceAll(explicitID, ".", "-") {
		t.Fatalf("Create = %+v, %v", explicit, err)
	}
	created, err := p.Create(ctx, streams.Stream{Name: "users.v1", Description: "before", Subjects: []string{subj}, UserID: "ada"})
	if err != nil || !strings.HasSuffix(created.ID, ":users-v1") {
		t.Fatalf("Create = %+v, %v", created, err)
	}

	// Stored on disk, not in memory.
	raw := rawStream(t, nc, created.ID)
	info, _ := raw.Info(ctx)
	if info.Config.Storage != jetstream.FileStorage {
		t.Errorf("storage = %v, want file", info.Config.Storage)
	}

	// Get, list, update.
	if got, _ := p.Get(ctx, created.ID); got.Name != "users.v1" || got.UserID != "ada" || !got.Active {
		t.Errorf("Get = %+v", got)
	}
	after := created
	after.Description = "after"
	updated, err := p.Update(ctx, created.ID, after)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := p.Get(ctx, created.ID); got.Description != "after" {
		t.Errorf("Get after Update = %+v", got)
	}
	list, _ := p.List(ctx)
	if !slices.ContainsFunc(list, func(s streams.Stream) bool { return s.ID == updated.ID }) ||
		!slices.ContainsFunc(list, func(s streams.Stream) bool { return s.ID == explicit.ID }) {
		t.Error("List is missing a stream")
	}
	info, _ = raw.Info(ctx)
	if info.Config.Storage != jetstream.FileStorage || info.Config.Description != "after" {
		t.Errorf("after Update: storage %v, description %q", info.Config.Storage, info.Config.Description)
	}

	// Bind; publish waits for the stored ack, so the message is already there.
	m, _ := p.Bind(ctx, created.ID)
	subCtx, cancelSubs := context.WithCancel(ctx)
	sub, _ := m.Subscribe(subCtx, subj)
	id, err := m.Publish(ctx, subj, event{User: "ada"}, streams.ID("js-1"))
	if err != nil || id != "js-1" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	if now, _ := raw.Info(ctx); now.State.Msgs != 1 {
		t.Errorf("stored %d, want 1", now.State.Msgs)
	}
	if got := next(t, sub, within); got.ID != "js-1" {
		t.Errorf("delivered %q", got.ID)
	}

	// A retried id is collapsed by the server.
	_, _ = m.Publish(ctx, subj, event{User: "ada"}, streams.ID("js-1"))
	if now, _ := raw.Info(ctx); now.State.Msgs != 1 {
		t.Errorf("stored %d after a retry, want 1", now.State.Msgs)
	}

	// Group on subscribe is ignored; partition key is ignored.
	tuned, _ := m.Subscribe(subCtx, subj, streams.Group("ignored"), streams.Prefetch(2))
	id, _ = m.Publish(ctx, subj, event{User: "bo"}, streams.PartitionKey("ada"))
	if next(t, tuned, within).ID != id || next(t, sub, within).ID != id {
		t.Error("a tuned publish was not delivered to both")
	}

	// Typed view.
	users := streams.For[event](m)
	typed, _ := users.Subscribe(subCtx, subj)
	want := event{User: "cy", Action: "created"}
	if _, perr := users.Publish(ctx, subj, want); perr != nil {
		t.Fatal(perr)
	}
	if got := next(t, typed, within); got != want {
		t.Errorf("typed = %+v", got)
	}
	next(t, sub, within)
	cancelSubs()

	// Refusals.
	if _, perr := m.Publish(ctx, other, event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish(other) = %v", perr)
	}
	if _, perr := m.Subscribe(ctx, other); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe(other) = %v", perr)
	}
	if _, perr := m.Publish(ctx, subj, event{}, streams.TTL(time.Second)); !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", perr)
	}
	durable, _ := streams.AsDurable(m)
	if _, perr := durable.Consume(ctx, subj, ""); !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", perr)
	}
	if _, perr := durable.Consume(ctx, subj, "w", streams.Group("g")); !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("Consume with a group = %v", perr)
	}
	if _, perr := durable.Consume(ctx, other, "w"); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Consume(other) = %v", perr)
	}

	// Durable consume: nak redelivers with a counted attempt, ack settles.
	consumeCtx, stopConsume := context.WithCancel(ctx)
	deliveries, _ := durable.Consume(consumeCtx, subj, "workers", streams.Prefetch(1))
	id, _ = m.Publish(ctx, subj, event{User: "dee"})
	first := next(t, deliveries, within)
	if first.ID != id || first.Attempt != 1 {
		t.Errorf("first %q attempt %d", first.ID, first.Attempt)
	}
	_ = first.Nak(ctx)
	second := next(t, deliveries, within)
	if second.ID != id || second.Attempt != 2 {
		t.Errorf("second %q attempt %d", second.ID, second.Attempt)
	}
	_ = second.Ack(ctx)
	nothing(t, deliveries, 500*time.Millisecond)
	stopConsume()
	c, err := raw.Consumer(ctx, "workers")
	if err != nil {
		t.Fatal(err)
	}
	ci, _ := c.Info(ctx)
	if ci.Config.Durable != "workers" || ci.NumAckPending != 0 || ci.NumPending != 0 {
		t.Errorf("consumer %q: ack pending %d, pending %d", ci.Config.Durable, ci.NumAckPending, ci.NumPending)
	}

	// Batch, then replay from the earliest stored message with prefetch 1.
	b, _ := streams.AsBatch(m)
	batch, err := b.PublishBatch(ctx, subj, []any{event{User: "e"}, event{User: "f"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishBatch(ctx, subj, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an id = %v", err)
	}
	info, _ = raw.Info(ctx)
	if info.State.Msgs != 6 {
		t.Errorf("stored %d, want every acked publish (6)", info.State.Msgs)
	}
	positioned, _ := streams.AsPositioned(m)
	replay, _ := positioned.ConsumeFrom(ctx, subj, "replayer", streams.FromEarliest, streams.Prefetch(1))
	replayed := make([]string, 0, info.State.Msgs)
	for range info.State.Msgs {
		d := next(t, replay, within)
		replayed = append(replayed, d.ID)
		_ = d.Ack(ctx)
	}
	if replayed[0] != "js-1" || !slices.Equal(replayed[len(replayed)-2:], batch) {
		t.Errorf("replayed %v", replayed)
	}
	nothing(t, replay, 300*time.Millisecond)

	// FromNew skips what is already stored.
	fresh, _ := positioned.ConsumeFrom(ctx, subj, "fresh", streams.FromNew)
	id, _ = m.Publish(ctx, subj, event{User: "g"})
	if d := next(t, fresh, within); d.ID != id {
		t.Errorf("fresh delivered %q, want %q", d.ID, id)
	} else {
		_ = d.Ack(ctx)
	}

	// Delete: the stream and every consumer on it are gone from the server.
	js, _ := jetstream.New(nc)
	for _, s := range []streams.Stream{explicit, updated} {
		if err := p.Delete(ctx, s.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := js.Stream(ctx, s.ID); err == nil {
			t.Errorf("%s survived", s.ID)
		}
		_, gerr := p.Get(ctx, s.ID)
		_, berr := p.Bind(ctx, s.ID)
		_, uerr := p.Update(ctx, s.ID, s)
		for _, err := range []error{gerr, berr, uerr, p.Delete(ctx, s.ID)} {
			if !errors.Is(err, streams.ErrNotFound) {
				t.Errorf("after Delete: %v", err)
			}
		}
	}
	names := js.StreamNames(ctx)
	for name := range names.Name() {
		if name == explicit.ID || name == updated.ID {
			t.Errorf("%s is still listed", name)
		}
	}
}

// ------------------------------------------------------- JetStream settings

// Unset, every setting is the server's own default, or 30 seconds for the
// ordered consumer's inactive threshold, as runtime-rs has it.
func TestE2EDefaultSettingsAreTheServers(t *testing.T) {
	nc := passwordConn(t)
	subj := e2eUnique("defaults")
	stream, m := liveDeclare(t, liveJetStream(t, nc), streams.Stream{Subjects: []string{subj}})
	raw := rawStream(t, nc, stream.ID)

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := m.Subscribe(ctx, subj); err != nil {
		t.Fatal(err)
	}
	ordered := onlyConsumer(t, raw)
	if ordered.Config.InactiveThreshold != 30*time.Second || ordered.Config.Durable != "" {
		t.Errorf("ordered: inactive %s, durable %q", ordered.Config.InactiveThreshold, ordered.Config.Durable)
	}
	cancel()

	d, _ := streams.AsDurable(m)
	if _, err := d.Consume(t.Context(), subj, "defaults"); err != nil {
		t.Fatal(err)
	}
	c, _ := raw.Consumer(t.Context(), "defaults")
	cfg := c.CachedInfo().Config
	if cfg.AckWait != 30*time.Second || cfg.MaxDeliver != -1 {
		t.Errorf("durable: ack wait %s, max deliver %d", cfg.AckWait, cfg.MaxDeliver)
	}
}

// The ack wait brings back what was left unsettled, and max deliver stops after
// that many attempts however the last one ended.
func TestE2EAckWaitAndMaxDeliverBoundRedelivery(t *testing.T) {
	nc := passwordConn(t)
	p := liveJetStream(t, nc, streamsnats.WithAckWait(e2eAckWait), streamsnats.WithMaxDeliver(3))
	subj := e2eUnique("bounded")
	stream, m := liveDeclare(t, p, streams.Stream{Subjects: []string{subj}})
	d, _ := streams.AsDurable(m)
	ch, _ := d.Consume(t.Context(), subj, "bounded", streams.Prefetch(1))
	c, _ := rawStream(t, nc, stream.ID).Consumer(t.Context(), "bounded")
	if cfg := c.CachedInfo().Config; cfg.AckWait != e2eAckWait || cfg.MaxDeliver != 3 {
		t.Errorf("consumer: ack wait %s, max deliver %d", cfg.AckWait, cfg.MaxDeliver)
	}

	id, _ := m.Publish(t.Context(), subj, event{User: "retry"})
	// A nak returns it at once.
	got := next(t, ch, within)
	if got.ID != id || got.Attempt != 1 {
		t.Errorf("first %q attempt %d", got.ID, got.Attempt)
	}
	_ = got.Nak(t.Context())
	// Left unsettled, it returns once the ack wait runs out.
	got = next(t, ch, within)
	if got.ID != id || got.Attempt != 2 {
		t.Errorf("second %q attempt %d", got.ID, got.Attempt)
	}
	left := time.Now()
	got = next(t, ch, e2eAckWait*5)
	if waited := time.Since(left); got.ID != id || got.Attempt != 3 || waited < e2eAckWait/2 {
		t.Errorf("third %q attempt %d after %s", got.ID, got.Attempt, waited)
	}
	// The third attempt is the last, nak or not.
	_ = got.Nak(t.Context())
	nothing(t, ch, e2eAckWait*3)
}

// The ordered consumer behind a subscription outlives it only by the inactive
// threshold.
func TestE2EACanceledSubscriptionReleasesItsOrderedConsumer(t *testing.T) {
	nc := passwordConn(t)
	p := liveJetStream(t, nc, streamsnats.WithInactiveThreshold(e2eInactive))
	subj := e2eUnique("ordered")
	stream, m := liveDeclare(t, p, streams.Stream{Subjects: []string{subj}})
	raw := rawStream(t, nc, stream.ID)
	if n := len(consumers(t, raw)); n != 0 {
		t.Fatalf("%d consumers before subscribing", n)
	}

	ctx, cancel := context.WithCancel(t.Context())
	sub, _ := m.Subscribe(ctx, subj)
	id, _ := m.Publish(t.Context(), subj, event{User: "ordered"})
	if got := next(t, sub, within); got.ID != id {
		t.Errorf("delivered %q", got.ID)
	}
	ordered := onlyConsumer(t, raw)
	if ordered.Config.InactiveThreshold != e2eInactive || ordered.Config.Durable != "" {
		t.Errorf("ordered: inactive %s, durable %q", ordered.Config.InactiveThreshold, ordered.Config.Durable)
	}

	// Still in use: the consumer outlives the threshold while it is pulled.
	time.Sleep(e2eInactive * 2)
	if still := onlyConsumer(t, raw); still.Name != ordered.Name {
		t.Errorf("consumer %s replaced by %s", ordered.Name, still.Name)
	}

	cancel()
	canceled := time.Now()
	for len(consumers(t, raw)) != 0 {
		if time.Since(canceled) > e2eInactive+3*time.Second {
			t.Fatalf("%s outlived its subscription by %s", ordered.Name, time.Since(canceled))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- headers

// Core NATS over mTLS: stream defaults and publish overrides arrive as NATS
// headers another client reads; reserved keys, expiry and retain are refused
// by name.
func TestE2ECoreHeadersOverTLS(t *testing.T) {
	nc := passwordConn(t)
	p := streamsnats.Use(nc)
	subj := e2eUnique("headers.core")
	if _, err := p.Create(t.Context(), liveHeaded(subj).WithHeader("Nats-Msg-Id", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create = %v", err)
	}
	_, m := liveDeclare(t, p, liveHeaded(subj))
	raw, _ := nc.SubscribeSync(subj)
	sub, _ := m.Subscribe(t.Context(), subj)

	id, _ := m.Publish(t.Context(), subj, event{User: "ada"}, streams.Header("site", "plant-4"))
	got := next(t, sub, within)
	if got.ID != id || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-4" {
		t.Errorf("delivered %q with %v", got.ID, got.Headers)
	}
	if native, err := raw.NextMsg(within); err != nil || native.Header.Get("site") != "plant-4" {
		t.Errorf("native = %v, %v", native, err)
	}

	if _, err := m.Publish(t.Context(), subj, event{}, streams.Header("Nats-TTL", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish with Nats-TTL = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Expires(5*time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with Expires = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with Retain = %v", err)
	}
}

// JetStream over mTLS: headers stored with the stream and on every message,
// carried through a redelivery; expiry removes a message; a delayed nak is
// held back by the server; retain and reserved keys are refused.
func TestE2EJetStreamHeadersExpiryAndDelayedNakOverTLS(t *testing.T) {
	nc := passwordConn(t)
	p := liveJetStream(t, nc, streamsnats.WithAckWait(e2eAckWait))
	subj := e2eUnique("headers.js")
	if _, err := p.Create(t.Context(), liveHeaded(subj).WithHeader("Nats-TTL", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create = %v", err)
	}
	stream, m := liveDeclare(t, p, liveHeaded(subj))
	if got, _ := p.Get(t.Context(), stream.ID); got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-3" {
		t.Errorf("Get = %v", got.Headers)
	}
	d, _ := streams.AsDurable(m)
	consumeCtx, stop := context.WithCancel(t.Context())
	ch, _ := d.Consume(consumeCtx, subj, "headed")

	site := streams.Header("site", "plant-4")
	id, _ := m.Publish(t.Context(), subj, event{User: "ada"}, site)
	first := next(t, ch, within)
	if first.Headers.Get("lang") != "go" || first.Headers.Get("site") != "plant-4" || !first.CanDelayRedelivery() {
		t.Errorf("first with %v, can delay %v", first.Headers, first.CanDelayRedelivery())
	}
	naked := time.Now()
	if err := first.NakAfter(t.Context(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	nothing(t, ch, 1500*time.Millisecond)
	second := next(t, ch, within)
	if time.Since(naked) < 2*time.Second || second.ID != id || second.Attempt != 2 || second.Headers.Get("site") != "plant-4" {
		t.Errorf("second %q attempt %d after %s with %v", second.ID, second.Attempt, time.Since(naked), second.Headers)
	}
	_ = second.Ack(t.Context())

	b, _ := streams.AsBatch(m)
	ids, _ := b.PublishBatch(t.Context(), subj, []any{event{User: "a"}, event{User: "b"}}, site)
	for _, id := range ids {
		got := next(t, ch, within)
		if got.ID != id || got.Headers.Get("site") != "plant-4" {
			t.Errorf("batch entry %q arrived as %q with %v", id, got.ID, got.Headers)
		}
		_ = got.Ack(t.Context())
	}
	stop()

	if _, err := m.Publish(t.Context(), subj, event{}, streams.Header("Nats-Msg-Id", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish with Nats-Msg-Id = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{}, streams.Retain()); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with Retain = %v", err)
	}
	if _, err := m.Publish(t.Context(), subj, event{User: "late"}, streams.Expires(time.Second)); err != nil {
		t.Fatal(err)
	}
	raw := rawStream(t, nc, stream.ID)
	deadline := time.Now().Add(within)
	for {
		info, _ := raw.Info(t.Context())
		if info.State.Msgs == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the message did not expire")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- restart

// docker runs docker with args, failing on a non-zero exit, and returns its
// trimmed output.
func docker(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", args...).Output()
	if err != nil {
		t.Fatalf("docker %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

// waitHealthy waits up to a minute for the server container to report healthy.
func waitHealthy(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(time.Minute)
	for {
		status := docker(t, "inspect", "-f", "{{.State.Health.Status}}", e2eContainer)
		if status == "healthy" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is %s", e2eContainer, status)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// Acked work stays done and unacked work comes back after the broker restarts:
// the stream, its headers, the durable consumer, and its position live on the
// named volume.
func TestE2EJetStreamSurvivesABrokerRestart(t *testing.T) {
	if os.Getenv("STREAMS_E2E_RESTART") != "1" {
		t.Skipf("set STREAMS_E2E_RESTART=1 to restart %s", e2eContainer)
	}
	e2eReachable(t)

	subj := e2eUnique("durable.orders")
	const consumer = "restart-workers"
	fast := streamsnats.WithAckWait(e2eAckWait)

	var streamID, acked, inflight, queued string
	func() {
		nc := passwordConn(t)
		defer nc.Close()
		p := liveJetStream(t, nc, fast)
		s, err := p.Create(t.Context(), streams.Stream{Name: "durable", Subjects: []string{subj}}.WithHeader("lang", "go"))
		if err != nil {
			t.Fatal(err)
		}
		streamID = s.ID
		m, _ := p.Bind(t.Context(), s.ID)
		d, _ := streams.AsDurable(m)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ch, _ := d.Consume(ctx, subj, consumer, streams.Prefetch(1))

		acked, _ = m.Publish(t.Context(), subj, event{User: "acked"})
		got := next(t, ch, within)
		if got.ID != acked {
			t.Fatalf("delivered %q, want %q", got.ID, acked)
		}
		_ = got.Ack(t.Context())

		// Delivered and left unsettled when the broker goes down.
		inflight, _ = m.Publish(t.Context(), subj, event{User: "inflight"}, streams.Header("site", "plant-4"))
		if got := next(t, ch, within); got.ID != inflight || got.Attempt != 1 {
			t.Fatalf("delivered %q attempt %d", got.ID, got.Attempt)
		}
		cancel()

		// Stored while nobody pulls, never delivered.
		queued, _ = m.Publish(t.Context(), subj, event{User: "queued"})
		info, _ := rawStream(t, nc, s.ID).Info(t.Context())
		if info.Config.Storage != jetstream.FileStorage || info.State.Msgs != 3 {
			t.Fatalf("storage %v, %d stored", info.Config.Storage, info.State.Msgs)
		}
	}()

	docker(t, "restart", e2eContainer)
	waitHealthy(t)

	nc := passwordConn(t)
	p := liveJetStream(t, nc, fast)
	s, err := p.Get(t.Context(), streamID)
	if err != nil {
		t.Fatalf("the stream did not survive: %v", err)
	}
	if s.Name != "durable" || s.Headers.Get("lang") != "go" {
		t.Errorf("after the restart: %+v", s)
	}
	m, _ := p.Bind(t.Context(), streamID)
	d, _ := streams.AsDurable(m)
	ch, _ := d.Consume(t.Context(), subj, consumer)

	// The never-delivered one comes at once; the in-flight one once its ack
	// wait (shortened, and kept across the restart) runs out, as a second
	// attempt. The acked one never comes back.
	c, _ := rawStream(t, nc, streamID).Consumer(t.Context(), consumer)
	if c.CachedInfo().Config.AckWait != e2eAckWait {
		t.Errorf("ack wait %s after the restart", c.CachedInfo().Config.AckWait)
	}
	var seen []string
	for len(seen) < 2 {
		got := next(t, ch, e2eAckWait+within)
		if got.ID == acked {
			t.Fatal("an acked message was redelivered")
		}
		if got.Headers.Get("lang") != "go" {
			t.Errorf("%q arrived with %v", got.ID, got.Headers)
		}
		if got.ID == inflight {
			if got.Attempt < 2 || got.Headers.Get("site") != "plant-4" {
				t.Errorf("in-flight attempt %d with %v", got.Attempt, got.Headers)
			}
		} else if got.ID != queued {
			t.Errorf("unexpected %q", got.ID)
		}
		seen = append(seen, got.ID)
		_ = got.Ack(t.Context())
	}
	want := []string{inflight, queued}
	slices.Sort(seen)
	slices.Sort(want)
	if !slices.Equal(seen, want) {
		t.Errorf("redelivered %v, want %v", seen, want)
	}
	nothing(t, ch, time.Second)

	if err := p.Delete(t.Context(), streamID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get(t.Context(), streamID); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v", err)
	}
}
