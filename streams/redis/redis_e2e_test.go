//go:build live

package redis_test

// The provider end to end over mutual TLS, against a durable server.
//
//	../docker/tls/generate.sh
//	docker compose -p tpp-streams-e2e-redis -f ../docker/compose.redis.secure.yaml up -d --wait
//	STREAMS_E2E_RESTART=1 go test -race -tags live ./redis -run E2E
//	docker compose -p tpp-streams-e2e-redis -f ../docker/compose.redis.secure.yaml down -v
//
//   - REDIS_E2E_ADDR (default 127.0.0.1:16381) is the TLS port.
//   - REDIS_E2E_CERTS (default ../docker/tls/out) holds ca.crt, client.crt,
//     and client.key.
//   - STREAMS_E2E_RESTART=1 enables the test that restarts the container
//     tpp-streams-redis-secure (REDIS_E2E_CONTAINER) and proves what survives
//     it. Without it that test is skipped, so it never restarts a server
//     somebody else is using by accident.
//
// The tests skip when nothing answers. Every test runs under its own key
// prefix in database 3. The restart test takes a write lock that every other
// test reads, so nothing else is in flight while the server is down.
//
// The server also requires an ACL user (docker/compose.redis.secure.acl): every
// provider here connects as "streams", which may run only the commands the
// driver sends, on only its own keys and channels. The helpers that inspect the
// server behind the provider's back (e2eAdmin) connect as "admin", because
// XINFO, CONFIG GET, PUBSUB NUMSUB, and cleanup of keys a test planted are
// exactly what "streams" is denied.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

// e2eWithin is how long an e2e test waits for something that should happen.
const e2eWithin = 10 * time.Second

// Test-only credentials, matching the hashes in the ACL file.
const (
	streamsUser     = "streams"
	streamsPassword = "streams-e2e-not-a-secret"
	adminUser       = "admin"
	adminPassword   = "admin-e2e-not-a-secret"
)

// e2eServer is read by every ordinary e2e test and written by the restart test.
var e2eServer sync.RWMutex

func e2eAddr() string {
	if addr := os.Getenv("REDIS_E2E_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:16381"
}

func e2eCerts() string {
	if dir := os.Getenv("REDIS_E2E_CERTS"); dir != "" {
		return dir
	}
	return filepath.Join("..", "docker", "tls", "out")
}

func e2eContainer() string {
	if name := os.Getenv("REDIS_E2E_CONTAINER"); name != "" {
		return name
	}
	return "tpp-streams-redis-secure"
}

// login is who a connection authenticates as; a zero login sends no AUTH.
type login struct{ user, password string }

var (
	asStreams = login{streamsUser, streamsPassword}
	asAdmin   = login{adminUser, adminPassword}
)

// e2eTLS is the client side of mutual TLS: the test CA as the only root, and
// the client certificate when mutual.
func e2eTLS(t *testing.T, mutual, trusted bool) *tls.Config {
	t.Helper()
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "localhost"}
	if trusted {
		ca, err := os.ReadFile(filepath.Join(e2eCerts(), "ca.crt"))
		if err != nil {
			t.Skipf("no test certificates (%v); run docker/tls/generate.sh", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		cfg.RootCAs = pool
	}
	if mutual {
		cert, err := tls.LoadX509KeyPair(filepath.Join(e2eCerts(), "client.crt"), filepath.Join(e2eCerts(), "client.key"))
		if err != nil {
			t.Skipf("no test certificates (%v); run docker/tls/generate.sh", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

// e2eClientAs is a client of the TLS server on database 3 logging in as who.
// It is patient, because a TLS handshake on a loaded machine can take a while.
func e2eClientAs(t *testing.T, who login, mutual bool) *goredis.Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{
		Addr:        e2eAddr(),
		DB:          3,
		Username:    who.user,
		Password:    who.password,
		TLSConfig:   e2eTLS(t, mutual, true),
		DialTimeout: 10 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// e2eClient is the mTLS client, as streams, every provider here uses. It skips
// the test when no server answers.
func e2eClient(t *testing.T) *goredis.Client {
	t.Helper()
	rdb := e2eClientAs(t, asStreams, true)
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Skipf("no TLS Redis at %s (%v); start docker/compose.redis.secure.yaml", e2eAddr(), err)
	}
	return rdb
}

// e2eAdmin is a client logged in as admin, for looking behind the provider.
func e2eAdmin(t *testing.T) *goredis.Client {
	t.Helper()
	return e2eClientAs(t, asAdmin, true)
}

// e2ePrefix is a fresh key prefix inside the ACL's e2e* namespaces.
func e2ePrefix() string { return "e2e" + core.NewID() }

// e2eStreams returns streams of mode as streams under prefix p, with
// quickReclaim so a redelivery happens within a test.
func e2eStreams(t *testing.T, rdb goredis.UniversalClient, mode liveMode, p string, opts ...streamsredis.Option) streams.Streams {
	t.Helper()
	return mode(rdb, append([]streamsredis.Option{
		streamsredis.WithPrefix(p), streamsredis.WithReclaimAfter(quickReclaim),
	}, opts...)...)
}

// e2eWithDefaults is a stream on subject stamping lang and site.
func e2eWithDefaults() streams.Stream {
	return streams.Stream{Name: "e2e-headers", Subjects: []string{subject}}.
		WithHeader("lang", "go").
		WithHeader("site", "plant-3")
}

// noperm fails the test unless err is the ACL's refusal.
func noperm(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !strings.HasPrefix(err.Error(), "NOPERM") {
		t.Errorf("%s: expected NOPERM, got %v", what, err)
	}
}

// pendingCount is the group's pending count, asked as admin.
func pendingCount(t *testing.T, key, group string) int64 {
	t.Helper()
	n, err := e2eAdmin(t).XPending(t.Context(), key, group).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	return n.Count
}

// consumerCount is how many consumers a group has, by identity, asked as admin.
func consumerCount(t *testing.T, key, group string) int {
	t.Helper()
	got, err := e2eAdmin(t).XInfoConsumers(t.Context(), key, group).Result()
	if err != nil {
		t.Fatalf("XINFO CONSUMERS: %v", err)
	}
	return len(got)
}

// exists reports whether key exists, asked as admin since streams may not look
// outside its prefixes.
func exists(t *testing.T, key string) bool {
	t.Helper()
	n, err := e2eAdmin(t).Exists(t.Context(), key).Result()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	return n == 1
}

// e2eEventually polls check until it holds, failing after e2eWithin.
func e2eEventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(e2eWithin)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s did not happen in time", what)
}

// --- the transport is really TLS ------------------------------------------------

func TestE2EPlaintextIsRefused(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	plain := goredis.NewClient(&goredis.Options{Addr: e2eAddr(), DB: 3, MaxRetries: -1, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second})
	defer func() { _ = plain.Close() }()
	if err := plain.Ping(t.Context()).Err(); err == nil {
		t.Error("a plaintext PING was answered on the TLS port")
	}
}

func TestE2ETLSWithoutAClientCertificateIsRefused(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	anonymous := e2eClientAs(t, asStreams, false)
	if err := anonymous.Ping(t.Context()).Err(); err == nil {
		t.Error("a TLS client without a certificate was answered")
	}
}

func TestE2EAnUntrustedServerIsRefused(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	// No root: the server's self-made CA is not in the system store, so the
	// client must refuse it rather than talk to it.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: e2eAddr(), DB: 3, Username: streamsUser, Password: streamsPassword,
		TLSConfig: e2eTLS(t, true, false), MaxRetries: -1,
	})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(t.Context()).Err(); err == nil {
		t.Error("a server signed by an unknown CA was trusted")
	}
}

// --- authentication: an ACL user on top of the certificate ----------------------

func TestE2ETheStreamsUserLogsIn(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	if pong, err := rdb.Ping(t.Context()).Result(); err != nil || pong != "PONG" {
		t.Errorf("PING = %q, %v", pong, err)
	}
	if who, err := e2eAdmin(t).Do(t.Context(), "ACL", "WHOAMI").Text(); err != nil || who != adminUser {
		t.Errorf("admin WHOAMI = %q, %v", who, err)
	}
}

// A valid certificate and no password is a connection that can run nothing.
func TestE2ENoPasswordIsRefused(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	err := e2eClientAs(t, login{}, true).Ping(t.Context()).Err()
	if err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Errorf("PING without a password = %v, want NOAUTH", err)
	}
}

func TestE2EAWrongPasswordIsRefused(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	wrong := e2eClientAs(t, login{streamsUser, "not-the-password"}, true)
	if err := wrong.Ping(t.Context()).Err(); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Errorf("PING with a wrong password = %v, want WRONGPASS", err)
	}

	// And the provider says so rather than limping on: its first command fails.
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	_, err := streamsredis.Use(wrong, streamsredis.WithPrefix(e2ePrefix())).
		Create(ctx, streams.Stream{Name: "e2e", Subjects: []string{subject}})
	if err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Errorf("Create with a wrong password = %v, want WRONGPASS", err)
	}
}

// default is off: it cannot log in with any password.
func TestE2ETheDefaultUserIsDisabled(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	for _, password := range []string{"nopass", "default", streamsPassword, adminPassword} {
		err := e2eClientAs(t, login{"default", password}, true).Ping(t.Context()).Err()
		if err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
			t.Errorf("default/%q = %v, want WRONGPASS", password, err)
		}
	}
	admin := e2eAdmin(t)
	rules, err := admin.Do(t.Context(), "ACL", "LIST").StringSlice()
	if err != nil {
		t.Fatalf("ACL LIST: %v", err)
	}
	for _, rule := range rules {
		if strings.HasPrefix(rule, "user default ") && !strings.Contains(rule, " off ") {
			t.Errorf("default is not off: %s", rule)
		}
	}
	users, err := admin.Do(t.Context(), "ACL", "USERS").StringSlice()
	if err != nil || !slices.Equal(users, []string{adminUser, "default", streamsUser}) {
		t.Errorf("ACL USERS = %v, %v", users, err)
	}
}

// The ACL is a real boundary: inside it the driver works (every other test),
// and one step outside, by command, key, or channel, is NOPERM.
func TestE2ETheACLConfinesTheStreamsUser(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	ctx := t.Context()
	mine := e2ePrefix() + ":durable:s:x:y"

	// Commands no mode needs, even on its own keys.
	noperm(t, "CONFIG GET", rdb.ConfigGet(ctx, "port").Err())
	noperm(t, "FLUSHDB", rdb.FlushDB(ctx).Err())
	noperm(t, "KEYS", rdb.Keys(ctx, "*").Err())
	noperm(t, "EVAL", rdb.Eval(ctx, "return 1", nil).Err())
	noperm(t, "GET", rdb.Get(ctx, mine).Err())
	noperm(t, "XINFO", rdb.XInfoStream(ctx, mine).Err())
	noperm(t, "XGROUP DESTROY", rdb.XGroupDestroy(ctx, mine, "g").Err())
	noperm(t, "ACL WHOAMI", rdb.Do(ctx, "ACL", "WHOAMI").Err())

	// A command it has, on a key outside its namespaces.
	noperm(t, "SET outside", rdb.Set(ctx, "outside:key", "x", 0).Err())
	noperm(t, "XADD another prefix", rdb.XAdd(ctx, &goredis.XAddArgs{Stream: "orders:durable:s:x:y", Values: []any{"msg", "x"}}).Err())

	// Channels: another prefix's subjects, and another database's expiries.
	for _, channel := range []string{"orders:stream:ch:x:y", "__keyevent@0__:expired"} {
		noperm(t, "PUBLISH "+channel, rdb.Publish(ctx, channel, "x").Err())
		sub := rdb.Subscribe(ctx, channel)
		_, err := sub.Receive(ctx)
		_ = sub.Close()
		noperm(t, "SUBSCRIBE "+channel, err)
	}

	// Through the provider: a prefix outside the ACL is refused at the first
	// write, and the refusal reaches the caller.
	_, err := streamsredis.UseDurable(rdb, streamsredis.WithPrefix("orders")).
		Create(ctx, streams.Stream{Name: "e2e", Subjects: []string{subject}})
	if err == nil || !strings.Contains(strings.ToUpper(err.Error()), "NOPERM") {
		t.Errorf("Create outside the ACL = %v, want NOPERM", err)
	}
	// Nothing the refusals touched exists.
	if exists(t, "outside:key") || exists(t, "orders:durable:s:x:y") {
		t.Error("a refused write left a key behind")
	}
}

// The server is configured the way the compose file says: TLS only, client
// certificates required, AOF fsynced on every write, expiry events on.
func TestE2ETheServerIsTLSOnlyAndDurable(t *testing.T) {
	e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	admin := e2eAdmin(t)
	get := func(name string) string {
		got, err := admin.ConfigGet(t.Context(), name).Result()
		if err != nil {
			t.Fatalf("CONFIG GET %s: %v", name, err)
		}
		return got[name]
	}
	for name, want := range map[string]string{
		"port": "0", "tls-port": "6380", "tls-auth-clients": "yes",
		"appendonly": "yes", "appendfsync": "always", "aclfile": "/etc/redis/users.acl",
	} {
		if got := get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if events := get("notify-keyspace-events"); !strings.Contains(events, "E") || !strings.Contains(events, "x") {
		t.Errorf("notify-keyspace-events = %q", events)
	}
}

// --- lifecycle ------------------------------------------------------------------

// Create (explicit and generated ids), get, list, update, bind, delete, and
// NotFound after, in each of the three modes, with nothing left behind.
func TestE2ETheFullLifecycleInEveryMode(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	ctx := t.Context()
	p := e2ePrefix()
	modes := []struct {
		kind string
		s    streams.Streams
	}{
		{"stream", e2eStreams(t, rdb, streamsredis.Use, p)},
		{"notify", e2eStreams(t, rdb, streamsredis.UseScheduled, p)},
		{"durable", e2eStreams(t, rdb, streamsredis.UseDurable, p)},
	}
	for _, mode := range modes {
		s := mode.s
		explicit, err := s.Create(ctx, streams.Stream{ID: "orders", Name: "orders", Description: "before", Subjects: []string{subject}})
		if err != nil || explicit.ID != "orders" || !explicit.Active {
			t.Fatalf("%s: Create = %+v, %v", mode.kind, explicit, err)
		}
		generated, err := s.Create(ctx, streams.Stream{Name: "users", Subjects: []string{subject, "user.deleted"}})
		if err != nil || generated.ID == "" || generated.ID == explicit.ID {
			t.Fatalf("%s: Create = %+v, %v", mode.kind, generated, err)
		}
		if got, cerr := s.Get(ctx, generated.ID); cerr != nil || got.ID != generated.ID || !slices.Equal(got.Subjects, generated.Subjects) {
			t.Errorf("%s: Get = %+v, %v", mode.kind, got, cerr)
		}
		list, err := s.List(ctx)
		if err != nil {
			t.Fatalf("%s: List: %v", mode.kind, err)
		}
		ids := []string{}
		for _, st := range list {
			ids = append(ids, st.ID)
		}
		want := []string{explicit.ID, generated.ID}
		slices.Sort(want)
		if !slices.Equal(ids, want) {
			t.Errorf("%s: List = %v, want %v", mode.kind, ids, want)
		}

		updated, err := s.Update(ctx, explicit.ID, streams.Stream{Name: "orders", Description: "after", Subjects: []string{subject, "order.shipped"}})
		if err != nil || updated.ID != explicit.ID {
			t.Fatalf("%s: Update = %+v, %v", mode.kind, updated, err)
		}
		reread, err := s.Get(ctx, explicit.ID)
		if err != nil || reread.Description != "after" || len(reread.Subjects) != 2 {
			t.Errorf("%s: Get after Update = %+v, %v", mode.kind, reread, err)
		}
		meta := fmt.Sprintf("%s:%s:meta:%s", p, mode.kind, explicit.ID)
		if n, cerr := e2eAdmin(t).XLen(ctx, meta).Result(); cerr != nil || n != 1 {
			t.Errorf("%s: an update must replace the metadata entry, not accumulate: XLEN = %d, %v", mode.kind, n, cerr)
		}
		if _, err = s.Update(ctx, "absent", streams.Stream{}); !errors.Is(err, streams.ErrNotFound) {
			t.Errorf("%s: Update absent = %v, want ErrNotFound", mode.kind, err)
		}

		// Bind sees the update: the added subject is now declared.
		m, err := s.Bind(ctx, explicit.ID)
		if err != nil {
			t.Fatalf("%s: Bind: %v", mode.kind, err)
		}
		var opts []streams.Option
		if mode.kind == "notify" {
			opts = append(opts, streams.TTL(time.Minute))
		}
		if _, err := m.Publish(ctx, "order.shipped", event{User: "bind"}, opts...); err != nil {
			t.Errorf("%s: Publish on the added subject: %v", mode.kind, err)
		}

		for _, st := range []streams.Stream{explicit, generated} {
			if err := s.Delete(ctx, st.ID); err != nil {
				t.Fatalf("%s: Delete: %v", mode.kind, err)
			}
			if _, err := s.Get(ctx, st.ID); !errors.Is(err, streams.ErrNotFound) {
				t.Errorf("%s: Get after Delete = %v", mode.kind, err)
			}
			if _, err := s.Bind(ctx, st.ID); !errors.Is(err, streams.ErrNotFound) {
				t.Errorf("%s: Bind after Delete = %v", mode.kind, err)
			}
			if err := s.Delete(ctx, st.ID); !errors.Is(err, streams.ErrNotFound) {
				t.Errorf("%s: second Delete = %v", mode.kind, err)
			}
			if exists(t, fmt.Sprintf("%s:%s:meta:%s", p, mode.kind, st.ID)) {
				t.Errorf("%s: the metadata key outlived delete", mode.kind)
			}
		}
		if list, err := s.List(ctx); err != nil || len(list) != 0 {
			t.Errorf("%s: List after deleting everything = %v, %v", mode.kind, list, err)
		}
	}
}

// --- immediate (pub/sub) ----------------------------------------------------------

func TestE2EPubSubOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	ctx := t.Context()
	p := e2ePrefix()
	s := e2eStreams(t, rdb, streamsredis.Use, p)
	stream, m := bind(t, s, subject, "other")
	channel := fmt.Sprintf("%s:stream:ch:%s:%s", p, stream.ID, subject)
	numsub := func() int64 {
		n, err := e2eAdmin(t).PubSubNumSub(ctx, channel).Result()
		if err != nil {
			t.Fatalf("PUBSUB NUMSUB: %v", err)
		}
		return n[channel]
	}

	subCtx, cancel := context.WithCancel(ctx)
	msgs, err := m.Subscribe(subCtx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if n := numsub(); n != 1 {
		t.Errorf("subscribers = %d, want 1", n)
	}

	// A generated id, then a chosen one, then a partition key, which a
	// partitionless backend accepts and ignores.
	generated, err := m.Publish(ctx, subject, event{User: "ada", Action: "created"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := recvMessage(t, msgs, e2eWithin)
	if got.ID != generated || got.Subject != subject || userOf(t, got) != "ada" {
		t.Errorf("delivered %+v", got)
	}
	if id, cerr := m.Publish(ctx, subject, event{User: "grace"}, streams.ID("order-42")); cerr != nil || id != "order-42" {
		t.Errorf("Publish with an id = %q, %v", id, cerr)
	}
	if got := recvMessage(t, msgs, e2eWithin).ID; got != "order-42" {
		t.Errorf("delivered id %q, want order-42", got)
	}
	if _, err = m.Publish(ctx, subject, event{User: "keyed"}, streams.PartitionKey("tenant-1")); err != nil {
		t.Fatalf("Publish with a partition key: %v", err)
	}
	if user := userOf(t, recvMessage(t, msgs, e2eWithin)); user != "keyed" {
		t.Errorf("delivered %q, want keyed", user)
	}

	// Exactly once, and other subjects stay on their own channel.
	if _, err = m.Publish(ctx, "other", event{User: "elsewhere"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	silent(t, msgs, 500*time.Millisecond)

	// Undeclared subjects and the options that would change the meaning.
	if _, err = m.Publish(ctx, "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish typo = %v", err)
	}
	if _, err = m.Subscribe(ctx, "typo"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe typo = %v", err)
	}
	if _, err = m.Publish(ctx, subject, event{}, streams.TTL(time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", err)
	}
	if _, err = m.Subscribe(ctx, subject, streams.Group("workers")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Subscribe with a group = %v", err)
	}
	if _, err = streams.AsDurable(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsDurable = %v", err)
	}
	if _, err = streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v", err)
	}

	// Prefetch is a request, and asking for none still delivers.
	for _, prefetch := range []int{-1, 1, 128} {
		// Each ends with its iteration, so only msgs is left subscribed.
		sizedCtx, stopSized := context.WithCancel(ctx)
		sized, cerr := m.Subscribe(sizedCtx, subject, streams.Prefetch(prefetch))
		if cerr != nil {
			t.Fatalf("Subscribe: %v", cerr)
		}
		if _, err = m.Publish(ctx, subject, event{User: "sized"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if userOf(t, recvMessage(t, sized, e2eWithin)) != "sized" || userOf(t, recvMessage(t, msgs, e2eWithin)) != "sized" {
			t.Errorf("prefetch %d: wrong message", prefetch)
		}
		stopSized()
	}

	// Batch: one pipeline, ids in order; a single id for several is refused.
	b := asBatch(t, m)
	ids, err := b.PublishBatch(ctx, subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil || len(ids) != 3 {
		t.Fatalf("PublishBatch = %v, %v", ids, err)
	}
	for _, id := range ids {
		if got := recvMessage(t, msgs, e2eWithin).ID; got != id {
			t.Errorf("delivered %q, want %q", got, id)
		}
	}
	if _, err := b.PublishBatch(ctx, subject, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("a batch under one id = %v", err)
	}
	if ids, err := b.PublishBatch(ctx, subject, nil); err != nil || len(ids) != 0 {
		t.Errorf("an empty batch = %v, %v", ids, err)
	}

	// Ending the subscription releases the server-side subscription.
	cancel()
	e2eEventually(t, "the server-side unsubscribe", func() bool { return numsub() == 0 })
	if err := s.Delete(ctx, stream.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestE2ETheTypedViewOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	s := e2eStreams(t, rdb, streamsredis.Use, e2ePrefix())
	stream, m := bind(t, s, subject)
	events := streams.For[event](m)
	received, err := events.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	grace := event{User: "grace", Action: "created"}
	if _, err := events.Publish(t.Context(), subject, grace); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-received:
		if got != grace {
			t.Errorf("got %+v, want %+v", got, grace)
		}
	case <-time.After(e2eWithin):
		t.Fatal("no typed delivery")
	}
	_ = s.Delete(t.Context(), stream.ID)
}

// --- scheduled (TTL expiry) ---------------------------------------------------------

func TestE2EScheduledOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	ctx := t.Context()
	p := e2ePrefix()
	s := e2eStreams(t, rdb, streamsredis.UseScheduled, p)
	stream, m := bind(t, s, "reminder", "unwanted")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	a, err := m.Subscribe(subCtx, "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(subCtx, "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	either := func(wait time.Duration) (streams.Message, bool) {
		select {
		case msg := <-a:
			return msg, true
		case msg := <-b:
			return msg, true
		case <-time.After(wait):
			return streams.Message{}, false
		}
	}

	// Fires after its TTL, under the id it was given, to exactly one of two
	// racing subscribers, and the payload key is claimed away.
	published := time.Now()
	id, err := m.Publish(ctx, "reminder", event{User: "alice", Action: "take a pill"}, streams.TTL(1500*time.Millisecond), streams.ID("pill-1"))
	if err != nil || id != "pill-1" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	payload := fmt.Sprintf("%s:notify:payload:%s", p, id)
	if !exists(t, payload) {
		t.Error("the payload key was not written")
	}
	fired, ok := either(e2eWithin)
	if !ok {
		t.Fatal("the scheduled message never fired")
	}
	if _, again := either(700 * time.Millisecond); again {
		t.Error("one expiry was delivered twice")
	}
	if time.Since(published) < 1500*time.Millisecond {
		t.Errorf("fired after %v, before its TTL", time.Since(published))
	}
	if fired.ID != "pill-1" {
		t.Errorf("fired %q, want pill-1", fired.ID)
	}
	if exists(t, payload) {
		t.Error("a delivered payload key was left behind")
	}

	// Other subjects do not leak in.
	if _, err = m.Publish(ctx, "unwanted", event{User: "nope"}, streams.TTL(200*time.Millisecond)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	silent(t, a, time.Second)

	// A scheduled batch fires whole.
	ids, err := asBatch(t, m).PublishBatch(ctx, "reminder", []any{event{User: "x"}, event{User: "y"}}, streams.TTL(200*time.Millisecond))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		msg, ok := either(e2eWithin)
		if !ok {
			t.Fatalf("the batch fired %d of 2", len(seen))
		}
		seen[msg.ID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("%s never fired", id)
		}
	}

	// A missing or zero TTL, a group, and an undeclared subject are refused.
	if _, err := m.Publish(ctx, "reminder", event{}); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("no TTL = %v", err)
	}
	if _, err := m.Publish(ctx, "reminder", event{}, streams.TTL(0)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("zero TTL = %v", err)
	}
	if _, err := m.Subscribe(ctx, "reminder", streams.Group("g")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("group = %v", err)
	}
	if _, err := m.Publish(ctx, "typo", event{}, streams.TTL(100*time.Millisecond)); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("typo = %v", err)
	}
	if _, err := streams.AsDurable(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsDurable = %v", err)
	}
	cancel()
	// The unwanted subject's payload was never claimed: nobody subscribed to
	// it. It is the leftover a missed expiry leaves, and why a subscriber has to
	// be attached when a scheduled message fires.
	_ = s.Delete(ctx, stream.ID)
}

// --- durable (Redis Streams) ----------------------------------------------------------

func TestE2EDurableOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	ctx := t.Context()
	p := e2ePrefix()
	s := e2eStreams(t, rdb, streamsredis.UseDurable, p)
	stream, m := bind(t, s, subject, "other")
	key := fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)
	d := asDurable(t, m)
	pos, err := streams.AsPositioned(m)
	if err != nil {
		t.Fatalf("AsPositioned: %v", err)
	}

	// A history entry before any consumer exists.
	history, err := m.Publish(ctx, subject, event{User: "history"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Consume (FromNew) with a prefetch: history is skipped, new work arrives
	// at attempt 1 under its chosen id, and an ack clears it.
	workersCtx, stopWorkers := context.WithCancel(ctx)
	workers, err := d.Consume(workersCtx, subject, "workers", streams.Prefetch(4))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if id, cerr := m.Publish(ctx, subject, event{User: "ada", Action: "created"}, streams.ID("evt-1"), streams.PartitionKey("tenant-1")); cerr != nil || id != "evt-1" {
		t.Fatalf("Publish = %q, %v", id, cerr)
	}
	got := recvDelivery(t, workers, e2eWithin)
	if got.ID != "evt-1" || got.Attempt != 1 || userOf(t, got.Message) != "ada" {
		t.Errorf("delivered %+v", got)
	}
	if err = got.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	e2eEventually(t, "the ack to clear the pending list", func() bool { return pendingCount(t, key, "workers") == 0 })

	// Nak: the message stays pending and comes back with a rising attempt.
	retry, err := m.Publish(ctx, subject, event{User: "retry"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := recvDelivery(t, workers, e2eWithin)
	if first.ID != retry {
		t.Fatalf("delivered %q, want %q", first.ID, retry)
	}
	_ = first.Nak(ctx)
	if n := pendingCount(t, key, "workers"); n != 1 {
		t.Errorf("pending after a nak = %d, want 1", n)
	}
	second := recvDelivery(t, workers, e2eWithin)
	if second.ID != retry || second.Attempt < 2 {
		t.Errorf("redelivered %q at attempt %d", second.ID, second.Attempt)
	}
	_ = second.Ack(ctx)

	// An unacked delivery whose consumer goes away is redelivered to the next
	// one under the name; an acked one is not.
	lost, err := m.Publish(ctx, subject, event{User: "lost"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if taken := recvDelivery(t, workers, e2eWithin); taken.ID != lost {
		t.Fatalf("delivered %q, want %q", taken.ID, lost)
	}
	stopWorkers()
	resumedCtx, stopResumed := context.WithCancel(ctx)
	resumed, err := d.Consume(resumedCtx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	back := recvDelivery(t, resumed, e2eWithin)
	if back.ID != lost || back.Attempt < 2 {
		t.Errorf("redelivered %q at attempt %d, want %q", back.ID, back.Attempt, lost)
	}
	_ = back.Ack(ctx)
	silent(t, resumed, time.Second)

	// Positioned: FromEarliest replays history under a new name; FromNew skips it.
	replayCtx, stopReplay := context.WithCancel(ctx)
	replay, err := pos.ConsumeFrom(replayCtx, subject, "replay", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	replayed := make([]string, 0, 4)
	for range 4 {
		delivery := recvDelivery(t, replay, e2eWithin)
		replayed = append(replayed, delivery.ID)
		_ = delivery.Ack(ctx)
	}
	if want := []string{history, "evt-1", retry, lost}; !slices.Equal(replayed, want) {
		t.Errorf("replayed %v, want %v", replayed, want)
	}
	silent(t, replay, 500*time.Millisecond)
	tailCtx, stopTail := context.WithCancel(ctx)
	tail, err := pos.ConsumeFrom(tailCtx, subject, "tail", streams.FromNew)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}
	silent(t, tail, time.Second)
	// Both hold nothing, so stopping them now lets them leave their groups;
	// wait for both before publishing more, which they would otherwise take.
	stopReplay()
	stopTail()
	for _, group := range []string{"replay", "tail"} {
		e2eEventually(t, group+" to leave its group", func() bool { return consumerCount(t, key, group) == 0 })
	}

	// Batch: one pipeline, in order.
	values := make([]any, 6)
	for i := range values {
		values[i] = event{User: "batch", Action: strconv.Itoa(i)}
	}
	ids, err := asBatch(t, m).PublishBatch(ctx, subject, values)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	seen := map[string]bool{}
	for len(seen) < len(ids) {
		delivery := recvDelivery(t, resumed, e2eWithin)
		seen[delivery.ID] = true
		_ = delivery.Ack(ctx)
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("%s was never delivered", id)
		}
	}
	if _, err = asBatch(t, m).PublishBatch(ctx, subject, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("a batch under one id = %v", err)
	}

	// The plain subscriber tails the log.
	tailing, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := m.Publish(ctx, subject, event{User: "tail"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if user := userOf(t, recvMessage(t, tailing, e2eWithin)); user != "tail" {
		t.Errorf("tailed %q", user)
	}
	_ = recvDelivery(t, resumed, e2eWithin).Ack(ctx)

	// Refusals: TTL, group, empty name, undeclared subject.
	if _, err := m.Publish(ctx, subject, event{}, streams.TTL(time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("TTL = %v", err)
	}
	if _, err := d.Consume(ctx, subject, "workers", streams.Group("g")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a group = %v", err)
	}
	if _, err := m.Subscribe(ctx, subject, streams.Group("g")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Subscribe with a group = %v", err)
	}
	if _, err := d.Consume(ctx, subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", err)
	}
	if _, err := d.Consume(ctx, "typo", "workers"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Consume typo = %v", err)
	}
	if _, err := m.Publish(ctx, "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish typo = %v", err)
	}

	// Stopping consumers removes their identities from the groups once they
	// hold nothing. The first "workers" identity stopped while it still held a
	// message, so it stayed in the roster for that message to be reclaimed
	// from; the resumed one, which holds nothing, leaves.
	if n := pendingCount(t, key, "workers"); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}
	stopResumed()
	e2eEventually(t, "workers to keep 1 consumer", func() bool { return consumerCount(t, key, "workers") == 1 })

	// Delete removes the stream; the subject logs are data and are not removed
	// with it.
	if err := s.Delete(ctx, stream.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, stream.ID); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v", err)
	}
	if !exists(t, key) {
		t.Error("the subject log went with the stream")
	}
	_ = e2eAdmin(t).Del(ctx, key).Err()
}

// The retention cap trims the log, approximately.
func TestE2EMaxLenCapsTheLogOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	p := e2ePrefix()
	s := e2eStreams(t, rdb, streamsredis.UseDurable, p, streamsredis.WithMaxLen(10))
	stream, m := bind(t, s, subject)
	values := make([]any, 100)
	for i := range values {
		values[i] = event{User: "u", Action: strconv.Itoa(i)}
	}
	for range 10 {
		if _, err := asBatch(t, m).PublishBatch(t.Context(), subject, values); err != nil {
			t.Fatalf("PublishBatch: %v", err)
		}
	}
	key := fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)
	if n, err := e2eAdmin(t).XLen(t.Context(), key).Result(); err != nil || n >= 1000 {
		t.Errorf("the log kept %d entries (%v)", n, err)
	}
	_ = s.Delete(t.Context(), stream.ID)
	_ = e2eAdmin(t).Del(t.Context(), key).Err()
}

// --- headers, expiry, retain, delayed nak -----------------------------------------------

// Headers in all three modes as the ACL-confined streams user: stream defaults
// arrive, a publish's own override them, durable ones are entry fields any
// client sees, and a header-less message is still the version-1 frame.
func TestE2EHeadersOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	ctx := t.Context()
	p := e2ePrefix()

	// Pub/sub, with a raw subscriber reading the frames.
	immediate := e2eStreams(t, rdb, streamsredis.Use, p)
	stream, m := bindStream(t, immediate, e2eWithDefaults())
	if got, err := immediate.Get(ctx, stream.ID); err != nil {
		t.Fatalf("Get: %v", err)
	} else {
		sameHeaders(t, "stored", got.Headers, defaults())
	}
	subCtx, cancel := context.WithCancel(ctx)
	msgs, err := m.Subscribe(subCtx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err = m.Publish(ctx, subject, event{User: "a"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	sameHeaders(t, "defaults", recvMessage(t, msgs, e2eWithin).Headers, defaults())
	if _, err = m.Publish(ctx, subject, event{User: "b"}, overrideOpts()...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := recvMessage(t, msgs, e2eWithin)
	sameHeaders(t, "overridden", got.Headers, overridden())
	if userOf(t, got) != "b" {
		t.Error("wrong payload")
	}
	cancel()

	plain, pm := bind(t, immediate, subject)
	raw := rdb.Subscribe(ctx, fmt.Sprintf("%s:stream:ch:%s:%s", p, plain.ID, subject))
	if _, err = raw.Receive(ctx); err != nil {
		t.Fatalf("SUBSCRIBE: %v", err)
	}
	id, err := pm.Publish(ctx, subject, event{User: "go"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	want, _ := core.Pack(streams.JSON, id, event{User: "go"})
	select {
	case frame := <-raw.Channel():
		if frame.Payload != string(want) {
			t.Error("a header-less publish is not the version-1 frame")
		}
	case <-time.After(e2eWithin):
		t.Fatal("no raw frame")
	}
	_ = raw.Close()

	// Scheduled.
	scheduled := e2eStreams(t, rdb, streamsredis.UseScheduled, p)
	_, sm := bindStream(t, scheduled, e2eWithDefaults())
	firedCtx, stopFired := context.WithCancel(ctx)
	fired, err := sm.Subscribe(firedCtx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err = sm.Publish(ctx, subject, event{User: "later"}, append(overrideOpts(), streams.TTL(200*time.Millisecond))...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	sameHeaders(t, "scheduled", recvMessage(t, fired, e2eWithin).Headers, overridden())
	stopFired()

	// Durable: entry fields beside a version-1 msg, read back on a redelivery.
	durable := e2eStreams(t, rdb, streamsredis.UseDurable, p)
	dstream, dm := bindStream(t, durable, e2eWithDefaults())
	key := fmt.Sprintf("%s:durable:s:%s:%s", p, dstream.ID, subject)
	consumeCtx, stopConsuming := context.WithCancel(ctx)
	deliveries, err := asDurable(t, dm).Consume(consumeCtx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	id, err = dm.Publish(ctx, subject, event{User: "d"}, overrideOpts()...)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	entries, err := e2eAdmin(t).XRange(ctx, key, "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("XRANGE = %v, %v", entries, err)
	}
	fields := make([]string, 0, len(entries[0].Values))
	for f := range entries[0].Values {
		fields = append(fields, f)
	}
	slices.Sort(fields)
	if !slices.Equal(fields, []string{"h:lang", "h:site", "h:trace", "msg"}) {
		t.Errorf("entry fields = %v", fields)
	}
	if frame := entries[0].Values["msg"].(string); frame[1] != 1 {
		t.Errorf("payload frame version %d, want 1", frame[1])
	}
	first := recvDelivery(t, deliveries, e2eWithin)
	sameHeaders(t, "first delivery", first.Headers, overridden())
	_ = first.Nak(ctx)
	again := recvDelivery(t, deliveries, e2eWithin)
	if again.ID != id || again.Attempt < 2 {
		t.Errorf("redelivered %q at attempt %d", again.ID, again.Attempt)
	}
	sameHeaders(t, "redelivery", again.Headers, overridden())
	_ = again.Ack(ctx)
	stopConsuming()

	// Invalid headers fail where they are set, in every mode.
	for _, s := range []streams.Streams{immediate, scheduled, durable} {
		if _, err := s.Create(ctx, streams.Stream{Name: "x", Subjects: []string{subject}}.WithHeader("a:b", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Create = %v, want ErrInvalidHeader", err)
		}
		if _, err := s.Update(ctx, dstream.ID, streams.Stream{Name: "x"}.WithHeader("bad key", "1")); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Update = %v, want ErrInvalidHeader", err)
		}
	}
	if _, err := dm.Publish(ctx, subject, event{}, streams.Header("k", "a\nb")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Publish = %v, want ErrInvalidHeader", err)
	}

	_ = immediate.Delete(ctx, stream.ID)
	_ = immediate.Delete(ctx, plain.ID)
	_ = durable.Delete(ctx, dstream.ID)
	_ = e2eAdmin(t).Del(ctx, key).Err()
}

func TestE2EExpiryAndRetainAreRefusedOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	p := e2ePrefix()
	for _, c := range everyMode() {
		s := e2eStreams(t, rdb, c.mode, p)
		stream, m := bind(t, s, subject)
		for _, opt := range []streams.Option{streams.Expires(5 * time.Second), streams.Retain()} {
			opts := append(slices.Clone(c.base), opt)
			if _, err := m.Publish(t.Context(), subject, event{}, opts...); !errors.Is(err, streams.ErrUnsupported) {
				t.Errorf("Publish = %v, want ErrUnsupported", err)
			}
			if _, err := asBatch(t, m).PublishBatch(t.Context(), subject, []any{event{}}, opts...); !errors.Is(err, streams.ErrUnsupported) {
				t.Errorf("PublishBatch = %v, want ErrUnsupported", err)
			}
		}
		_ = s.Delete(t.Context(), stream.ID)
	}
}

// A delayed nak over TLS, as streams (whose ACL grants XCLAIM for it): held
// back for the delay, well short of a plain nak's reclaim interval, and counted
// as one redelivery.
func TestE2EADelayedNakOverTLS(t *testing.T) {
	rdb := e2eClient(t)
	e2eServer.RLock()
	defer e2eServer.RUnlock()
	const (
		reclaim = 8 * time.Second
		delay   = 1500 * time.Millisecond
	)
	p := e2ePrefix()
	s := e2eStreams(t, rdb, streamsredis.UseDurable, p, streamsredis.WithReclaimAfter(reclaim))
	stream, m := bindStream(t, s, e2eWithDefaults())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	deliveries, err := asDurable(t, m).Consume(ctx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	id, err := m.Publish(ctx, subject, event{User: "busy"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := recvDelivery(t, deliveries, e2eWithin)
	if !first.CanDelayRedelivery() {
		t.Fatal("a durable delivery cannot delay its redelivery")
	}
	nakedAt := time.Now()
	if err := first.NakAfter(ctx, delay); err != nil {
		t.Fatalf("NakAfter: %v", err)
	}
	silent(t, deliveries, delay-100*time.Millisecond)
	again := recvDelivery(t, deliveries, e2eWithin)
	waited := time.Since(nakedAt)
	if again.ID != id || waited < delay || waited >= reclaim-2*time.Second || again.Attempt != 2 {
		t.Errorf("redelivered %q at attempt %d after %v", again.ID, again.Attempt, waited)
	}
	sameHeaders(t, "delayed redelivery", again.Headers, defaults())

	// Longer than the reclaim interval cannot be honored.
	if err := again.NakAfter(ctx, 2*reclaim); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("NakAfter past the reclaim interval = %v, want ErrUnsupported", err)
	}
	cancel()
	_ = s.Delete(t.Context(), stream.ID)
	_ = e2eAdmin(t).Del(t.Context(), fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)).Err()
}

// --- a broker restart -----------------------------------------------------------------

// restartBroker restarts the container and waits until it answers over mTLS
// again.
func restartBroker(t *testing.T) {
	t.Helper()
	if out, err := exec.CommandContext(t.Context(), "docker", "restart", e2eContainer()).CombinedOutput(); err != nil {
		t.Fatalf("docker restart: %v: %s", err, out)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		health, _ := exec.CommandContext(t.Context(), "docker", "inspect", "-f", "{{.State.Health.Status}}", e2eContainer()).Output()
		if strings.TrimSpace(string(health)) == "healthy" {
			probe := e2eClientAs(t, asStreams, true)
			if pong, err := probe.Ping(t.Context()).Result(); err == nil && pong == "PONG" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not come back healthy", e2eContainer())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// What survives `docker restart` of a server with an fsynced AOF.
//
// Every provider's client predates the restart, and the first command each
// sends afterwards succeeds: a read, and on the immediate stream a PUBLISH,
// which the provider never lets the client repeat, so it can only succeed
// because the dead connection was not used for it.
//
// Durable (Redis Streams): the stream's metadata, the log, the consumer
// group's position, and its pending list all survive. A consumer that was
// running across the restart reconnects on its own, gets back the message it
// held unacknowledged (attempt >= 2), never sees the one it acknowledged, and
// keeps receiving new work; a fresh process under the same name sees the same
// group.
//
// Scheduled (TTL expiry): the timer survives, because the AOF stores the
// pending key's absolute expiry, so a message due after the restart fires to a
// subscriber attached after the restart. A message that comes due while no
// subscriber is attached, which includes the downtime itself, is never
// delivered: Redis publishes the expiry event to nobody and does not replay it.
// Its payload key stays behind, orphaned. That is the mode's contract, not
// something durability settings can change.
func TestE2ESurvivesABrokerRestart(t *testing.T) {
	if os.Getenv("STREAMS_E2E_RESTART") != "1" {
		t.Skipf("set STREAMS_E2E_RESTART=1 to restart %s", e2eContainer())
	}
	rdb := e2eClient(t)
	e2eServer.Lock()
	defer e2eServer.Unlock()
	ctx := t.Context()
	p := e2ePrefix()

	// Durable setup: one acked, one held unacked by a consumer that stays
	// attached through the restart.
	durable := e2eStreams(t, rdb, streamsredis.UseDurable, p)
	stream, m := bindStream(t, durable, e2eWithDefaults())
	key := fmt.Sprintf("%s:durable:s:%s:%s", p, stream.ID, subject)
	workersCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	workers, err := asDurable(t, m).Consume(workersCtx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	acked, err := m.Publish(ctx, subject, event{User: "acked"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	held, err := m.Publish(ctx, subject, event{User: "held"}, overrideOpts()...)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := recvDelivery(t, workers, e2eWithin)
	if first.ID != acked {
		t.Fatalf("delivered %q, want %q", first.ID, acked)
	}
	if err = first.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// Held: neither acked nor naked, the way a crashed handler leaves it.
	if second := recvDelivery(t, workers, e2eWithin); second.ID != held {
		t.Fatalf("delivered %q, want %q", second.ID, held)
	}
	if n := pendingCount(t, key, "workers"); n != 1 {
		t.Fatalf("pending = %d, want 1", n)
	}

	// Scheduled setup: no subscriber attached. One message comes due during
	// the restart, one well after it.
	scheduled := e2eStreams(t, rdb, streamsredis.UseScheduled, p)
	reminders, sm := bind(t, scheduled, "reminder")
	missed, err := sm.Publish(ctx, "reminder", event{User: "missed"}, streams.TTL(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	later, err := sm.Publish(ctx, "reminder", event{User: "later"}, streams.TTL(12*time.Second))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	scheduledAt := time.Now()

	// Immediate setup: a stream whose provider has used its client.
	immediate := e2eStreams(t, rdb, streamsredis.Use, p)
	live, pm := bind(t, immediate, subject)

	restartBroker(t)

	// Everything durable is still on the server.
	if got, cerr := durable.Get(ctx, stream.ID); cerr != nil || got.ID != stream.ID {
		t.Fatalf("durable Get after the restart = %+v, %v", got, cerr)
	}
	if got, cerr := scheduled.Get(ctx, reminders.ID); cerr != nil || got.ID != reminders.ID {
		t.Fatalf("scheduled Get after the restart = %+v, %v", got, cerr)
	}

	// A publish, which is never sent twice, arrives exactly once.
	listenCtx, stopListening := context.WithCancel(ctx)
	listener, err := pm.Subscribe(listenCtx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	once, err := pm.Publish(ctx, subject, event{User: "after-restart"})
	if err != nil {
		t.Fatalf("the first publish after a restart failed: %v", err)
	}
	if got := recvMessage(t, listener, e2eWithin).ID; got != once {
		t.Errorf("delivered %q, want %q", got, once)
	}
	silent(t, listener, 500*time.Millisecond)
	stopListening()
	_ = immediate.Delete(ctx, live.ID)
	if n := pendingCount(t, key, "workers"); n != 1 {
		t.Errorf("the pending list was lost: %d", n)
	}
	if n, cerr := e2eAdmin(t).XLen(ctx, key).Result(); cerr != nil || n != 2 {
		t.Errorf("XLEN = %d, %v; want 2", n, cerr)
	}

	// The consumer that lived through the restart reconnects by itself: the
	// held message comes back, the acked one does not, and new work flows.
	back := recvDelivery(t, workers, e2eWithin)
	if back.ID != held || back.Attempt < 2 {
		t.Errorf("redelivered %q at attempt %d, want %q at 2 or more", back.ID, back.Attempt, held)
	}
	sameHeaders(t, "headers across the restart", back.Headers, overridden())
	_ = back.Ack(ctx)
	fresh, err := m.Publish(ctx, subject, event{User: "after"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	next := recvDelivery(t, workers, e2eWithin)
	if next.ID != fresh || next.Attempt != 1 {
		t.Errorf("delivered %q at attempt %d, want %q at 1 (the acked message came back?)", next.ID, next.Attempt, fresh)
	}
	sameHeaders(t, "stream headers across the restart", next.Headers, defaults())
	_ = next.Ack(ctx)
	silent(t, workers, time.Second)
	stopWorkers()

	// A new process with a new client under the same name: same group, same
	// position, nothing outstanding.
	reborn := e2eStreams(t, e2eClient(t), streamsredis.UseDurable, p)
	m2, err := reborn.Bind(ctx, stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	againCtx, stopAgain := context.WithCancel(ctx)
	again, err := asDurable(t, m2).Consume(againCtx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	silent(t, again, time.Second)
	stopAgain()

	// Scheduled: the late timer survived; the one that came due while nobody
	// listened is gone for good, its payload orphaned.
	subCtx, stopSub := context.WithCancel(ctx)
	defer stopSub()
	subscriber, err := sm.Subscribe(subCtx, "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	pendingLater := fmt.Sprintf("%s:notify:pending:%s:reminder:%s", p, reminders.ID, later)
	if !exists(t, pendingLater) && time.Since(scheduledAt) < 12*time.Second {
		t.Error("the scheduled timer did not survive the restart")
	}
	fired := recvMessage(t, subscriber, 20*time.Second)
	if fired.ID != later || time.Since(scheduledAt) < 12*time.Second {
		t.Errorf("fired %q after %v", fired.ID, time.Since(scheduledAt))
	}
	silent(t, subscriber, time.Second)
	missedPayload := fmt.Sprintf("%s:notify:payload:%s", p, missed)
	if !exists(t, missedPayload) {
		t.Error("expected the missed message's payload to be left behind")
	}
	stopSub()

	// Clean up what delete does not own.
	_ = durable.Delete(ctx, stream.ID)
	_ = scheduled.Delete(ctx, reminders.ID)
	_ = e2eAdmin(t).Del(ctx, key, missedPayload).Err()
}
