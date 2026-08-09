package redis_test

// These tests need a live server and skip without one. Override the target with
// REDIS_TEST_HOST / REDIS_TEST_PORT (default 127.0.0.1:6379):
//
//	docker compose -f ../../cache/docker/compose.yaml up -d redis
//	go test ./...

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/the-protobuf-project/runtime-go/database"
	dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
)

// dialTimeout bounds the reachability probe: a test that skips should skip
// quickly rather than hang on an unreachable address.
const dialTimeout = 2 * time.Second

var seq atomic.Int64

func testAddr() string {
	host, port := os.Getenv("REDIS_TEST_HOST"), os.Getenv("REDIS_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}
	return net.JoinHostPort(host, port)
}

// requireServer skips the test unless a Redis is reachable, and returns its
// address.
func requireServer(t *testing.T) string {
	t.Helper()
	addr := testAddr()
	ctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()

	var dialer net.Dialer
	c, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Skipf("no Redis at %s: %v", addr, err)
	}
	_ = c.Close()
	return addr
}

// setup returns a database over a live server, namespaced per test so parallel
// runs cannot see each other, and dropped when the test ends.
func setup(t *testing.T) (*database.DB, *database.Resource, protoreflect.MessageDescriptor) {
	t.Helper()
	addr := requireServer(t)

	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	prefix := fmt.Sprintf("itest-%d-%d", os.Getpid(), seq.Add(1))
	p := dbredis.NewProvider(rdb, dbredis.WithPrefix(prefix))
	db, err := p.SetDatabase(context.Background(), "tenant_a")
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}

	md := userMD(t)
	res := userRes(md)
	t.Cleanup(func() {
		_ = db.Schema.DropSchema(context.Background(), res)
		_ = db.Close()
	})
	return db, res, md
}

// userMD builds a dynamic User descriptor exercising the value kinds that a
// JSON encoding would corrupt: bytes and a large uint64.
func userMD(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("itest/v1/redis_test.proto"),
		Package: proto.String("itest.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("User"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("email"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("avatar"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("credits"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func userRes(md protoreflect.MessageDescriptor) *database.Resource {
	return &database.Resource{
		Name:     "User",
		Table:    "users",
		PKColumn: "id",
		New:      func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []database.Column{
			{Name: "id", Field: "id", Kind: database.KindString, PrimaryKey: true, NotNull: true},
			{Name: "email", Field: "email", Kind: database.KindString, Unique: true},
			{Name: "avatar", Field: "avatar", Kind: database.KindBytes},
			{Name: "credits", Field: "credits", Kind: database.KindUint},
		},
	}
}

func newUser(md protoreflect.MessageDescriptor, id, email string, avatar []byte, credits uint64) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("email"), protoreflect.ValueOfString(email))
	m.Set(f.ByName("avatar"), protoreflect.ValueOfBytes(avatar))
	m.Set(f.ByName("credits"), protoreflect.ValueOfUint64(credits))
	return msg
}

func field(msg proto.Message, md protoreflect.MessageDescriptor, name string) protoreflect.Value {
	return msg.ProtoReflect().Get(md.Fields().ByName(protoreflect.Name(name)))
}

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	if _, err := db.Create(ctx, res, newUser(md, "users/ada", "ada@example.com", []byte{1, 2}, 10)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := db.Get(ctx, res, "users/ada")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e := field(got, md, "email").String(); e != "ada@example.com" {
		t.Errorf("email = %q", e)
	}

	ok, err := db.Exists(ctx, res, "users/ada")
	if err != nil || !ok {
		t.Fatalf("Exists = %v, %v; want true", ok, err)
	}

	if _, err := db.Update(ctx, res, newUser(md, "users/ada", "ada2@example.com", []byte{3}, 20)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = db.Get(ctx, res, "users/ada")
	if e := field(got, md, "email").String(); e != "ada2@example.com" {
		t.Errorf("email after update = %q", e)
	}

	if err := db.Delete(ctx, res, "users/ada"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get(ctx, res, "users/ada"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

// The reason records are stored as proto wire format rather than JSON: both of
// these come back wrong through a JSON round trip, and silently.
func TestValuesSurviveTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	// Bytes that are not valid UTF-8, and a uint64 past 2^53 where a float64
	// starts losing integers.
	avatar := []byte{0x00, 0xff, 0xfe, 0x80, 0x01}
	const credits = uint64(9007199254740993)

	if _, err := db.Create(ctx, res, newUser(md, "users/x", "x@example.com", avatar, credits)); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(ctx, res, "users/x")
	if err != nil {
		t.Fatal(err)
	}

	if b := field(got, md, "avatar").Bytes(); !bytes.Equal(b, avatar) {
		t.Errorf("avatar = %v, want %v", b, avatar)
	}
	if c := field(got, md, "credits").Uint(); c != credits {
		t.Errorf("credits = %d, want %d", c, credits)
	}
}

func TestCreateRefusesADuplicateKey(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	if _, err := db.Create(ctx, res, newUser(md, "users/ada", "a@example.com", nil, 0)); err != nil {
		t.Fatal(err)
	}
	_, err := db.Create(ctx, res, newUser(md, "users/ada", "b@example.com", nil, 0))
	if !errors.Is(err, database.ErrAlreadyExists) {
		t.Fatalf("second Create = %v, want ErrAlreadyExists", err)
	}

	// And the first record is untouched.
	got, _ := db.Get(ctx, res, "users/ada")
	if e := field(got, md, "email").String(); e != "a@example.com" {
		t.Errorf("the refused write changed the record: email = %q", e)
	}
}

// A column the descriptor marks Unique has to actually be unique, or the
// descriptor is lying.
func TestUniqueColumnIsEnforced(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	if _, err := db.Create(ctx, res, newUser(md, "users/a", "shared@example.com", nil, 0)); err != nil {
		t.Fatal(err)
	}
	_, err := db.Create(ctx, res, newUser(md, "users/b", "shared@example.com", nil, 0))
	if !errors.Is(err, database.ErrAlreadyExists) {
		t.Fatalf("duplicate email = %v, want ErrAlreadyExists", err)
	}

	// The refused write left nothing behind: the key it claimed is free again.
	if ok, _ := db.Exists(ctx, res, "users/b"); ok {
		t.Error("a refused Create left its record behind")
	}
	if _, err := db.Create(ctx, res, newUser(md, "users/b", "other@example.com", nil, 0)); err != nil {
		t.Errorf("the rolled-back key could not be reused: %v", err)
	}
}

// Changing a unique value must release the old one, or nobody can ever use it
// again.
func TestUpdateMovesAUniqueReservation(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	if _, err := db.Create(ctx, res, newUser(md, "users/a", "old@example.com", nil, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Update(ctx, res, newUser(md, "users/a", "new@example.com", nil, 0)); err != nil {
		t.Fatal(err)
	}

	// The old address is free.
	if _, err := db.Create(ctx, res, newUser(md, "users/b", "old@example.com", nil, 0)); err != nil {
		t.Errorf("the released address could not be reused: %v", err)
	}
	// The new one is not.
	_, err := db.Create(ctx, res, newUser(md, "users/c", "new@example.com", nil, 0))
	if !errors.Is(err, database.ErrAlreadyExists) {
		t.Errorf("the claimed address was not held: %v", err)
	}
}

// Rewriting a record without changing its unique value must not deadlock
// against its own reservation.
func TestUpdateKeepingItsOwnUniqueValue(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	if _, err := db.Create(ctx, res, newUser(md, "users/a", "same@example.com", nil, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Update(ctx, res, newUser(md, "users/a", "same@example.com", nil, 2)); err != nil {
		t.Fatalf("Update keeping its own email: %v", err)
	}
	got, _ := db.Get(ctx, res, "users/a")
	if c := field(got, md, "credits").Uint(); c != 2 {
		t.Errorf("credits = %d, want 2", c)
	}
}

// Deleting must release the reservation, escaped exactly as it was claimed.
func TestDeleteReleasesAUniqueValue(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	// A value containing the separator, which is the case an unescaped key
	// would get wrong.
	const email = "a:b@example.com"
	if _, err := db.Create(ctx, res, newUser(md, "users/a", email, nil, 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(ctx, res, "users/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(ctx, res, newUser(md, "users/b", email, nil, 0)); err != nil {
		t.Errorf("the address was still reserved after the record was deleted: %v", err)
	}
}

func TestListPagesStably(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	const n = 25
	for i := range n {
		id := fmt.Sprintf("users/%02d", i)
		if _, err := db.Create(ctx, res, newUser(md, id, id+"@example.com", nil, uint64(i))); err != nil {
			t.Fatal(err)
		}
	}

	total, err := db.Count(ctx, res, database.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if total != n {
		t.Errorf("Count = %d, want %d", total, n)
	}

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > n {
			t.Fatal("paging did not terminate")
		}
		out, lerr := db.List(ctx, res, database.ListOptions{PageSize: 10, PageToken: token})
		if lerr != nil {
			t.Fatal(lerr)
		}
		for _, m := range out.Items {
			seen = append(seen, field(m, md, "id").String())
		}
		if out.Total != n {
			t.Errorf("page Total = %d, want %d", out.Total, n)
		}
		if out.NextPageToken == "" {
			break
		}
		token = out.NextPageToken
	}

	if len(seen) != n {
		t.Fatalf("paged over %d records, want %d", len(seen), n)
	}
	// Sorted by key, so paging cannot repeat or skip.
	if !sortedAscending(seen) {
		t.Errorf("ids came back out of order: %v", seen)
	}
}

func TestListDescending(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	for i := range 5 {
		id := fmt.Sprintf("users/%02d", i)
		if _, err := db.Create(ctx, res, newUser(md, id, id+"@x.com", nil, 0)); err != nil {
			t.Fatal(err)
		}
	}
	out, err := db.List(ctx, res, database.ListOptions{OrderBy: "id desc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 5 {
		t.Fatalf("got %d items", len(out.Items))
	}
	if first := field(out.Items[0], md, "id").String(); first != "users/04" {
		t.Errorf("first id = %q, want users/04", first)
	}
}

// Concurrent writers racing on one unique value: exactly one wins, and the
// losers leave nothing behind.
func TestConcurrentUniqueClaims(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	const writers = 20
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		wins     int
		conflict int
	)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("users/%d", i)
			_, err := db.Create(ctx, res, newUser(md, id, "contested@example.com", nil, 0))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, database.ErrAlreadyExists):
				conflict++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Errorf("%d writers claimed the same unique value, want 1", wins)
	}
	if conflict != writers-1 {
		t.Errorf("%d conflicts, want %d", conflict, writers-1)
	}

	n, err := db.Count(ctx, res, database.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("%d records survived, want 1 — the losers left something behind", n)
	}
}

// Two tenants, one descriptor: the reason SetDatabase exists.
func TestDatabasesAreIsolated(t *testing.T) {
	ctx := context.Background()
	addr := requireServer(t)

	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	prefix := fmt.Sprintf("itest-%d-%d", os.Getpid(), seq.Add(1))
	p := dbredis.NewProvider(rdb, dbredis.WithPrefix(prefix))
	md := userMD(t)
	res := userRes(md)

	a, err := p.SetDatabase(ctx, "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.SetDatabase(ctx, "tenant_b")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = a.Schema.DropSchema(ctx, res)
		_ = b.Schema.DropSchema(ctx, res)
	}()

	if _, err := a.Create(ctx, res, newUser(md, "users/ada", "ada@example.com", nil, 0)); err != nil {
		t.Fatal(err)
	}

	if _, err := b.Get(ctx, res, "users/ada"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("tenant_b sees tenant_a's record: %v", err)
	}
	// The same unique value is free in the other tenant.
	if _, err := b.Create(ctx, res, newUser(md, "users/ada", "ada@example.com", nil, 0)); err != nil {
		t.Errorf("tenant_b could not use a value tenant_a holds: %v", err)
	}
	if n, _ := a.Count(ctx, res, database.ListOptions{}); n != 1 {
		t.Errorf("tenant_a holds %d records, want 1", n)
	}
}

// Redis has no transactions across these keys, and the contract says so rather
// than pretending.
func TestTransactionsAreRefusedByName(t *testing.T) {
	db, _, _ := setup(t)

	err := db.Tx.Run(context.Background(), func(*database.DB) error { return nil })
	if !errors.Is(err, database.ErrUnimplemented) {
		t.Fatalf("Tx.Run = %v, want ErrUnimplemented", err)
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("the refusal does not name the backend: %v", err)
	}
}

// Redis has no schema, so EnsureSchema succeeding is the honest answer: a
// program that migrates on startup runs unchanged here and against SQL.
func TestSchemaIsAlwaysReady(t *testing.T) {
	ctx := context.Background()
	db, res, _ := setup(t)

	if err := db.Schema.EnsureSchema(ctx, res); err != nil {
		t.Errorf("EnsureSchema: %v", err)
	}
	if has, err := db.Schema.HasSchema(ctx, res); err != nil || !has {
		t.Errorf("HasSchema = %v, %v; want true", has, err)
	}
}

func TestDropSchemaRemovesEverything(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	for i := range 5 {
		id := fmt.Sprintf("users/%d", i)
		if _, err := db.Create(ctx, res, newUser(md, id, id+"@x.com", nil, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Schema.DropSchema(ctx, res); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.Count(ctx, res, database.ListOptions{}); n != 0 {
		t.Errorf("%d records survived the drop", n)
	}
	// And the reservations went with them.
	if _, err := db.Create(ctx, res, newUser(md, "users/0", "users/0@x.com", nil, 0)); err != nil {
		t.Errorf("a unique value survived the drop: %v", err)
	}
}

// A bulk read must cost round trips proportional to batches, not to records.
func TestGetManyReturnsInOrderWithGaps(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	for _, id := range []string{"users/a", "users/c"} {
		if _, err := db.Create(ctx, res, newUser(md, id, id+"@x.com", nil, 0)); err != nil {
			t.Fatal(err)
		}
	}
	batcher, ok := db.Driver.(database.Batcher)
	if !ok {
		t.Fatal("the redis driver does not implement Batcher")
	}

	got, err := batcher.GetMany(ctx, res, []string{"users/a", "users/b", "users/c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d results, want 3", len(got))
	}
	if got[0] == nil || got[2] == nil {
		t.Error("a stored record came back nil")
	}
	if got[1] != nil {
		t.Error("a missing record must be a nil entry, not an error")
	}
	if id := field(got[0], md, "id").String(); id != "users/a" {
		t.Errorf("results are out of order: [0] = %q", id)
	}
}

func TestTypedViewOverRedis(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	users, err := database.For[*dynamicpb.Message](db, res)
	if err != nil {
		t.Fatal(err)
	}
	if _, cerr := users.Create(ctx, newUser(md, "users/ada", "ada@x.com", nil, 7).(*dynamicpb.Message)); cerr != nil {
		t.Fatal(cerr)
	}
	got, err := users.Get(ctx, "users/ada")
	if err != nil {
		t.Fatal(err)
	}
	if c := field(got, md, "credits").Uint(); c != 7 {
		t.Errorf("credits = %d, want 7", c)
	}

	all, err := users.All(ctx, database.ListOptions{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Errorf("All returned %d records, want 1", len(all))
	}
}

func sortedAscending(s []string) bool {
	for i := 1; i < len(s); i++ {
		if s[i-1] >= s[i] {
			return false
		}
	}
	return true
}
