package mongodb_test

// These need a live MongoDB replica set and skip without one. Transactions and
// change streams are replica-set features, so a standalone server would leave
// both untestable here and quietly different in production:
//
//	docker compose -f ../docker/compose.yaml up -d mongodb
//	go test ./...

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/the-protobuf-project/runtime-go/database/mongodb"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

const dialTimeout = 2 * time.Second

var seq atomic.Int64

func testAddr() string {
	host, port := os.Getenv("MONGO_TEST_HOST"), os.Getenv("MONGO_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "27017"
	}
	return net.JoinHostPort(host, port)
}

func setup(t *testing.T) (*store.DB, *store.Resource, protoreflect.MessageDescriptor) {
	t.Helper()
	addr := testAddr()

	dctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		t.Skipf("no MongoDB at %s: %v", addr, err)
	}
	_ = conn.Close()

	client, err := mongodb.NewClient(t.Context(), mongodb.Config{Address: addr})
	if err != nil {
		t.Skipf("cannot reach MongoDB at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })

	p := mongodb.NewProvider(client)
	dbName := fmt.Sprintf("itest_%d_%d", os.Getpid(), seq.Add(1))
	db, err := p.SetDatabase(t.Context(), dbName)
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = p.DropDatabase(context.Background(), dbName)
		_ = db.Close()
	})

	md := userMD(t)
	res := userRes(md)
	if err := db.Schema.EnsureSchema(t.Context(), res); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return db, res, md
}

func userMD(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("itest/v1/mongodb_test.proto"),
		Package: proto.String("itestmongo.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("User"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("email"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("age"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("avatar"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("credits"), Number: proto.Int32(5), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func userRes(md protoreflect.MessageDescriptor) *store.Resource {
	return &store.Resource{
		Name: "User", Table: "users", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "email", Field: "email", Kind: store.KindString, Unique: true},
			{Name: "age", Field: "age", Kind: store.KindInt},
			{Name: "avatar", Field: "avatar", Kind: store.KindBytes},
			{Name: "credits", Field: "credits", Kind: store.KindUint},
		},
	}
}

func newUser(md protoreflect.MessageDescriptor, id, email string, age int32, avatar []byte, credits uint64) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("email"), protoreflect.ValueOfString(email))
	m.Set(f.ByName("age"), protoreflect.ValueOfInt32(age))
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

	if _, err := db.Create(ctx, res, newUser(md, "users/ada", "ada@example.com", 36, []byte{1, 2}, 10)); err != nil {
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

	if _, err := db.Update(ctx, res, newUser(md, "users/ada", "ada2@example.com", 37, nil, 11)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = db.Get(ctx, res, "users/ada")
	if e := field(got, md, "email").String(); e != "ada2@example.com" {
		t.Errorf("email after update = %q", e)
	}

	if err := db.Delete(ctx, res, "users/ada"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get(ctx, res, "users/ada"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := db.Delete(ctx, res, "users/ada"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete of a missing record = %v, want ErrNotFound", err)
	}
}

// BSON has no unsigned 64-bit integer, so a large uint64 would come back a
// negative number if it were handed to the driver as-is.
func TestValuesSurviveTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	avatar := []byte{0x00, 0xff, 0xfe, 0x80, 0x01}
	const credits = uint64(18446744073709551615) // 2^64-1

	if _, err := db.Create(ctx, res, newUser(md, "users/x", "x@example.com", 1, avatar, credits)); err != nil {
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

	if _, err := db.Create(ctx, res, newUser(md, "users/a", "a@example.com", 1, nil, 0)); err != nil {
		t.Fatal(err)
	}
	_, err := db.Create(ctx, res, newUser(md, "users/a", "b@example.com", 2, nil, 0))
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second Create = %v, want ErrAlreadyExists", err)
	}
}

// A column the descriptor marks Unique is only unique if EnsureSchema built the
// index for it.
func TestUniqueColumnIsEnforced(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	if _, err := db.Create(ctx, res, newUser(md, "users/a", "shared@example.com", 1, nil, 0)); err != nil {
		t.Fatal(err)
	}
	_, err := db.Create(ctx, res, newUser(md, "users/b", "shared@example.com", 2, nil, 0))
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate email = %v, want ErrAlreadyExists", err)
	}
}

func TestListPagesAndFilters(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	const n = 25
	for i := range n {
		id := fmt.Sprintf("users/%02d", i)
		if _, err := db.Create(ctx, res, newUser(md, id, id+"@example.com", int32(i), nil, 0)); err != nil {
			t.Fatal(err)
		}
	}

	total, err := db.Count(ctx, res, store.ListOptions{})
	if err != nil || total != n {
		t.Fatalf("Count = %d, %v; want %d", total, err, n)
	}

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > n {
			t.Fatal("paging did not terminate")
		}
		out, lerr := db.List(ctx, res, store.ListOptions{PageSize: 10, PageToken: token})
		if lerr != nil {
			t.Fatal(lerr)
		}
		for _, m := range out.Items {
			seen = append(seen, field(m, md, "id").String())
		}
		if out.NextPageToken == "" {
			break
		}
		token = out.NextPageToken
	}
	if len(seen) != n {
		t.Fatalf("paged over %d records, want %d", len(seen), n)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] >= seen[i] {
			t.Fatalf("ids came back out of order at %d: %v", i, seen[i-1:i+1])
		}
	}

	// The server does the filtering, so a page size means what it says.
	out, err := db.List(ctx, res, store.ListOptions{Filter: "age >= 20"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 5 {
		t.Errorf("filter returned %d records, want 5", len(out.Items))
	}
	if out.Total != 5 {
		t.Errorf("filtered Total = %d, want 5", out.Total)
	}

	if _, err := db.List(ctx, res, store.ListOptions{Filter: "nosuch = 1"}); err == nil {
		t.Error("a filter naming an unknown column was accepted")
	}
	if _, err := db.List(ctx, res, store.ListOptions{Filter: "age LIKE 3"}); err == nil {
		t.Error("a filter this backend cannot honor was accepted rather than refused")
	}
}

func TestListDescending(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)
	for i := range 5 {
		id := fmt.Sprintf("users/%02d", i)
		if _, err := db.Create(ctx, res, newUser(md, id, id+"@x.com", int32(i), nil, 0)); err != nil {
			t.Fatal(err)
		}
	}
	out, err := db.List(ctx, res, store.ListOptions{OrderBy: "id desc"})
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

func TestTransactionCommitsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	err := db.Tx.Run(ctx, func(tx *store.DB) error {
		if _, cerr := tx.Create(ctx, res, newUser(md, "users/a", "a@x.com", 1, nil, 0)); cerr != nil {
			return cerr
		}
		_, cerr := tx.Create(ctx, res, newUser(md, "users/b", "b@x.com", 2, nil, 0))
		return cerr
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n, _ := db.Count(ctx, res, store.ListOptions{}); n != 2 {
		t.Errorf("committed %d records, want 2", n)
	}

	boom := errors.New("second write failed")
	err = db.Tx.Run(ctx, func(tx *store.DB) error {
		if _, cerr := tx.Create(ctx, res, newUser(md, "users/c", "c@x.com", 3, nil, 0)); cerr != nil {
			return cerr
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the caller's error", err)
	}
	if n, _ := db.Count(ctx, res, store.ListOptions{}); n != 2 {
		t.Errorf("%d records after a rollback, want 2 — the transaction leaked a write", n)
	}
}

func TestSchemaLifecycle(t *testing.T) {
	ctx := context.Background()
	db, res, _ := setup(t)

	if has, err := db.Schema.HasSchema(ctx, res); err != nil || !has {
		t.Fatalf("HasSchema = %v, %v; want true", has, err)
	}
	// Idempotent: it runs on startup.
	for range 3 {
		if err := db.Schema.EnsureSchema(ctx, res); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
	}
	if err := db.Schema.DropSchema(ctx, res); err != nil {
		t.Fatal(err)
	}
	if has, err := db.Schema.HasSchema(ctx, res); err != nil || has {
		t.Errorf("HasSchema after drop = %v, %v; want false", has, err)
	}
}

func TestBulk(t *testing.T) {
	ctx := context.Background()
	db, res, md := setup(t)

	batcher, ok := db.Driver.(store.Batcher)
	if !ok {
		t.Fatal("the mongodb driver does not implement Batcher")
	}

	msgs := []proto.Message{
		newUser(md, "users/a", "a@x.com", 1, nil, 0),
		newUser(md, "users/b", "b@x.com", 2, nil, 0),
		newUser(md, "users/c", "c@x.com", 3, nil, 0),
	}
	out, err := batcher.CreateMany(ctx, res, msgs)
	if err != nil {
		t.Fatalf("CreateMany: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("CreateMany returned %d results, want 3", len(out))
	}

	got, err := batcher.GetMany(ctx, res, []string{"users/a", "users/missing", "users/c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("GetMany returned %d results, want 3", len(got))
	}
	if got[1] != nil {
		t.Error("a missing record must be a nil entry, not an error")
	}
	if id := field(got[0], md, "id").String(); id != "users/a" {
		t.Errorf("results are out of order: [0] = %q", id)
	}
	if id := field(got[2], md, "id").String(); id != "users/c" {
		t.Errorf("results are out of order: [2] = %q", id)
	}
}

// The capability that makes MongoDB worth reaching for when something has to
// react to writes: the alternative without it is polling.
func TestWatchDeliversChanges(t *testing.T) {
	db, res, md := setup(t)

	watcher, ok := db.Driver.(store.Watcher)
	if !ok {
		t.Fatal("the mongodb driver does not implement Watcher")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes, err := watcher.Watch(ctx, res, store.WatchOptions{})
	if err != nil {
		t.Skipf("change streams unavailable (needs a replica set): %v", err)
	}

	if _, err := db.Create(context.Background(), res, newUser(md, "users/a", "a@x.com", 1, nil, 0)); err != nil {
		t.Fatal(err)
	}

	select {
	case c := <-changes:
		if c.Kind != store.ChangeCreated {
			t.Errorf("Kind = %v, want ChangeCreated", c.Kind)
		}
		if c.Key != "users/a" {
			t.Errorf("Key = %q, want users/a", c.Key)
		}
		if c.Message == nil {
			t.Error("an insert must carry the record")
		} else if e := field(c.Message, md, "email").String(); e != "a@x.com" {
			t.Errorf("email = %q", e)
		}
		if c.Resume == "" {
			t.Error("a change must carry a resume token")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no change delivered within 10s")
	}

	// Canceling closes the channel rather than leaking the goroutine.
	cancel()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, open := <-changes:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("the change channel was not closed after cancellation")
		}
	}
}

// Two tenants, one descriptor, real server-side isolation.
func TestDatabasesAreIsolated(t *testing.T) {
	ctx := context.Background()
	addr := testAddr()

	dctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		t.Skipf("no MongoDB at %s: %v", addr, err)
	}
	_ = conn.Close()

	client, err := mongodb.NewClient(ctx, mongodb.Config{Address: addr})
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = client.Close(ctx) }()

	p := mongodb.NewProvider(client)
	md := userMD(t)
	res := userRes(md)

	nameA := fmt.Sprintf("itest_a_%d_%d", os.Getpid(), seq.Add(1))
	nameB := fmt.Sprintf("itest_b_%d_%d", os.Getpid(), seq.Add(1))
	a, err := p.SetDatabase(ctx, nameA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.SetDatabase(ctx, nameB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = p.DropDatabase(ctx, nameA)
		_ = p.DropDatabase(ctx, nameB)
	}()

	if _, err := a.Create(ctx, res, newUser(md, "users/ada", "ada@x.com", 1, nil, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, res, "users/ada"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("tenant B sees tenant A's record: %v", err)
	}
	// The same unique value is free in the other store.
	if _, err := b.Create(ctx, res, newUser(md, "users/ada", "ada@x.com", 1, nil, 0)); err != nil {
		t.Errorf("tenant B could not use a value tenant A holds: %v", err)
	}
}

func TestDatabaseNameIsChecked(t *testing.T) {
	ctx := context.Background()
	// A short timeout, because this test has nothing to say about an
	// unreachable server: without one it waits out the driver's thirty-second
	// server-selection default before skipping, which turns a four-second suite
	// into a thirty-four-second one whenever the container is not up.
	client, err := mongodb.NewClient(ctx, mongodb.Config{
		Address:        testAddr(),
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("no MongoDB: %v", err)
	}
	defer func() { _ = client.Close(ctx) }()

	if _, err := mongodb.NewProvider(client).SetDatabase(ctx, "a b"); err == nil {
		t.Error("SetDatabase accepted a name with a space")
	}
}

// A password containing @ or / is ordinary, and concatenating one into a URI
// either fails to parse or connects somewhere else entirely.
func TestCredentialsAreEscaped(t *testing.T) {
	ctx := context.Background()
	_, err := mongodb.NewClient(ctx, mongodb.Config{
		Address:        "127.0.0.1:1",
		Username:       "user",
		Password:       "p@ss/word:1",
		ConnectTimeout: 200 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected a connection failure against a dead port")
	}
	// It must fail reaching 127.0.0.1:1 — not reaching a host called "word:1"
	// or failing to parse.
	if strings.Contains(err.Error(), "word") {
		t.Errorf("the password leaked into the host: %v", err)
	}
}
