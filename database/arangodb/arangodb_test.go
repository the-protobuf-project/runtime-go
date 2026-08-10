package arangodb_test

// These need a live ArangoDB and skip without one:
//
//	docker compose -f ../docker/compose.yaml up -d arangodb
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

	"github.com/the-protobuf-project/runtime-go/database/arangodb"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

const dialTimeout = 2 * time.Second

var seq atomic.Int64

func endpoint() string {
	host, port := os.Getenv("ARANGO_TEST_HOST"), os.Getenv("ARANGO_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "8529"
	}
	return "http://" + net.JoinHostPort(host, port)
}

type fixture struct {
	db     *store.DB
	user   *store.Resource
	org    *store.Resource
	member *store.Resource
	md     protoreflect.MessageDescriptor
	edgeMD protoreflect.MessageDescriptor
}

func setup(t *testing.T) fixture {
	t.Helper()
	addr := strings.TrimPrefix(endpoint(), "http://")

	dctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		t.Skipf("no ArangoDB at %s: %v", addr, err)
	}
	_ = conn.Close()

	client, err := arangodb.NewClient(t.Context(), arangodb.Config{
		Endpoints:      []string{endpoint()},
		Username:       "root",
		Password:       "root",
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("cannot reach ArangoDB: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	md, edgeMD := schemas(t)
	userRes := &store.Resource{
		Name: "User", Table: "users", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "email", Field: "email", Kind: store.KindString, Unique: true},
			{Name: "age", Field: "age", Kind: store.KindInt},
			{Name: "avatar", Field: "avatar", Kind: store.KindBytes},
		},
	}
	orgRes := &store.Resource{
		Name: "Org", Table: "orgs", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "email", Field: "email", Kind: store.KindString},
			{Name: "age", Field: "age", Kind: store.KindInt},
			{Name: "avatar", Field: "avatar", Kind: store.KindBytes},
		},
	}
	memberRes := &store.Resource{
		Name: "MemberOf", Table: "member_of", PKColumn: "id", IsEdge: true,
		New: func() proto.Message { return dynamicpb.NewMessage(edgeMD) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true},
			{Name: "role", Field: "role", Kind: store.KindString},
		},
	}

	reg := store.NewRegistry(*userRes, *orgRes, *memberRes)
	p := arangodb.NewProvider(client, arangodb.WithRegistry(reg))

	name := fmt.Sprintf("itest_%d_%d", os.Getpid(), seq.Add(1))
	if eerr := p.EnsureDatabase(t.Context(), name); eerr != nil {
		t.Fatalf("EnsureDatabase: %v", eerr)
	}
	db, err := p.SetDatabase(t.Context(), name)
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = p.DropDatabase(context.Background(), name)
		_ = db.Close()
	})

	for _, r := range []*store.Resource{userRes, orgRes, memberRes} {
		if err := db.Schema.EnsureSchema(t.Context(), r); err != nil {
			t.Fatalf("EnsureSchema %s: %v", r.Name, err)
		}
	}
	return fixture{db: db, user: userRes, org: orgRes, member: memberRes, md: md, edgeMD: edgeMD}
}

func schemas(t *testing.T) (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor) {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("itest/v1/arangodb_test.proto"),
		Package: proto.String("itestarango.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("User"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
					{Name: proto.String("email"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
					{Name: proto.String("age"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
					{Name: proto.String("avatar"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				},
			},
			{
				Name: proto.String("MemberOf"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
					{Name: proto.String("role"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0), fd.Messages().Get(1)
}

func newUser(md protoreflect.MessageDescriptor, id, email string, age int32, avatar []byte) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("email"), protoreflect.ValueOfString(email))
	m.Set(f.ByName("age"), protoreflect.ValueOfInt32(age))
	m.Set(f.ByName("avatar"), protoreflect.ValueOfBytes(avatar))
	return msg
}

func field(msg proto.Message, md protoreflect.MessageDescriptor, name string) protoreflect.Value {
	return msg.ProtoReflect().Get(md.Fields().ByName(protoreflect.Name(name)))
}

// ---------------------------------------------------------------- record half

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/ada", "ada@x.com", 36, []byte{1, 2})); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := f.db.Get(ctx, f.user, "users/ada")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e := field(got, f.md, "email").String(); e != "ada@x.com" {
		t.Errorf("email = %q", e)
	}
	if id := field(got, f.md, "id").String(); id != "users/ada" {
		t.Errorf("id came back as %q — the key encoding did not round-trip", id)
	}

	ok, err := f.db.Exists(ctx, f.user, "users/ada")
	if err != nil || !ok {
		t.Fatalf("Exists = %v, %v; want true", ok, err)
	}

	if _, err := f.db.Update(ctx, f.user, newUser(f.md, "users/ada", "ada2@x.com", 37, nil)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = f.db.Get(ctx, f.user, "users/ada")
	if e := field(got, f.md, "email").String(); e != "ada2@x.com" {
		t.Errorf("email after update = %q", e)
	}

	if err := f.db.Delete(ctx, f.user, "users/ada"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := f.db.Get(ctx, f.user, "users/ada"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := f.db.Delete(ctx, f.user, "users/ada"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete of a missing record = %v, want ErrNotFound", err)
	}
}

// An AIP resource name contains a slash, which ArangoDB refuses as a document
// key because it separates a collection from a key in the id it builds.
func TestKeysWithSlashesRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	for _, id := range []string{"users/ada", "users/a/b/c", "plain", "users/100%"} {
		if _, err := f.db.Create(ctx, f.user, newUser(f.md, id, id+"@x.com", 1, nil)); err != nil {
			t.Fatalf("Create(%q): %v", id, err)
		}
		got, err := f.db.Get(ctx, f.user, id)
		if err != nil {
			t.Fatalf("Get(%q): %v", id, err)
		}
		if back := field(got, f.md, "id").String(); back != id {
			t.Errorf("id %q came back as %q", id, back)
		}
	}
}

// JSON has no binary, so bytes need an encoding that survives and that a plain
// string cannot be mistaken for.
func TestBytesSurviveTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	avatar := []byte{0x00, 0xff, 0xfe, 0x80, 0x01}
	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/x", "x@x.com", 1, avatar)); err != nil {
		t.Fatal(err)
	}
	got, err := f.db.Get(ctx, f.user, "users/x")
	if err != nil {
		t.Fatal(err)
	}
	if b := field(got, f.md, "avatar").Bytes(); !bytes.Equal(b, avatar) {
		t.Errorf("avatar = %v, want %v", b, avatar)
	}

	// And a caller's own base64-looking string stays a string.
	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/y", "dGVzdA==", 1, nil)); err != nil {
		t.Fatal(err)
	}
	got, _ = f.db.Get(ctx, f.user, "users/y")
	if e := field(got, f.md, "email").String(); e != "dGVzdA==" {
		t.Errorf("a base64-shaped string was decoded as bytes: %q", e)
	}
}

func TestDuplicateAndUnique(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/a", "a@x.com", 1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/a", "b@x.com", 2, nil)); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("duplicate key = %v, want ErrAlreadyExists", err)
	}
	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/b", "a@x.com", 2, nil)); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("duplicate unique column = %v, want ErrAlreadyExists", err)
	}
}

func TestListPagesFiltersAndOrders(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	const n = 25
	for i := range n {
		id := fmt.Sprintf("users/%02d", i)
		if _, err := f.db.Create(ctx, f.user, newUser(f.md, id, id+"@x.com", int32(i), nil)); err != nil {
			t.Fatal(err)
		}
	}
	if total, err := f.db.Count(ctx, f.user, store.ListOptions{}); err != nil || total != n {
		t.Fatalf("Count = %d, %v; want %d", total, err, n)
	}

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		if pages > n {
			t.Fatal("paging did not terminate")
		}
		out, err := f.db.List(ctx, f.user, store.ListOptions{PageSize: 10, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Items {
			seen = append(seen, field(m, f.md, "id").String())
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
			t.Fatalf("out of order at %d: %v", i, seen[i-1:i+1])
		}
	}

	out, err := f.db.List(ctx, f.user, store.ListOptions{Filter: "age >= 20"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 5 || out.Total != 5 {
		t.Errorf("filter returned %d items, total %d; want 5 and 5", len(out.Items), out.Total)
	}

	desc, err := f.db.List(ctx, f.user, store.ListOptions{OrderBy: "id desc", PageSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	if first := field(desc.Items[0], f.md, "id").String(); first != "users/24" {
		t.Errorf("first descending id = %q, want users/24", first)
	}

	if _, err := f.db.List(ctx, f.user, store.ListOptions{Filter: "nosuch = 1"}); err == nil {
		t.Error("a filter naming an unknown column was accepted")
	}
	if _, err := f.db.List(ctx, f.user, store.ListOptions{Filter: "age LIKE 3"}); err == nil {
		t.Error("a filter this backend cannot honor was accepted rather than refused")
	}
}

func TestTransactionCommitsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	err := f.db.Tx.Run(ctx, func(tx *store.DB) error {
		if _, cerr := tx.Create(ctx, f.user, newUser(f.md, "users/a", "a@x.com", 1, nil)); cerr != nil {
			return cerr
		}
		_, cerr := tx.Create(ctx, f.user, newUser(f.md, "users/b", "b@x.com", 2, nil))
		return cerr
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n, _ := f.db.Count(ctx, f.user, store.ListOptions{}); n != 2 {
		t.Errorf("committed %d records, want 2", n)
	}

	boom := errors.New("second write failed")
	err = f.db.Tx.Run(ctx, func(tx *store.DB) error {
		if _, cerr := tx.Create(ctx, f.user, newUser(f.md, "users/c", "c@x.com", 3, nil)); cerr != nil {
			return cerr
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the caller's error", err)
	}
	if n, _ := f.db.Count(ctx, f.user, store.ListOptions{}); n != 2 {
		t.Errorf("%d records after a rollback, want 2 — the transaction leaked a write", n)
	}
}

func TestBulk(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	batcher, ok := f.db.Driver.(store.Batcher)
	if !ok {
		t.Fatal("the arangodb driver does not implement Batcher")
	}
	msgs := []proto.Message{
		newUser(f.md, "users/a", "a@x.com", 1, nil),
		newUser(f.md, "users/b", "b@x.com", 2, nil),
		newUser(f.md, "users/c", "c@x.com", 3, nil),
	}
	if _, err := batcher.CreateMany(ctx, f.user, msgs); err != nil {
		t.Fatalf("CreateMany: %v", err)
	}

	got, err := batcher.GetMany(ctx, f.user, []string{"users/a", "users/missing", "users/c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("GetMany returned %d results, want 3", len(got))
	}
	if got[1] != nil {
		t.Error("a missing record must be a nil entry, not an error")
	}
	if id := field(got[0], f.md, "id").String(); id != "users/a" {
		t.Errorf("results are out of order: [0] = %q", id)
	}
}

// ----------------------------------------------------------------- graph half

func TestConnectAndNeighbors(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/ada", "ada@x.com", 36, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Create(ctx, f.org, newUser(f.md, "orgs/acme", "acme@x.com", 0, nil)); err != nil {
		t.Fatal(err)
	}

	ada := store.Ref{Resource: "User", Key: "users/ada"}
	acme := store.Ref{Resource: "Org", Key: "orgs/acme"}

	edge, err := f.db.Graph.Connect(ctx, f.member, ada, acme, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if edge.Key == "" {
		t.Error("Connect returned no key")
	}
	if edge.From != ada || edge.To != acme {
		t.Errorf("edge endpoints = %v -> %v", edge.From, edge.To)
	}

	edges, err := f.db.Graph.Neighbors(ctx, ada, store.TraverseOptions{
		Types: []string{"MemberOf"}, Direction: store.Outbound,
	})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("got %d neighbors, want 1", len(edges))
	}
	if edges[0].To != acme {
		t.Errorf("neighbor = %v, want %v", edges[0].To, acme)
	}
	if edges[0].Type != "MemberOf" {
		t.Errorf("edge type = %q, want MemberOf", edges[0].Type)
	}

	// Inbound finds it from the other end.
	back, err := f.db.Graph.Neighbors(ctx, acme, store.TraverseOptions{
		Types: []string{"MemberOf"}, Direction: store.Inbound,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 || back[0].From != ada {
		t.Errorf("inbound neighbors = %v", back)
	}

	if err := f.db.Graph.Disconnect(ctx, f.member, edge.Key); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	edges, _ = f.db.Graph.Neighbors(ctx, ada, store.TraverseOptions{Types: []string{"MemberOf"}})
	if len(edges) != 0 {
		t.Errorf("%d edges survived Disconnect", len(edges))
	}
}

// An edge pointing at a document that is not there is a dangling edge ArangoDB
// would happily store, and it surfaces much later as a traversal returning
// nothing for a record that is plainly present.
func TestConnectRefusesAMissingEndpoint(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Create(ctx, f.user, newUser(f.md, "users/ada", "ada@x.com", 1, nil)); err != nil {
		t.Fatal(err)
	}
	_, err := f.db.Graph.Connect(ctx,
		f.member,
		store.Ref{Resource: "User", Key: "users/ada"},
		store.Ref{Resource: "Org", Key: "orgs/ghost"},
		nil)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Connect to a missing endpoint = %v, want ErrNotFound", err)
	}
}

// The reason to hold this data in a graph store: a multi-hop question is one
// round trip rather than a fan-out per level.
func TestTraverseWalksMultipleHops(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	// a -> b -> c -> d
	ids := []string{"users/a", "users/b", "users/c", "users/d"}
	for _, id := range ids {
		if _, err := f.db.Create(ctx, f.user, newUser(f.md, id, id+"@x.com", 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	ref := func(id string) store.Ref { return store.Ref{Resource: "User", Key: id} }
	for i := range 3 {
		if _, err := f.db.Graph.Connect(ctx, f.member, ref(ids[i]), ref(ids[i+1]), nil); err != nil {
			t.Fatal(err)
		}
	}

	paths, err := f.db.Graph.Traverse(ctx, ref("users/a"), store.TraverseOptions{
		Types: []string{"MemberOf"}, MaxDepth: 3,
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	if len(paths) != 3 {
		t.Fatalf("got %d paths, want 3 (b, c, d)", len(paths))
	}

	deepest := paths[len(paths)-1]
	if deepest.Depth() != 3 {
		t.Errorf("deepest path has %d hops, want 3", deepest.Depth())
	}
	end, moved := deepest.End()
	if !moved || end.Key != "users/d" {
		t.Errorf("deepest path ends at %v, want users/d", end)
	}
	// Vertices always carry one more entry than edges, starting where the walk
	// began.
	if len(deepest.Vertices) != len(deepest.Edges)+1 {
		t.Errorf("path has %d vertices and %d edges", len(deepest.Vertices), len(deepest.Edges))
	}
	if deepest.Vertices[0].Key != "users/a" {
		t.Errorf("path starts at %v, want users/a", deepest.Vertices[0])
	}

	// A shallower bound stops earlier.
	shallow, err := f.db.Graph.Traverse(ctx, ref("users/a"), store.TraverseOptions{
		Types: []string{"MemberOf"}, MaxDepth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(shallow) != 1 {
		t.Errorf("one hop returned %d paths, want 1", len(shallow))
	}
}

// An unbounded walk on a connected graph visits everything, so the contract
// requires a bound rather than defaulting one.
func TestTraverseRequiresADepth(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	_, err := f.db.Graph.Traverse(ctx, store.Ref{Resource: "User", Key: "users/a"},
		store.TraverseOptions{Types: []string{"MemberOf"}})
	if err == nil {
		t.Fatal("an unbounded traversal was accepted")
	}
	if !strings.Contains(err.Error(), "MaxDepth") {
		t.Errorf("the error does not say what is missing: %v", err)
	}
}

func TestEdgePropertiesRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	for _, id := range []string{"users/ada"} {
		if _, err := f.db.Create(ctx, f.user, newUser(f.md, id, id+"@x.com", 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.Create(ctx, f.org, newUser(f.md, "orgs/acme", "acme@x.com", 0, nil)); err != nil {
		t.Fatal(err)
	}

	props := dynamicpb.NewMessage(f.edgeMD)
	props.ProtoReflect().Set(f.edgeMD.Fields().ByName("role"), protoreflect.ValueOfString("admin"))

	ada := store.Ref{Resource: "User", Key: "users/ada"}
	acme := store.Ref{Resource: "Org", Key: "orgs/acme"}
	if _, err := f.db.Graph.Connect(ctx, f.member, ada, acme, props); err != nil {
		t.Fatalf("Connect with props: %v", err)
	}

	edges, err := f.db.Graph.Neighbors(ctx, ada, store.TraverseOptions{
		Types: []string{"MemberOf"}, WithProps: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 {
		t.Fatalf("got %d edges", len(edges))
	}
	if edges[0].Props == nil {
		t.Fatal("WithProps did not load the edge fields")
	}
	role := edges[0].Props.ProtoReflect().Get(f.edgeMD.Fields().ByName("role")).String()
	if role != "admin" {
		t.Errorf("role = %q, want admin", role)
	}

	// And without asking, the fields are not paid for.
	plain, err := f.db.Graph.Neighbors(ctx, ada, store.TraverseOptions{Types: []string{"MemberOf"}})
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].Props != nil {
		t.Error("edge fields were loaded without WithProps")
	}
}

// A record and the edges that join it, atomically — the reason Tx.Run hands
// over a whole DB rather than only the CRUD half.
func TestGraphInsideATransaction(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	if _, err := f.db.Create(ctx, f.org, newUser(f.md, "orgs/acme", "acme@x.com", 0, nil)); err != nil {
		t.Fatal(err)
	}
	acme := store.Ref{Resource: "Org", Key: "orgs/acme"}

	err := f.db.Tx.Run(ctx, func(tx *store.DB) error {
		if _, cerr := tx.Create(ctx, f.user, newUser(f.md, "users/new", "new@x.com", 1, nil)); cerr != nil {
			return cerr
		}
		_, cerr := tx.Graph.Connect(ctx, f.member,
			store.Ref{Resource: "User", Key: "users/new"}, acme, nil)
		return cerr
	})
	if err != nil {
		t.Fatalf("record and edge in one transaction: %v", err)
	}

	edges, err := f.db.Graph.Neighbors(ctx, store.Ref{Resource: "User", Key: "users/new"},
		store.TraverseOptions{Types: []string{"MemberOf"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 {
		t.Errorf("the committed edge is not there: got %d", len(edges))
	}

	// And a rollback takes both halves with it.
	boom := errors.New("no")
	_ = f.db.Tx.Run(ctx, func(tx *store.DB) error {
		if _, cerr := tx.Create(ctx, f.user, newUser(f.md, "users/gone", "gone@x.com", 1, nil)); cerr != nil {
			return cerr
		}
		if _, cerr := tx.Graph.Connect(ctx, f.member,
			store.Ref{Resource: "User", Key: "users/gone"}, acme, nil); cerr != nil {
			return cerr
		}
		return boom
	})
	if ok, _ := f.db.Exists(ctx, f.user, "users/gone"); ok {
		t.Error("a rolled-back transaction left its record behind")
	}
}

func TestGraphMigrator(t *testing.T) {
	ctx := context.Background()
	f := setup(t)

	gm, ok := f.db.Driver.(store.GraphMigrator)
	if !ok {
		t.Fatal("the arangodb driver does not implement GraphMigrator")
	}
	defs := []store.EdgeDefinition{{
		Edge: "MemberOf", From: []string{"User"}, To: []string{"Org"},
	}}
	if err := gm.EnsureGraph(ctx, "org_chart", defs); err != nil {
		t.Fatalf("EnsureGraph: %v", err)
	}
	// Safe to call repeatedly, because it runs on startup.
	if err := gm.EnsureGraph(ctx, "org_chart", defs); err != nil {
		t.Fatalf("EnsureGraph twice: %v", err)
	}
	if err := gm.DropGraph(ctx, "org_chart"); err != nil {
		t.Fatalf("DropGraph: %v", err)
	}
	// Dropping one that is gone is not an error.
	if err := gm.DropGraph(ctx, "org_chart"); err != nil {
		t.Errorf("DropGraph of a missing graph: %v", err)
	}
}

// Without a registry a Ref cannot be turned into a collection, and the error
// says so rather than failing somewhere deeper.
func TestGraphNeedsARegistry(t *testing.T) {
	ctx := context.Background()
	addr := strings.TrimPrefix(endpoint(), "http://")
	dctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		t.Skipf("no ArangoDB at %s: %v", addr, err)
	}
	_ = conn.Close()

	client, err := arangodb.NewClient(ctx, arangodb.Config{
		Endpoints: []string{endpoint()}, Username: "root", Password: "root",
	})
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = client.Close() }()

	p := arangodb.NewProvider(client) // no registry
	name := fmt.Sprintf("itest_noreg_%d_%d", os.Getpid(), seq.Add(1))
	if eerr := p.EnsureDatabase(ctx, name); eerr != nil {
		t.Fatal(eerr)
	}
	defer func() { _ = p.DropDatabase(context.Background(), name) }()

	db, err := p.SetDatabase(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Graph.Neighbors(ctx, store.Ref{Resource: "User", Key: "x"},
		store.TraverseOptions{Types: []string{"MemberOf"}, MaxDepth: 1})
	if err == nil {
		t.Fatal("a graph walk without a registry was accepted")
	}
	if !strings.Contains(err.Error(), "registry") {
		t.Errorf("the error does not say what is missing: %v", err)
	}
}

func TestSetDatabaseRequiresAnExistingDatabase(t *testing.T) {
	ctx := context.Background()
	addr := strings.TrimPrefix(endpoint(), "http://")
	dctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		t.Skipf("no ArangoDB at %s: %v", addr, err)
	}
	_ = conn.Close()

	client, err := arangodb.NewClient(ctx, arangodb.Config{
		Endpoints: []string{endpoint()}, Username: "root", Password: "root",
	})
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = client.Close() }()

	_, err = arangodb.NewProvider(client).SetDatabase(ctx, "itest_does_not_exist_ever")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("SetDatabase on a missing database = %v, want ErrNotFound", err)
	}
}
