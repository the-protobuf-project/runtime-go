package d1_test

// These tests run the driver against a stand-in for D1's REST query endpoint
// backed by a real SQLite, so the column affinity the driver leans on — text
// bound into an INTEGER column coming back as a number — is SQLite's own
// behavior rather than the fake's idea of it.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/option"
	_ "github.com/glebarez/go-sqlite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/the-protobuf-project/runtime-go/database/d1"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

const (
	account = "acct"
	mainID  = "11111111-1111-1111-1111-111111111111"
)

// fakeD1 serves the slice of the D1 REST API the driver uses, one SQLite
// database per id.
type fakeD1 struct {
	mu    sync.Mutex
	names map[string]string // name -> id
	dbs   map[string]*sql.DB
}

func newFake(t *testing.T, names map[string]string) *httptest.Server {
	t.Helper()
	f := &fakeD1{names: names, dbs: map[string]*sql.DB{}}
	for _, id := range names {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1) // one connection, or every one gets its own :memory:
		f.dbs[id] = db
		t.Cleanup(func() { _ = db.Close() })
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeD1) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	prefix := "/accounts/" + account + "/d1/database"
	path := strings.TrimPrefix(r.URL.Path, prefix)
	switch {
	case r.Method == http.MethodGet && path == "":
		var list []map[string]string
		for name, id := range f.names {
			if strings.Contains(name, r.URL.Query().Get("name")) {
				list = append(list, map[string]string{"name": name, "uuid": id})
			}
		}
		reply(w, http.StatusOK, map[string]any{
			"success": true, "result": list,
			"result_info": map[string]int{"page": 1, "per_page": 100, "count": len(list), "total_count": len(list)},
		})
	case r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/")
		if _, ok := f.dbs[id]; !ok {
			fail(w, http.StatusNotFound, 7404, "database not found")
			return
		}
		reply(w, http.StatusOK, map[string]any{"success": true, "result": map[string]string{"uuid": id}})
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/query"):
		db, ok := f.dbs[strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/query")]
		if !ok {
			fail(w, http.StatusNotFound, 7404, "database not found")
			return
		}
		f.query(w, r, db)
	default:
		fail(w, http.StatusNotFound, 7000, "no route for "+r.Method+" "+r.URL.Path)
	}
}

type stmt struct {
	SQL    string   `json:"sql"`
	Params []string `json:"params"`
}

// query runs a single statement or a batch; a batch is one transaction.
func (f *fakeD1) query(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	var body struct {
		stmt
		Batch []stmt `json:"batch"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		fail(w, http.StatusBadRequest, 7400, err.Error())
		return
	}
	stmts := body.Batch
	if stmts == nil {
		stmts = []stmt{body.stmt}
	}

	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		fail(w, http.StatusInternalServerError, 7500, err.Error())
		return
	}
	var results []map[string]any
	for _, s := range stmts {
		res, err := run(r.Context(), tx, s)
		if err != nil {
			_ = tx.Rollback()
			fail(w, http.StatusBadRequest, 7500, err.Error()+": SQLITE_CONSTRAINT")
			return
		}
		results = append(results, res)
	}
	if err := tx.Commit(); err != nil {
		fail(w, http.StatusInternalServerError, 7500, err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{"success": true, "result": results})
}

func run(ctx context.Context, tx *sql.Tx, s stmt) (map[string]any, error) {
	args := make([]any, len(s.Params))
	for i, p := range s.Params {
		args[i] = p
	}
	upper := strings.ToUpper(strings.TrimSpace(s.SQL))
	if strings.HasPrefix(upper, "SELECT") {
		rows, err := tx.QueryContext(ctx, s.SQL, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		out := []map[string]any{}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, err
			}
			row := map[string]any{}
			for i, c := range cols {
				if b, ok := vals[i].([]byte); ok {
					vals[i] = string(b)
				}
				row[c] = vals[i]
			}
			out = append(out, row)
		}
		return map[string]any{"success": true, "results": out, "meta": map[string]any{}}, rows.Err()
	}
	res, err := tx.ExecContext(ctx, s.SQL, args...)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	return map[string]any{"success": true, "results": []any{}, "meta": map[string]any{"changes": n}}, nil
}

func reply(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func fail(w http.ResponseWriter, status, code int, msg string) {
	reply(w, status, map[string]any{
		"success": false, "result": nil, "messages": []any{},
		"errors": []map[string]any{{"code": code, "message": msg}},
	})
}

func client(srv *httptest.Server) *cloudflare.Client {
	return cloudflare.NewClient(
		option.WithBaseURL(srv.URL+"/"),
		option.WithAPIToken("test"),
		option.WithMaxRetries(0),
	)
}

// wrap is a D1 client over the stand-in, driving databaseID by default.
func wrap(t *testing.T, srv *httptest.Server, databaseID string) *d1.Client {
	t.Helper()
	c, err := d1.Wrap(client(srv), d1.Config{AccountID: account, DatabaseID: databaseID})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The client alone: SQL in, rows out, no descriptor and no proto anywhere.
func TestClientRunsSQLWithoutProto(t *testing.T) {
	srv := newFake(t, map[string]string{"main": mainID})
	c := wrap(t, srv, mainID)
	ctx := t.Context()

	if _, err := c.Query(ctx, mainID, d1.Statement{SQL: "CREATE TABLE notes (id TEXT PRIMARY KEY, words INTEGER)"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	put, err := c.Batch(ctx, mainID,
		d1.Statement{SQL: "INSERT INTO notes VALUES (?, ?)", Args: []string{"a", "3"}},
		d1.Statement{SQL: "INSERT INTO notes VALUES (?, ?)", Args: []string{"b", "10"}},
	)
	if err != nil || len(put) != 2 || put[0].Changes != 1 {
		t.Fatalf("batch: %v, %+v", err, put)
	}
	got, err := c.Query(ctx, mainID, d1.Statement{SQL: "SELECT id, words FROM notes ORDER BY words DESC"})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	// Bound as text, stored as numbers: 10 sorts above 3.
	if len(got.Rows) != 2 || got.Rows[0]["id"] != "b" || got.Rows[0]["words"].(json.Number).String() != "10" {
		t.Fatalf("rows: %+v", got.Rows)
	}
	_, err = c.Query(ctx, mainID, d1.Statement{SQL: "INSERT INTO notes VALUES (?, ?)", Args: []string{"a", "1"}})
	if !errors.Is(err, d1.ErrUnique) {
		t.Fatalf("duplicate: got %v, want ErrUnique", err)
	}
	if err := c.Ping(ctx, "nope"); err == nil {
		t.Fatal("Ping reached a database that is not there")
	}
	if _, err := d1.NewClient(d1.Config{AccountID: account}); err == nil {
		t.Fatal("NewClient accepted a config with no API token")
	}
}

// setup returns the configured database over a fresh fake, with the user table
// created.
func setup(t *testing.T) (*store.DB, *store.Resource, protoreflect.MessageDescriptor) {
	t.Helper()
	srv := newFake(t, map[string]string{"main": mainID, "tenant_a": "22222222-2222-2222-2222-222222222222"})
	p := d1.NewProvider(wrap(t, srv, mainID))
	db, err := p.SetDatabase(t.Context(), "")
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	md := userMD(t)
	res := userRes(md)
	if err := db.Schema.EnsureSchema(t.Context(), res); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return db, res, md
}

func userMD(t testing.TB) protoreflect.MessageDescriptor {
	t.Helper()
	_ = timestamppb.Now() // registers google/protobuf/timestamp.proto
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	field := func(name string, n int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(n), Type: typ.Enum(), Label: opt}
	}
	born := field("born", 7, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	born.TypeName = proto.String(".google.protobuf.Timestamp")
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("itest/v1/d1_test.proto"),
		Package:    proto.String("itest.d1.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("User"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
				field("email", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING),
				field("avatar", 3, descriptorpb.FieldDescriptorProto_TYPE_BYTES),
				field("credits", 4, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
				field("active", 5, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
				field("score", 6, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE),
				born,
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
		Name:     "User",
		Table:    "users",
		PKColumn: "id",
		New:      func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "email", Field: "email", Kind: store.KindString, Unique: true},
			{Name: "avatar", Field: "avatar", Kind: store.KindBytes},
			{Name: "credits", Field: "credits", Kind: store.KindUint},
			{Name: "active", Field: "active", Kind: store.KindBool},
			{Name: "score", Field: "score", Kind: store.KindFloat},
			{Name: "born", Field: "born", Kind: store.KindTimestamp},
		},
	}
}

func newUser(md protoreflect.MessageDescriptor, id, email string, credits uint64, born time.Time) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("email"), protoreflect.ValueOfString(email))
	m.Set(f.ByName("avatar"), protoreflect.ValueOfBytes([]byte{0, 1, 2, 0xff}))
	m.Set(f.ByName("credits"), protoreflect.ValueOfUint64(credits))
	m.Set(f.ByName("active"), protoreflect.ValueOfBool(credits%2 == 0))
	m.Set(f.ByName("score"), protoreflect.ValueOfFloat64(float64(credits)/4))
	m.Set(f.ByName("born"), protoreflect.ValueOfMessage(timestamppb.New(born).ProtoReflect()))
	return msg
}

// batcher is the bulk capability, which a DB carries on its driver.
func batcher(t *testing.T, db *store.DB) store.Batcher {
	t.Helper()
	b, ok := db.Driver.(store.Batcher)
	if !ok {
		t.Fatal("d1 lost its Batcher")
	}
	return b
}

func TestRoundTripKeepsEveryKind(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	born := time.Date(1990, 5, 17, 8, 30, 0, 123456789, time.UTC)
	want := newUser(md, "u1", "a@example.com", 42, born)

	if _, err := db.Create(ctx, res, want); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := db.Get(ctx, res, "u1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("round trip changed the record:\n got %v\nwant %v", got, want)
	}
}

func TestDuplicatesAreConflicts(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	if _, err := db.Create(ctx, res, newUser(md, "u1", "a@example.com", 1, time.Now())); err != nil {
		t.Fatal(err)
	}
	for name, dup := range map[string]proto.Message{
		"key":    newUser(md, "u1", "b@example.com", 1, time.Now()),
		"unique": newUser(md, "u2", "a@example.com", 1, time.Now()),
	} {
		if _, err := db.Create(ctx, res, dup); !errors.Is(err, store.ErrAlreadyExists) {
			t.Errorf("duplicate %s: got %v, want ErrAlreadyExists", name, err)
		}
	}
}

func TestMissingRecordsAreNotFound(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	if _, err := db.Get(ctx, res, "nobody"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get: got %v", err)
	}
	if _, err := db.Update(ctx, res, newUser(md, "nobody", "x@example.com", 1, time.Now())); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Update: got %v", err)
	}
	if err := db.Delete(ctx, res, "nobody"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Delete: got %v", err)
	}
	if ok, err := db.Exists(ctx, res, "nobody"); err != nil || ok {
		t.Errorf("Exists: got %v, %v", ok, err)
	}
}

func TestUpdateAndDelete(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	if _, err := db.Create(ctx, res, newUser(md, "u1", "a@example.com", 1, time.Now())); err != nil {
		t.Fatal(err)
	}
	changed := newUser(md, "u1", "new@example.com", 7, time.Now())
	if _, err := db.Update(ctx, res, changed); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := db.Get(ctx, res, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, changed) {
		t.Fatalf("update not applied: %v", got)
	}
	if err := db.Delete(ctx, res, "u1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := db.Exists(ctx, res, "u1"); ok {
		t.Fatal("record survived its delete")
	}
}

// Filters bind text like every write; affinity must still make them numeric
// and timestamps must still compare as times.
func TestListFiltersOrdersAndPages(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	msgs := make([]proto.Message, 0, 12)
	for i := range 12 {
		// credits 1..12: 9 sorts after 10 as text and before it as a number.
		msgs = append(msgs, newUser(md, fmt.Sprintf("u%02d", i), fmt.Sprintf("%d@example.com", i), uint64(i+1), base.Add(time.Duration(i)*time.Hour)))
	}
	if _, err := batcher(t, db).CreateMany(ctx, res, msgs); err != nil {
		t.Fatalf("CreateMany: %v", err)
	}

	page, err := db.List(ctx, res, store.ListOptions{Filter: "credits >= 9", OrderBy: "credits desc", PageSize: 3})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if page.Total != 4 || len(page.Items) != 3 || page.NextPageToken == "" {
		t.Fatalf("got total %d, %d items, token %q", page.Total, len(page.Items), page.NextPageToken)
	}
	first := page.Items[0].ProtoReflect().Get(md.Fields().ByName("credits")).Uint()
	if first != 12 {
		t.Fatalf("numeric ordering: first credits %d, want 12", first)
	}
	rest, err := db.List(ctx, res, store.ListOptions{Filter: "credits >= 9", OrderBy: "credits desc", PageSize: 3, PageToken: page.NextPageToken})
	if err != nil || len(rest.Items) != 1 || rest.NextPageToken != "" {
		t.Fatalf("second page: %v items, token %q, err %v", len(rest.Items), rest.NextPageToken, err)
	}

	n, err := db.Count(ctx, res, store.ListOptions{Filter: "born > 2020-01-01T05:30:00Z AND active = true"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	// hours 6..11, of which active (even credits, so odd i) are 7, 9, 11.
	if n != 3 {
		t.Fatalf("Count = %d, want 3", n)
	}
	if _, err := db.Count(ctx, res, store.ListOptions{Filter: "password = x"}); err == nil {
		t.Fatal("a filter on an unknown column was accepted")
	}
}

func TestGetManyKeepsOrderAndGaps(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	keys := make([]string, 0, 150)
	msgs := make([]proto.Message, 0, 120)
	for i := range 120 {
		id := fmt.Sprintf("u%03d", i)
		msgs = append(msgs, newUser(md, id, id+"@example.com", uint64(i), time.Now()))
	}
	if _, err := batcher(t, db).CreateMany(ctx, res, msgs); err != nil {
		t.Fatal(err)
	}
	for i := 149; i >= 0; i-- { // past the bind limit, reversed, with gaps
		keys = append(keys, fmt.Sprintf("u%03d", i))
	}
	got, err := batcher(t, db).GetMany(ctx, res, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	for i, key := range keys {
		var want string
		if key < "u120" {
			want = key
		}
		var have string
		if got[i] != nil {
			have = got[i].ProtoReflect().Get(md.Fields().ByName("id")).String()
		}
		if have != want {
			t.Fatalf("GetMany[%d] (%s) = %q, want %q", i, key, have, want)
		}
	}
}

func TestBatchIsOneUnit(t *testing.T) {
	db, res, md := setup(t)
	ctx := t.Context()
	msgs := []proto.Message{
		newUser(md, "a", "same@example.com", 1, time.Now()),
		newUser(md, "b", "same@example.com", 2, time.Now()),
	}
	if _, err := batcher(t, db).CreateMany(ctx, res, msgs); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("got %v, want ErrAlreadyExists", err)
	}
	if ok, _ := db.Exists(ctx, res, "a"); ok {
		t.Fatal("half a failed batch landed")
	}
}

func TestSchemaAndCapabilities(t *testing.T) {
	db, res, _ := setup(t)
	ctx := t.Context()
	if ok, err := db.Schema.HasSchema(ctx, res); err != nil || !ok {
		t.Fatalf("HasSchema after Ensure: %v, %v", ok, err)
	}
	if err := db.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatalf("EnsureSchema is not repeatable: %v", err)
	}
	if err := db.Tx.Run(ctx, func(*store.DB) error { return nil }); !errors.Is(err, store.ErrUnimplemented) {
		t.Fatalf("Tx.Run: got %v, want ErrUnimplemented", err)
	}
	if err := db.Schema.DropSchema(ctx, res); err != nil {
		t.Fatal(err)
	}
	if ok, _ := db.Schema.HasSchema(ctx, res); ok {
		t.Fatal("table survived DropSchema")
	}
}

func TestSetDatabaseByName(t *testing.T) {
	srv := newFake(t, map[string]string{"tenant_a": "22222222-2222-2222-2222-222222222222", "tenant_ab": "33333333-3333-3333-3333-333333333333"})
	p := d1.NewProvider(wrap(t, srv, ""))
	ctx := context.Background()

	md := userMD(t)
	res := userRes(md)
	a, err := p.SetDatabase(ctx, "tenant_a")
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	if eerr := a.Schema.EnsureSchema(ctx, res); eerr != nil {
		t.Fatal(eerr)
	}
	ab, err := p.SetDatabase(ctx, "tenant_ab")
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := ab.Schema.HasSchema(ctx, res); ok {
		t.Fatal("tenant_ab sees tenant_a's table: the name matched loosely")
	}
	if _, err := p.SetDatabase(ctx, "tenant_b"); err == nil || !strings.Contains(err.Error(), "wrangler d1 create") {
		t.Fatalf("unknown name: got %v", err)
	}
	if _, err := p.SetDatabase(ctx, ""); !errors.Is(err, store.ErrBadConfig) {
		t.Fatalf("empty name without a DatabaseID: got %v", err)
	}
	if _, err := p.SetDatabase(ctx, "bad name;"); err == nil {
		t.Fatal("an unsafe name was accepted")
	}
}

func TestRegistered(t *testing.T) {
	if _, err := store.NewDriver(store.D1, "nope"); !errors.Is(err, store.ErrBadConfig) {
		t.Fatalf("got %v", err)
	}
	srv := newFake(t, map[string]string{"main": mainID})
	if _, err := store.NewDriver(store.D1, wrap(t, srv, mainID)); err != nil {
		t.Fatal(err)
	}
}
