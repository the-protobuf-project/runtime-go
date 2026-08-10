package cached_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/the-protobuf-project/runtime-go/database/cached"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// ---------------------------------------------------------------- descriptors

func bookMD(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("cached/v1/cached_test.proto"),
		Package: proto.String("cached.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Book"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("title"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("cover"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

func bookRes(md protoreflect.MessageDescriptor) *store.Resource {
	return &store.Resource{
		Name: "Book", Table: "books", PKColumn: "id",
		New: func() proto.Message { return dynamicpb.NewMessage(md) },
		Columns: []store.Column{
			{Name: "id", Field: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "title", Field: "title", Kind: store.KindString},
			{Name: "cover", Field: "cover", Kind: store.KindBytes},
		},
	}
}

func newBook(md protoreflect.MessageDescriptor, id, title string, cover []byte) proto.Message {
	msg := dynamicpb.NewMessage(md)
	m := msg.ProtoReflect()
	f := md.Fields()
	m.Set(f.ByName("id"), protoreflect.ValueOfString(id))
	m.Set(f.ByName("title"), protoreflect.ValueOfString(title))
	m.Set(f.ByName("cover"), protoreflect.ValueOfBytes(cover))
	return msg
}

func title(msg proto.Message, md protoreflect.MessageDescriptor) string {
	return msg.ProtoReflect().Get(md.Fields().ByName("title")).String()
}

// ---------------------------------------------------------------- a fake store

// fakeStore is an in-memory store.Driver that counts what is asked of it, so a
// test can assert how many times the backing store was reached rather than only
// that the right value came back. That count is the entire point of a cache.
type fakeStore struct {
	mu   sync.Mutex
	recs map[string][]byte
	res  *store.Resource

	gets    atomic.Int64
	hold    chan struct{} // when non-nil, Get blocks on it
	arrived chan struct{} // closed once, when the first Get arrives
	once    sync.Once
}

func newFakeStore(res *store.Resource) *fakeStore {
	return &fakeStore{recs: map[string][]byte{}, res: res}
}

func (f *fakeStore) Create(_ context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	key, err := store.KeyOf(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.recs[key]; ok {
		return store.WriteResult{}, store.ErrAlreadyExists
	}
	f.recs[key] = body
	return store.WriteResult{Message: msg}, nil
}

func (f *fakeStore) Get(_ context.Context, res *store.Resource, key string) (proto.Message, error) {
	f.gets.Add(1)
	if f.arrived != nil {
		f.once.Do(func() { close(f.arrived) })
	}
	if f.hold != nil {
		<-f.hold
	}
	f.mu.Lock()
	body, ok := f.recs[key]
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}
	msg := res.New()
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (f *fakeStore) Update(_ context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	key, err := store.KeyOf(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.recs[key]; !ok {
		return store.WriteResult{}, fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}
	f.recs[key] = body
	return store.WriteResult{Message: msg}, nil
}

func (f *fakeStore) Delete(_ context.Context, _ *store.Resource, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.recs[key]; !ok {
		return fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}
	delete(f.recs, key)
	return nil
}

func (f *fakeStore) List(context.Context, *store.Resource, store.ListOptions) (store.ListResult, error) {
	return store.ListResult{}, nil
}

func (f *fakeStore) Count(context.Context, *store.Resource, store.ListOptions) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.recs)), nil
}

func (f *fakeStore) Exists(_ context.Context, _ *store.Resource, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.recs[key]
	return ok, nil
}

// ---------------------------------------------------------------- a fake cache

// fakeCache is a minimal in-process read-through cache: enough to hold entries,
// remember an absence, and collapse concurrent loads. It exists so these tests
// assert the decorator's behavior rather than the cache module's, which has its
// own.
type fakeCache struct {
	mu       sync.Mutex
	entries  map[string]entry
	overload bool
}

type entry struct {
	body []byte
	void bool
}

func newFakeCache() *fakeCache { return &fakeCache{entries: map[string]entry{}} }

func (c *fakeCache) Aside(load func(context.Context, string) ([]byte, error)) cached.Aside {
	return &fakeAside{c: c, load: load, flight: map[string]*sync.WaitGroup{}}
}

type fakeAside struct {
	c    *fakeCache
	load func(context.Context, string) ([]byte, error)

	mu     sync.Mutex
	flight map[string]*sync.WaitGroup
}

func (a *fakeAside) GetOrLoad(ctx context.Context, key string, dest *[]byte) error {
	a.c.mu.Lock()
	if a.c.overload {
		a.c.mu.Unlock()
		return fmt.Errorf("%w: %s", cached.ErrOverloaded, key)
	}
	e, ok := a.c.entries[key]
	a.c.mu.Unlock()
	if ok {
		if e.void {
			return fmt.Errorf("%w: %s", store.ErrNotFound, key)
		}
		*dest = e.body
		return nil
	}

	// Collapse concurrent loads of one key, which is the property that makes a
	// cache worth putting in front of anything.
	a.mu.Lock()
	if wg, running := a.flight[key]; running {
		a.mu.Unlock()
		wg.Wait()
		return a.GetOrLoad(ctx, key, dest)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	a.flight[key] = wg
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		delete(a.flight, key)
		a.mu.Unlock()
		wg.Done()
	}()

	body, err := a.load(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			a.c.mu.Lock()
			a.c.entries[key] = entry{void: true}
			a.c.mu.Unlock()
		}
		return err
	}
	a.c.mu.Lock()
	a.c.entries[key] = entry{body: body}
	a.c.mu.Unlock()
	*dest = body
	return nil
}

func (a *fakeAside) Invalidate(_ context.Context, keys ...string) error {
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	for _, k := range keys {
		delete(a.c.entries, k)
	}
	return nil
}

func setup(t *testing.T) (*store.DB, *fakeStore, *fakeCache, *store.Resource, protoreflect.MessageDescriptor) {
	t.Helper()
	md := bookMD(t)
	res := bookRes(md)
	backing := newFakeStore(res)
	fc := newFakeCache()
	db := cached.Wrap(store.Build(backing, "fake", "test", nil), fc)
	return db, backing, fc, res, md
}

// ---------------------------------------------------------------------- tests

func TestASecondReadDoesNotReachTheStore(t *testing.T) {
	ctx := context.Background()
	db, backing, _, res, md := setup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/dune", "Dune", nil)); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := db.Get(ctx, res, "books/dune"); err != nil {
			t.Fatal(err)
		}
	}
	if n := backing.gets.Load(); n != 1 {
		t.Errorf("five reads reached the store %d times, want 1", n)
	}
}

// The bytes go through the cache as an encoded message. Both of these come back
// wrong through any JSON shape but a []byte, and both come back silently wrong.
func TestValuesSurviveTheCache(t *testing.T) {
	ctx := context.Background()
	db, _, _, res, md := setup(t)

	cover := []byte{0x00, 0xff, 0xfe, 0x80, 0x01}
	if _, err := db.Create(ctx, res, newBook(md, "books/x", "X", cover)); err != nil {
		t.Fatal(err)
	}
	// First read populates, second is served from the cache.
	if _, err := db.Get(ctx, res, "books/x"); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(ctx, res, "books/x")
	if err != nil {
		t.Fatal(err)
	}
	back := got.ProtoReflect().Get(md.Fields().ByName("cover")).Bytes()
	if !bytes.Equal(back, cover) {
		t.Errorf("cover = %v, want %v", back, cover)
	}
}

// The bug a read-through cache produces that looks exactly like a database
// losing a write: a Get for a key that is not there remembers the absence, and
// without invalidation the record created afterwards stays invisible.
func TestCreateClearsARememberedAbsence(t *testing.T) {
	ctx := context.Background()
	db, _, _, res, md := setup(t)

	if _, err := db.Get(ctx, res, "books/new"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("first Get = %v, want ErrNotFound", err)
	}
	if _, err := db.Create(ctx, res, newBook(md, "books/new", "New", nil)); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get(ctx, res, "books/new")
	if err != nil {
		t.Fatalf("the created record is invisible: %v", err)
	}
	if title(got, md) != "New" {
		t.Errorf("title = %q", title(got, md))
	}
}

func TestUpdateIsVisibleImmediately(t *testing.T) {
	ctx := context.Background()
	db, _, _, res, md := setup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/a", "First", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); err != nil { // populate
		t.Fatal(err)
	}
	if _, err := db.Update(ctx, res, newBook(md, "books/a", "Second", nil)); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get(ctx, res, "books/a")
	if err != nil {
		t.Fatal(err)
	}
	if title(got, md) != "Second" {
		t.Errorf("a stale record was served after an update: %q", title(got, md))
	}
}

func TestDeleteIsVisibleImmediately(t *testing.T) {
	ctx := context.Background()
	db, _, _, res, md := setup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/a", "A", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(ctx, res, "books/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a deleted record was still served: %v", err)
	}
}

// An absence has to be remembered, or a stream of requests for something that
// does not exist reaches the store forever.
func TestAbsenceIsRemembered(t *testing.T) {
	ctx := context.Background()
	db, backing, _, res, _ := setup(t)

	for range 10 {
		if _, err := db.Get(ctx, res, "books/ghost"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("Get = %v, want ErrNotFound", err)
		}
	}
	if n := backing.gets.Load(); n != 1 {
		t.Errorf("ten requests for a missing record reached the store %d times, want 1", n)
	}
}

// Concurrent misses on one key must become one load, which is the property that
// makes a cache worth putting in front of a database at all.
func TestConcurrentMissesCollapse(t *testing.T) {
	ctx := context.Background()
	db, backing, _, res, md := setup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/hot", "Hot", nil)); err != nil {
		t.Fatal(err)
	}
	backing.hold = make(chan struct{})
	backing.arrived = make(chan struct{})

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = db.Get(ctx, res, "books/hot")
		}()
	}
	// The first load arriving only means one caller got there. Hold it long
	// enough for the rest to reach the cache and find that load already
	// running — releasing on the first arrival measures how fast fifty
	// goroutines start, not whether the cache collapses them.
	<-backing.arrived
	time.Sleep(100 * time.Millisecond)
	close(backing.hold)
	wg.Wait()

	if n := backing.gets.Load(); n > 2 {
		t.Errorf("50 concurrent misses caused %d loads, want about 1", n)
	}
}

// A cache refusing new work must not fail the request: the load happens either
// way, and reading through directly runs it on the caller's own goroutine.
func TestAnOverloadedCacheFallsThrough(t *testing.T) {
	ctx := context.Background()
	db, backing, fc, res, md := setup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/a", "A", nil)); err != nil {
		t.Fatal(err)
	}
	fc.overload = true

	got, err := db.Get(ctx, res, "books/a")
	if err != nil {
		t.Fatalf("an overloaded cache failed the read: %v", err)
	}
	if title(got, md) != "A" {
		t.Errorf("title = %q", title(got, md))
	}
	if backing.gets.Load() == 0 {
		t.Error("the read did not reach the store")
	}
}

// Wrapping a database in a cache must not quietly cost it a capability.
func TestWrapKeepsCapabilities(t *testing.T) {
	md := bookMD(t)
	res := bookRes(md)
	backing := newFakeStore(res)

	inner := store.Build(backing, "fake", "test", nil)
	outer := cached.Wrap(inner, newFakeCache())

	if outer.Schema == nil || outer.Tx == nil {
		t.Fatal("capability fields must never be nil")
	}
	if outer.Name != inner.Name {
		t.Errorf("Name = %q, want %q", outer.Name, inner.Name)
	}
	// The fake store has neither capability, so both must still refuse by name
	// rather than pretend.
	err := outer.Tx.Run(context.Background(), func(*store.DB) error { return nil })
	if !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("Tx.Run = %v, want ErrUnimplemented", err)
	}
	if err := outer.Schema.EnsureSchema(context.Background(), res); !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("EnsureSchema = %v, want ErrUnimplemented", err)
	}
}

// A committed transaction must correct the cache for everything it wrote, or a
// write that went through a transaction is invisible in a way the same write
// outside one is not.
func TestCommittedTransactionInvalidates(t *testing.T) {
	ctx := context.Background()
	md := bookMD(t)
	res := bookRes(md)
	backing := newFakeStore(res)

	inner := store.Build(backing, "fake", "test", nil)
	inner.Tx = fakeTx{backing}
	db := cached.Wrap(inner, newFakeCache())

	if _, err := db.Create(ctx, res, newBook(md, "books/a", "Before", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); err != nil { // populate the cache
		t.Fatal(err)
	}

	err := db.Tx.Run(ctx, func(tx *store.DB) error {
		_, uerr := tx.Update(ctx, res, newBook(md, "books/a", "After", nil))
		return uerr
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := db.Get(ctx, res, "books/a")
	if err != nil {
		t.Fatal(err)
	}
	if title(got, md) != "After" {
		t.Errorf("a stale record survived a committed transaction: %q", title(got, md))
	}
}

// A rolled-back transaction wrote nothing, so the cache is still right and must
// be left alone.
func TestRolledBackTransactionKeepsTheCache(t *testing.T) {
	ctx := context.Background()
	md := bookMD(t)
	res := bookRes(md)
	backing := newFakeStore(res)

	inner := store.Build(backing, "fake", "test", nil)
	inner.Tx = fakeTx{backing}
	db := cached.Wrap(inner, newFakeCache())

	if _, err := db.Create(ctx, res, newBook(md, "books/a", "Before", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); err != nil {
		t.Fatal(err)
	}
	before := backing.gets.Load()

	boom := errors.New("rolled back")
	if err := db.Tx.Run(ctx, func(*store.DB) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the caller's error", err)
	}

	if _, err := db.Get(ctx, res, "books/a"); err != nil {
		t.Fatal(err)
	}
	if backing.gets.Load() != before {
		t.Error("a rolled-back transaction threw away a cache entry that was still correct")
	}
}

// fakeTx runs the body against the store directly — enough to exercise the
// decorator's invalidation, which is what is under test here rather than
// transaction semantics.
type fakeTx struct{ backing *fakeStore }

func (f fakeTx) Run(_ context.Context, fn func(*store.DB) error) error {
	return fn(store.Build(f.backing, "fake", "test", nil))
}
