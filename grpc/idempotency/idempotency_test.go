package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// memStore is a Store in memory: names are unique, as a primary key makes
// them.
type memStore struct {
	mu      sync.Mutex
	keys    map[string]Key
	now     func() time.Time
	failGet error
	failNew error
}

func newMemStore(now func() time.Time) *memStore { return &memStore{keys: map[string]Key{}, now: now} }

func (m *memStore) Create(_ context.Context, k Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNew != nil {
		return m.failNew
	}
	if _, ok := m.keys[k.Name]; ok {
		return errors.New("duplicate key")
	}
	k.CreateTime = m.now()
	m.keys[k.Name] = k
	return nil
}

func (m *memStore) Get(_ context.Context, name string) (Key, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failGet != nil {
		return Key{}, m.failGet
	}
	k, ok := m.keys[name]
	if !ok {
		return Key{}, ErrNotFound
	}
	return k, nil
}

func (m *memStore) Settle(_ context.Context, k Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.keys[k.Name]
	cur.State, cur.Response = k.State, k.Response
	m.keys[k.Name] = cur
	return nil
}

func (m *memStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keys, name)
	return nil
}

// requestType is a request message with request_id and validate_only.
func requestType(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("test/v1/req.proto"), Package: proto.String("test.v1"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("CreateThingRequest"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("request_id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: opt},
				{Name: proto.String("validate_only"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(), Label: opt},
			},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return fd.Messages().Get(0)
}

func request(md protoreflect.MessageDescriptor, id string, validateOnly bool) proto.Message {
	m := dynamicpb.NewMessage(md)
	m.Set(md.Fields().ByName("request_id"), protoreflect.ValueOfString(id))
	m.Set(md.Fields().ByName("validate_only"), protoreflect.ValueOfBool(validateOnly))
	return m
}

var info = &grpc.UnaryServerInfo{FullMethod: "/test.v1.Things/CreateThing"}

func reason(err error) string {
	for _, d := range status.Convert(err).Details() {
		if i, ok := d.(*errdetails.ErrorInfo); ok {
			return i.GetReason()
		}
	}
	return ""
}

func TestReplayReturnsTheFirstResponse(t *testing.T) {
	md := requestType(t)
	store := newMemStore(time.Now)
	ic := Interceptor(store, Options{Domain: "test"})
	var runs atomic.Int32
	handler := func(context.Context, any) (any, error) {
		return wrapperspb.String(fmt.Sprintf("thing-%d", runs.Add(1))), nil
	}
	first, err := ic(context.Background(), request(md, "r1", false), info, handler)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ic(context.Background(), request(md, "r1", false), info, handler)
	if err != nil {
		t.Fatal(err)
	}
	if runs.Load() != 1 || !proto.Equal(first.(proto.Message), again.(proto.Message)) {
		t.Fatalf("replay ran the handler again (%d runs) or answered %v, want %v", runs.Load(), again, first)
	}
	// Another id, or the same id on another method, runs.
	if _, err := ic(context.Background(), request(md, "r2", false), info, handler); err != nil || runs.Load() != 2 {
		t.Fatalf("a new id did not run: %v", err)
	}
	other := &grpc.UnaryServerInfo{FullMethod: "/test.v1.Things/OtherThing"}
	if _, err := ic(context.Background(), request(md, "r1", false), other, handler); err != nil || runs.Load() != 3 {
		t.Fatalf("an id on another method did not run: %v", err)
	}
}

func TestOptOutsAreUntouched(t *testing.T) {
	md := requestType(t)
	store := newMemStore(time.Now)
	ic := Interceptor(store, Options{})
	handler := func(context.Context, any) (any, error) { return wrapperspb.String("x"), nil }
	for _, req := range []any{request(md, "", false), request(md, "r1", true), "not a proto", wrapperspb.String("no field")} {
		if _, err := ic(context.Background(), req, info, handler); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.keys) != 0 {
		t.Fatalf("recorded %d keys for requests that opt out", len(store.keys))
	}
}

func TestHandlerErrorReleasesTheID(t *testing.T) {
	md := requestType(t)
	store := newMemStore(time.Now)
	ic := Interceptor(store, Options{})
	boom := status.Error(codes.FailedPrecondition, "no")
	if _, err := ic(context.Background(), request(md, "r1", false), info, func(context.Context, any) (any, error) { return nil, boom }); err != boom { //nolint:errorlint // identity
		t.Fatalf("err = %v", err)
	}
	if len(store.keys) != 0 {
		t.Fatal("a failed call kept its key")
	}
	if _, err := ic(context.Background(), request(md, "r1", false), info, func(context.Context, any) (any, error) { return wrapperspb.String("ok"), nil }); err != nil {
		t.Fatalf("the retry after a failure: %v", err)
	}
}

func TestInFlightAndAbandoned(t *testing.T) {
	md := requestType(t)
	now := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	store := newMemStore(clock)
	ic := Interceptor(store, Options{Domain: "test", Now: clock})

	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error)
	go func() {
		_, err := ic(context.Background(), request(md, "r1", false), info, func(context.Context, any) (any, error) {
			close(started)
			<-release
			return wrapperspb.String("first"), nil
		})
		done <- err
	}()
	<-started
	_, err := ic(context.Background(), request(md, "r1", false), info, func(context.Context, any) (any, error) {
		t.Error("a concurrent duplicate ran")
		return wrapperspb.String("second"), nil
	})
	if status.Code(err) != codes.Aborted || reason(err) != ReasonInFlight {
		t.Fatalf("concurrent duplicate: %v (%s)", err, reason(err))
	}
	close(release)
	if ferr := <-done; ferr != nil {
		t.Fatal(ferr)
	}

	// A claim left IN_FLIGHT past StaleAfter is cleared, and the retry runs.
	name := KeyName(info.FullMethod, "r2")
	store.keys[name] = Key{Name: name, State: InFlight, CreateTime: now.Add(-11 * time.Minute)}
	_, err = ic(context.Background(), request(md, "r2", false), info, nil)
	if status.Code(err) != codes.Aborted || reason(err) != ReasonAbandoned {
		t.Fatalf("abandoned: %v (%s)", err, reason(err))
	}
	if _, ok := store.keys[name]; ok {
		t.Fatal("an abandoned claim was kept")
	}
	if _, err := ic(context.Background(), request(md, "r2", false), info, func(context.Context, any) (any, error) { return wrapperspb.String("ok"), nil }); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFaults(t *testing.T) {
	md := requestType(t)
	store := newMemStore(time.Now)
	store.failNew = errors.New("connection refused")
	sentinel := errors.New("mapped")
	ic := Interceptor(store, Options{CreateError: func(err error) error { return errors.Join(sentinel, err) }})
	if _, err := ic(context.Background(), request(md, "r1", false), info, nil); !errors.Is(err, sentinel) {
		t.Fatalf("a claim no key explains: %v", err)
	}
	store.failNew, store.failGet = errors.New("dup"), errors.New("read failed")
	if _, err := ic(context.Background(), request(md, "r1", false), info, nil); status.Code(err) != codes.Internal {
		t.Fatalf("an unreadable key: %v", err)
	}
}

func TestKeyName(t *testing.T) {
	a := KeyName("/m/A", "r")
	if len(a) != len("idempotencyKeys/")+64 || a == KeyName("/m/B", "r") || a != KeyName("/m/A", "r") {
		t.Fatalf("KeyName = %q", a)
	}
}
