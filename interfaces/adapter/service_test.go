package adapter_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/interfaces/adapter"
)

// fakeDriver returns canned results so the adapter's dispatch + error mapping can
// be tested without a real backend.
type fakeDriver struct {
	getMsg    proto.Message
	getErr    error
	createErr error
}

func (f *fakeDriver) Create(context.Context, *database.Resource, proto.Message) (database.WriteResult, error) {
	return database.WriteResult{}, f.createErr
}
func (f *fakeDriver) Get(context.Context, *database.Resource, string) (proto.Message, error) {
	return f.getMsg, f.getErr
}
func (f *fakeDriver) Update(context.Context, *database.Resource, proto.Message) (database.WriteResult, error) {
	return database.WriteResult{}, nil
}
func (f *fakeDriver) Delete(context.Context, *database.Resource, string) error { return nil }
func (f *fakeDriver) List(context.Context, *database.Resource, database.ListOptions) (database.ListResult, error) {
	return database.ListResult{}, nil
}
func (f *fakeDriver) Count(context.Context, *database.Resource, database.ListOptions) (int64, error) {
	return 0, nil
}
func (f *fakeDriver) Exists(context.Context, *database.Resource, string) (bool, error) {
	return false, nil
}

func registry() *database.Registry {
	return database.NewRegistry(database.Resource{
		Name: "Book", Table: "books", PKColumn: "id",
		New: func() proto.Message { return &wrapperspb.StringValue{} },
	})
}

func TestServiceErrorMapping(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not found", database.ErrNotFound, codes.NotFound},
		{"unimplemented", database.ErrUnimplemented, codes.Unimplemented},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := adapter.New(&fakeDriver{getErr: tc.err}, registry())
			_, err := svc.Get(ctx, "Book", "books/x")
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tc.want, err)
			}
		})
	}

	svc := adapter.New(&fakeDriver{createErr: database.ErrAlreadyExists}, registry())
	if _, err := svc.Create(ctx, "Book", &wrapperspb.StringValue{}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("Create code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestServiceUnregisteredResourceIsInternal(t *testing.T) {
	svc := adapter.New(&fakeDriver{}, registry())
	_, err := svc.Get(context.Background(), "Nope", "x")
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
}

func TestServiceHappyPath(t *testing.T) {
	want := wrapperspb.String("Dune")
	svc := adapter.New(&fakeDriver{getMsg: want}, registry())
	got, err := svc.Get(context.Background(), "Book", "books/dune")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("Get returned %v, want %v", got, want)
	}
}
