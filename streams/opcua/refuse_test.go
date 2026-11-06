package opcua_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// Each of these is a request this provider cannot honor, and each has to fail
// by name rather than do something weaker than was asked.
func TestRefusals(t *testing.T) {
	s, m := connect(t)
	ctx := context.Background()

	cases := []struct {
		name string
		err  error
		want error
	}{
		{"undeclared subject", publishErr(m, "ns=1;s=elsewhere", 1.0), streams.ErrUnknownSubject},
		{"ttl", publishErr(m, temperature, 1.0, streams.TTL(time.Second)), streams.ErrUnsupported},
		{"go int", publishErr(m, temperature, 1), streams.ErrUnsupported},
		{"group", subscribeErr(m, temperature, streams.Group("g")), streams.ErrUnsupported},
		{"durable", asErr(streams.AsDurable(m)), streams.ErrUnsupported},
		{"positioned", asErr(streams.AsPositioned(m)), streams.ErrUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.want) {
				t.Fatalf("err = %v, want %v", tc.err, tc.want)
			}
		})
	}

	if _, err := s.Create(ctx, streams.Stream{Subjects: []string{"ns=1;i=not-a-number"}}); err == nil {
		t.Fatal("Create accepted a subject that is not a node id")
	}
}

// The server's own refusals come back as errors naming the subject.
func TestServerRefusals(t *testing.T) {
	_, m := connect(t)
	ctx := context.Background()

	if _, err := m.Publish(ctx, readOnly, int32(1)); err == nil {
		t.Fatal("writing a read-only node succeeded")
	}
	if _, err := m.Publish(ctx, missing, 1.0); err == nil {
		t.Fatal("writing a node the server does not have succeeded")
	}

	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(ctx, readOnly, []any{int32(1), int32(2)})
	if err == nil || len(ids) != 2 {
		t.Fatalf("PublishBatch = %d ids, %v; want 2 ids and an error", len(ids), err)
	}
}

func TestLifecycle(t *testing.T) {
	s, _ := connect(t)
	ctx := context.Background()

	created, err := s.Create(ctx, streams.Stream{ID: "plant", Subjects: []string{temperature}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got, _ := s.Get(ctx, "plant"); got.ID != created.ID {
		t.Fatalf("Get = %+v, want %+v", got, created)
	}

	updated, err := s.Update(ctx, "plant", streams.Stream{Subjects: []string{payload}})
	if err != nil || updated.Subjects[0] != payload {
		t.Fatalf("Update = %+v, %v", updated, err)
	}
	if list, _ := s.List(ctx); len(list) != 2 {
		t.Fatalf("List has %d streams, want 2", len(list))
	}

	if err := s.Delete(ctx, "plant"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "plant"); !errors.Is(err, streams.ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
	if _, err := s.Bind(ctx, "plant"); !errors.Is(err, streams.ErrNotFound) {
		t.Fatalf("Bind after Delete = %v, want ErrNotFound", err)
	}
}

func publishErr(m streams.Manager, subject string, v any, opts ...streams.Option) error {
	_, err := m.Publish(context.Background(), subject, v, opts...)
	return err
}

func subscribeErr(m streams.Manager, subject string, opts ...streams.Option) error {
	_, err := m.Subscribe(context.Background(), subject, opts...)
	return err
}

func asErr[T any](_ T, err error) error { return err }
