package database

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

// The registry is package state, so tests that register a driver have to put it
// back. Backend names here are deliberately not the real ones.
func withRegistered(t *testing.T, name Backend, opener Opener) {
	t.Helper()
	Register(name, opener)
	t.Cleanup(func() {
		registryMu.Lock()
		delete(drivers, name)
		registryMu.Unlock()
	})
}

type stubDriver struct{ cfg any }

func (s *stubDriver) Create(context.Context, *Resource, proto.Message) (WriteResult, error) {
	return WriteResult{}, nil
}
func (s *stubDriver) Get(context.Context, *Resource, string) (proto.Message, error) {
	return nil, ErrNotFound
}
func (s *stubDriver) Update(context.Context, *Resource, proto.Message) (WriteResult, error) {
	return WriteResult{}, nil
}
func (s *stubDriver) Delete(context.Context, *Resource, string) error { return nil }
func (s *stubDriver) List(context.Context, *Resource, ListOptions) (ListResult, error) {
	return ListResult{}, nil
}
func (s *stubDriver) Count(context.Context, *Resource, ListOptions) (int64, error) { return 0, nil }
func (s *stubDriver) Exists(context.Context, *Resource, string) (bool, error)      { return false, nil }

// The config is opaque, so the registry's job is to hand it to the opener
// untouched — each driver asserts its own type.
func TestNewDriverPassesConfigThrough(t *testing.T) {
	type ormish struct{ dsn string }
	want := &ormish{dsn: "file::memory:"}

	var got any
	withRegistered(t, "stub", func(cfg any) (Driver, error) {
		got = cfg
		return &stubDriver{cfg: cfg}, nil
	})

	d, err := NewDriver("stub", want)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	if d == nil {
		t.Fatal("NewDriver returned a nil Driver with no error")
	}
	if got != any(want) {
		t.Errorf("opener received %#v, want the same config %#v", got, want)
	}
}

// An unregistered backend is the likely mistake — forgetting the import — so
// the error has to say which backends are available.
func TestNewDriverOnUnregisteredBackendNamesTheFix(t *testing.T) {
	withRegistered(t, "stub", func(any) (Driver, error) { return &stubDriver{}, nil })

	_, err := NewDriver("nope", nil)
	if err == nil {
		t.Fatal("expected an error for an unregistered backend")
	}
	for _, want := range []string{"nope", "stub", "import"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// A driver handed the wrong config type must report ErrBadConfig, so callers
// can tell a wiring mistake from a storage outcome.
func TestNewDriverPropagatesBadConfig(t *testing.T) {
	withRegistered(t, "stub", func(cfg any) (Driver, error) {
		if _, ok := cfg.(string); !ok {
			return nil, ErrBadConfig
		}
		return &stubDriver{}, nil
	})

	if _, err := NewDriver("stub", 42); !errors.Is(err, ErrBadConfig) {
		t.Errorf("NewDriver error = %v, want it to wrap ErrBadConfig", err)
	}
}

// Registering twice is a wiring bug that should surface at startup, not a
// silent last-one-wins.
func TestRegisterPanicsOnDuplicate(t *testing.T) {
	withRegistered(t, "stub", func(any) (Driver, error) { return &stubDriver{}, nil })

	defer func() {
		if recover() == nil {
			t.Error("Register did not panic on a duplicate backend")
		}
	}()
	Register("stub", func(any) (Driver, error) { return &stubDriver{}, nil })
}

func TestRegisterPanicsOnNilOpener(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Register did not panic on a nil opener")
		}
	}()
	Register("nil-opener", nil)
}

func TestBackendsIsSorted(t *testing.T) {
	withRegistered(t, "zeta", func(any) (Driver, error) { return &stubDriver{}, nil })
	withRegistered(t, "alpha", func(any) (Driver, error) { return &stubDriver{}, nil })

	names := Backends()
	for i := 1; i < len(names); i++ {
		if names[i-1] > names[i] {
			t.Fatalf("Backends() is not sorted: %v", names)
		}
	}
}
