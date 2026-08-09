package core

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// TestEnumerationIsBatched pins the round-trip cost of a sweep. One bulk call per
// 256 ids, and no per-key checks at all.
func TestEnumerationIsBatched(t *testing.T) {
	const entries = 600

	db, driver := build(t, Spec{Prefix: "t"})
	for i := range entries {
		if _, err := db.Document.Create(t.Context(), record{Name: "x"}, cache.ID(fmt.Sprintf("id%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	driver.exists.Store(0)
	driver.existsMany.Store(0)
	driver.getMany.Store(0)

	ids, err := db.Document.Keys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != entries {
		t.Fatalf("expected %d ids, got %d", entries, len(ids))
	}
	if n := driver.exists.Load(); n != 0 {
		t.Fatalf("expected no per-key checks, got %d", n)
	}
	want := int64((entries + batchSize - 1) / batchSize)
	if n := driver.existsMany.Load(); n != want {
		t.Fatalf("expected %d bulk checks, got %d", want, n)
	}
	t.Logf("%d entries swept in %d bulk calls (was %d serial round trips)", entries, want, entries)
}

// TestUnlockIsFenced covers the release that used to delete whatever lock was
// present, including one another holder had since taken.
func TestUnlockIsFenced(t *testing.T) {
	driver := newFake()
	ctx := t.Context()

	if _, err := driver.Add(ctx, "lock", []byte("mine"), time.Minute); err != nil {
		t.Fatal(err)
	}
	released, err := driver.DeleteIf(ctx, "lock", []byte("someone else's"))
	if err != nil {
		t.Fatal(err)
	}
	if released {
		t.Fatal("released a lock held by another token")
	}
	if released, err = driver.DeleteIf(ctx, "lock", []byte("mine")); err != nil || !released {
		t.Fatalf("owner could not release its own lock: %v %v", released, err)
	}
}

// TestCloseDrainsBackgroundWork covers shutdown: a refresh still running must
// finish before whatever it runs against is released.
func TestCloseDrainsBackgroundWork(t *testing.T) {
	driver := newFake()
	var released atomic.Bool
	db := Build(driver, Spec{Prefix: "t", Release: func() error {
		released.Store(true)
		return nil
	}})

	running := make(chan struct{})
	var finished atomic.Bool
	view := db.Aside(func(ctx context.Context, id string) (any, error) {
		if loaded := driver.sets_.Load(); loaded > 0 {
			close(running)
			time.Sleep(150 * time.Millisecond)
			finished.Store(true)
		}
		return record{Name: "v"}, nil
	})

	opts := []cache.Option{cache.TTL(20 * time.Millisecond), cache.Stale(time.Minute)}
	var got record
	if err := view.GetOrLoad(t.Context(), "id", &got, opts...); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := view.GetOrLoad(t.Context(), "id", &got, opts...); err != nil { // triggers the refresh
		t.Fatal(err)
	}
	<-running

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("Close returned while a background refresh was still running")
	}
	if !released.Load() {
		t.Fatal("Close did not release the database")
	}
}

// TestAbsenceIsRemembered keeps the negative-caching guarantee honest.
func TestAbsenceIsRemembered(t *testing.T) {
	db, _ := build(t, Spec{Prefix: "t"})

	var loads atomic.Int64
	view := db.Aside(func(ctx context.Context, id string) (any, error) {
		loads.Add(1)
		return nil, cache.ErrNotFound
	})

	var got record
	for range 5 {
		if err := view.GetOrLoad(t.Context(), "ghost", &got); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
	if n := loads.Load(); n != 1 {
		t.Fatalf("expected 1 load for 5 reads of something absent, got %d", n)
	}
}
