package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

type record struct {
	Name string `json:"name"`
}

// build returns a database over a counting driver.
func build(t *testing.T, spec Spec) (*cache.DB, *fake) {
	t.Helper()
	driver := newFake()
	db := Build(driver, spec)
	t.Cleanup(func() { _ = db.Close() })
	return db, driver
}

// TestLoadsCollapse is the claim the old polling implementation only appeared to
// satisfy: many concurrent misses cause one load, and cost round trips
// proportional to the callers rather than to how long the load took.
func TestLoadsCollapse(t *testing.T) {
	const callers = 500

	db, driver := build(t, Spec{Prefix: "t"})
	var loads atomic.Int64
	view := db.Aside(func(ctx context.Context, id string) (any, error) {
		loads.Add(1)
		time.Sleep(100 * time.Millisecond) // a slow backing read
		return record{Name: "loaded"}, nil
	})

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var got record
			if err := view.GetOrLoad(t.Context(), "hot", &got, cache.TTL(time.Minute)); err != nil {
				t.Error(err)
			} else if got.Name != "loaded" {
				t.Errorf("got %+v", got)
			}
		}()
	}
	wg.Wait()

	if n := loads.Load(); n != 1 {
		t.Fatalf("expected exactly 1 load, got %d", n)
	}
	// Each caller reads the entry once, and misses also read the absence marker.
	// The old version added a poll every 25ms per waiting caller on top; over a
	// 100ms load that alone would have been thousands more.
	if n := driver.gets.Load(); n > 2*callers {
		t.Fatalf("expected at most %d reads, got %d", 2*callers, n)
	}
	t.Logf("%d concurrent misses: %d load, %d driver reads", callers, loads.Load(), driver.gets.Load())
}

// TestCancelledCallerDoesNotCancelTheLoad covers the failure that only shows up
// under load: one caller giving up must not fail the load everyone else joined.
func TestCancelledCallerDoesNotCancelTheLoad(t *testing.T) {
	db, _ := build(t, Spec{Prefix: "t"})

	release := make(chan struct{})
	var loads atomic.Int64
	view := db.Aside(func(ctx context.Context, id string) (any, error) {
		loads.Add(1)
		<-release
		if err := ctx.Err(); err != nil {
			return nil, err // would fire if the load inherited a caller's context
		}
		return record{Name: "loaded"}, nil
	})

	quitter, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	go func() {
		close(started)
		var got record
		_ = view.GetOrLoad(quitter, "id", &got, cache.TTL(time.Minute))
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // let it become the leader

	var stayed sync.WaitGroup
	results := make([]error, 5)
	for i := range results {
		stayed.Add(1)
		go func() {
			defer stayed.Done()
			var got record
			results[i] = view.GetOrLoad(t.Context(), "id", &got, cache.TTL(time.Minute))
		}()
	}

	time.Sleep(20 * time.Millisecond)
	cancel()       // the caller that started the load gives up
	close(release) // the loader finishes
	stayed.Wait()

	for i, err := range results {
		if err != nil {
			t.Fatalf("waiter %d failed after the leader canceled: %v", i, err)
		}
	}
	if n := loads.Load(); n != 1 {
		t.Fatalf("expected 1 load, got %d", n)
	}
}

// TestStaleIsServedWithoutBlocking is the non-blocking claim: past its TTL but
// inside its stale window, a read returns at once and the refresh happens behind
// it.
func TestStaleIsServedWithoutBlocking(t *testing.T) {
	db, _ := build(t, Spec{Prefix: "t"})

	var loads atomic.Int64
	view := db.Aside(func(ctx context.Context, id string) (any, error) {
		n := loads.Add(1)
		if n > 1 {
			time.Sleep(200 * time.Millisecond) // the refresh is slow on purpose
		}
		return record{Name: fmt.Sprintf("v%d", n)}, nil
	})

	opts := []cache.Option{cache.TTL(50 * time.Millisecond), cache.Stale(time.Minute)}
	var got record
	if err := view.GetOrLoad(t.Context(), "id", &got, opts...); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond) // now stale, still servable

	start := time.Now()
	if err := view.GetOrLoad(t.Context(), "id", &got, opts...); err != nil {
		t.Fatal(err)
	}
	served := time.Since(start)

	if got.Name != "v1" {
		t.Fatalf("expected the stale value, got %+v", got)
	}
	if served > 50*time.Millisecond {
		t.Fatalf("stale read blocked for %v; it should not have waited for the refresh", served)
	}
	t.Logf("stale read served in %v while a 200ms refresh ran behind it", served)

	// And the refresh does land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := view.GetOrLoad(t.Context(), "id", &got, opts...); err == nil && got.Name == "v2" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the background refresh never published, still %+v", got)
}
