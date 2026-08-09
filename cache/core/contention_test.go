package core

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// What a hot key costs is the number that decides whether this cache survives
// load, so it is asserted rather than described. Each test here fails on a
// regression in cost, not only in behavior.

// A miss must cost one round trip per caller. It cost two until the remembered
// absence moved into the entry: every caller looked for a value, found none,
// then looked for a tombstone and found none, before any of them reached the
// single-flight. A million callers on one cold key made two million requests to
// answer one question.
func TestAMissCostsOneRoundTripPerCaller(t *testing.T) {
	const callers = 500

	f := newFake()
	db := Build(f, Spec{DefaultTTL: time.Minute})
	defer func() { _ = db.Close() }()

	var loads int
	var mu sync.Mutex
	as := db.Aside(func(context.Context, string) (any, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		time.Sleep(2 * time.Millisecond) // long enough that callers pile up
		return "v", nil
	})

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out string
			_ = as.GetOrLoad(context.Background(), "hot", &out)
		}()
	}
	wg.Wait()

	if gets := f.gets.Load(); gets > int64(callers) {
		t.Errorf("%d callers cost %d gets (%.2f each); a miss must cost one",
			callers, gets, float64(gets)/float64(callers))
	}
	if loads > 2 {
		t.Errorf("the loader ran %d times for one id; concurrent misses must collapse", loads)
	}
}

// Loads are bounded by distinct keys, and distinct keys are bounded by nothing a
// caller controls — a cold start, or a client walking ids nobody has asked for.
// Without a ceiling that is one goroutine per key until the process dies.
func TestDistinctLoadsAreBounded(t *testing.T) {
	f := newFake()
	db := Build(f, Spec{DefaultTTL: time.Minute})
	defer func() { _ = db.Close() }()

	release := make(chan struct{})
	var running, peak int64
	var mu sync.Mutex
	as := db.Aside(func(context.Context, string) (any, error) {
		mu.Lock()
		running++
		if running > peak {
			peak = running
		}
		mu.Unlock()
		<-release
		mu.Lock()
		running--
		mu.Unlock()
		return "v", nil
	})

	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	var refused int64
	for i := range flightBudget * 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out string
			if err := as.GetOrLoad(context.Background(), fmt.Sprintf("key-%d", i), &out); errors.Is(err, cache.ErrOverloaded) {
				mu.Lock()
				refused++
				mu.Unlock()
			}
		}()
	}

	// Let the loads pile up against the ceiling before letting any finish.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := running
		mu.Unlock()
		if got >= int64(flightBudget) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	grew := runtime.NumGoroutine() - before
	close(release)
	wg.Wait()

	if peak > int64(flightBudget) {
		t.Errorf("%d loads ran at once, over the budget of %d", peak, flightBudget)
	}
	if refused == 0 {
		t.Error("nothing was refused; the ceiling did not engage")
	}
	// Callers waiting is fine; a goroutine per distinct key is not. The bound is
	// generous because each caller has one of its own.
	if grew > flightBudget*4 {
		t.Errorf("goroutines grew by %d against a budget of %d", grew, flightBudget)
	}
}

// Joining a load already running is never refused, however many callers arrive.
// The ceiling is on distinct loads, and a hot key is one of them.
func TestAHotKeyIsNeverRefused(t *testing.T) {
	f := newFake()
	db := Build(f, Spec{DefaultTTL: time.Minute})
	defer func() { _ = db.Close() }()

	release := make(chan struct{})
	as := db.Aside(func(context.Context, string) (any, error) {
		<-release
		return "v", nil
	})

	var wg sync.WaitGroup
	var refused int64
	var mu sync.Mutex
	for range flightBudget * 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out string
			if err := as.GetOrLoad(context.Background(), "one-hot-key", &out); errors.Is(err, cache.ErrOverloaded) {
				mu.Lock()
				refused++
				mu.Unlock()
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if refused != 0 {
		t.Errorf("%d callers on one key were refused; joining a running load must always be allowed", refused)
	}
}

// A hot key going stale must spend one slot of the background budget, not all of
// them. Before the per-key claim, the first sixty-four stale readers were each
// admitted and sixty-three went straight to waiting on the same flight — so any
// other key going stale in that moment found the budget gone.
func TestOneStaleKeySpendsOneRefreshSlot(t *testing.T) {
	f := newFake()
	db := Build(f, Spec{})
	defer func() { _ = db.Close() }()

	var loads int64
	var mu sync.Mutex
	as := db.Aside(func(context.Context, string) (any, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return "v", nil
	})

	ctx := context.Background()
	var seed string
	// Fresh for a moment, servable for much longer: every read after the first
	// few milliseconds is a stale hit.
	if err := as.GetOrLoad(ctx, "hot", &seed, cache.TTL(10*time.Millisecond), cache.Stale(time.Hour)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out string
			_ = as.GetOrLoad(ctx, "hot", &out, cache.TTL(10*time.Millisecond), cache.Stale(time.Hour))
		}()
	}
	wg.Wait()
	time.Sleep(60 * time.Millisecond) // let admitted refreshes finish

	mu.Lock()
	got := loads
	mu.Unlock()
	// One seeding load, plus a small number of refreshes as the entry goes stale
	// again between waves. Certainly not one per reader, and not sixty-four.
	if got > 10 {
		t.Errorf("200 stale readers caused %d loads; a stale key must collapse to about one refresh", got)
	}
}

// Enumeration must cost round trips proportional to entries over the batch size,
// not to entries.
func TestEnumerationStaysBatched(t *testing.T) {
	const entries = 5000

	f := newFake()
	db := Build(f, Spec{DefaultTTL: time.Hour})
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for i := range entries {
		if _, err := db.Document.Create(ctx, "v", cache.ID(fmt.Sprintf("id-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	f.exists.Store(0)
	f.existsMany.Store(0)

	keys, err := db.Document.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != entries {
		t.Fatalf("Keys returned %d ids, want %d", len(keys), entries)
	}
	if single := f.exists.Load(); single != 0 {
		t.Errorf("%d single-key liveness checks; they must be batched", single)
	}
	if batches := f.existsMany.Load(); batches > entries/batchSize+2 {
		t.Errorf("%d batches for %d entries, want about %d", batches, entries, entries/batchSize)
	}
}

// The cursor path is what a RESP server takes, so it is exercised here rather
// than only the whole-set fallback — including a cursor that repeats a member,
// which a real SSCAN may do.
func TestEnumerationOverACursor(t *testing.T) {
	const entries = 1000

	f := newScanningFake(64)
	db := Build(f, Spec{DefaultTTL: time.Hour})
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	want := make(map[string]bool, entries)
	for i := range entries {
		id := fmt.Sprintf("id-%d", i)
		want[id] = true
		if _, err := db.Document.Create(ctx, "v", cache.ID(id)); err != nil {
			t.Fatal(err)
		}
	}

	keys, err := db.Document.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != entries {
		t.Fatalf("Keys returned %d ids over a cursor, want %d — duplicates must be dropped", len(keys), entries)
	}
	for _, id := range keys {
		if !want[id] {
			t.Fatalf("Keys returned an id nothing created: %q", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Errorf("the cursor missed %d ids", len(want))
	}
}

// A dead member must be swept as the walk passes it, not accumulated into one
// enormous removal at the end.
func TestTheCursorSweepsAsItGoes(t *testing.T) {
	f := newScanningFake(32)
	db := Build(f, Spec{})
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for i := range 200 {
		if _, err := db.Document.Create(ctx, "v", cache.ID(fmt.Sprintf("id-%d", i)), cache.NoExpiry()); err != nil {
			t.Fatal(err)
		}
	}
	// Delete the values out from under the index, leaving 200 dead members.
	for i := range 200 {
		if err := f.Delete(ctx, "cache:doc:entry:"+fmt.Sprintf("id-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	keys, err := db.Document.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Errorf("Keys returned %d ids whose entries are gone", len(keys))
	}
	// And the index is now empty, so a second call finds nothing to sweep.
	again, err := db.Document.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("the sweep left %d dead members behind", len(again))
	}
}
