package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// RequireTTL exists to catch a forgotten lease, not a deliberate one. These
// separate the two, because the whole value of the setting is that it only fires
// on the first.

func requiring(t *testing.T) *cache.DB {
	t.Helper()
	db := Build(newFake(), Spec{RequireTTL: true})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWritesWithoutATTLAreRefused(t *testing.T) {
	db := requiring(t)
	ctx := context.Background()

	if _, err := db.Document.Create(ctx, "v", cache.ID("a")); !errors.Is(err, cache.ErrNoTTL) {
		t.Errorf("Document.Create: got %v, want ErrNoTTL", err)
	}
	if err := db.Document.Update(ctx, "a", "v"); !errors.Is(err, cache.ErrNoTTL) {
		t.Errorf("Document.Update: got %v, want ErrNoTTL", err)
	}
	if err := db.Volatile.Set(ctx, "k", "v"); !errors.Is(err, cache.ErrNoTTL) {
		t.Errorf("Volatile.Set: got %v, want ErrNoTTL", err)
	}
	if err := db.Volatile.Touch(ctx, "k", 0); !errors.Is(err, cache.ErrNoTTL) {
		t.Errorf("Volatile.Touch: got %v, want ErrNoTTL", err)
	}
}

// The strategy this matters most for: a read-through cache with no lease keeps
// every id it was ever asked for.
func TestReadThroughWithoutATTLIsRefused(t *testing.T) {
	db := requiring(t)
	var loaded int
	as := db.Aside(func(context.Context, string) (any, error) {
		loaded++
		return "v", nil
	})

	var out string
	err := as.GetOrLoad(context.Background(), "a", &out)
	if !errors.Is(err, cache.ErrNoTTL) {
		t.Fatalf("got %v, want ErrNoTTL", err)
	}
	// Refused before the loader ran, not after storing what it returned.
	if loaded != 0 {
		t.Errorf("the loader ran %d time(s); it should have been refused first", loaded)
	}
}

func TestATTLSatisfiesTheRequirement(t *testing.T) {
	db := requiring(t)
	ctx := context.Background()

	if _, err := db.Document.Create(ctx, "v", cache.ID("a"), cache.TTL(time.Minute)); err != nil {
		t.Errorf("a per-call TTL was refused: %v", err)
	}
	if err := db.Volatile.Set(ctx, "k", "v", cache.TTL(time.Minute)); err != nil {
		t.Errorf("a per-call TTL was refused: %v", err)
	}
}

func TestAConfiguredDefaultSatisfiesTheRequirement(t *testing.T) {
	db := Build(newFake(), Spec{RequireTTL: true, DefaultTTL: time.Minute})
	defer func() { _ = db.Close() }()

	if _, err := db.Document.Create(context.Background(), "v", cache.ID("a")); err != nil {
		t.Errorf("the configured default was not applied: %v", err)
	}
}

// A deliberate permanent entry is not the bug this catches, so it is allowed.
func TestNoExpiryIsAllowedUnderRequireTTL(t *testing.T) {
	db := requiring(t)
	ctx := context.Background()

	if _, err := db.Document.Create(ctx, "v", cache.ID("a"), cache.NoExpiry()); err != nil {
		t.Errorf("a deliberate permanent entry was refused: %v", err)
	}
	if err := db.Volatile.Set(ctx, "k", "v", cache.NoExpiry()); err != nil {
		t.Errorf("a deliberate permanent entry was refused: %v", err)
	}
}

// NoExpiry has to beat a configured default, or a cache with a DefaultTTL could
// never store a permanent entry at all.
func TestNoExpiryOverridesTheConfiguredDefault(t *testing.T) {
	o := cache.NewOptions(cache.Options{TTL: time.Minute}, cache.NoExpiry())
	if o.TTL != 0 {
		t.Errorf("NoExpiry left a TTL of %v", o.TTL)
	}
	if !o.Permanent {
		t.Error("NoExpiry did not mark the entry permanent")
	}
}

// Order matters the other way too: a TTL named after NoExpiry should win, since
// the later option is the more specific statement of intent.
func TestALaterTTLWinsOverNoExpiry(t *testing.T) {
	o := cache.NewOptions(cache.Options{}, cache.NoExpiry(), cache.TTL(time.Minute))
	if o.TTL != time.Minute {
		t.Errorf("TTL = %v, want 1m", o.TTL)
	}
}

func TestTheRefusalSaysHowToFixIt(t *testing.T) {
	db := requiring(t)
	_, err := db.Document.Create(context.Background(), "v", cache.ID("orders-42"))
	if err == nil {
		t.Fatal("expected a refusal")
	}
	for _, want := range []string{"orders-42", "Document.Create", "cache.TTL", "DefaultTTL", "NoExpiry"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not mention %q: %v", want, err)
		}
	}
}

// Off by default: every existing caller keeps working unchanged.
func TestWithoutRequireTTLNothingChanges(t *testing.T) {
	db := Build(newFake(), Spec{})
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.Document.Create(ctx, "v", cache.ID("a")); err != nil {
		t.Errorf("Document.Create: %v", err)
	}
	if err := db.Volatile.Set(ctx, "k", "v"); err != nil {
		t.Errorf("Volatile.Set: %v", err)
	}
}

// Create names the entry itself unless told otherwise. The empty-string
// parameter this replaced meant "generate one" nowhere in the call.
func TestCreateGeneratesAnIDUnlessGivenOne(t *testing.T) {
	db := Build(newFake(), Spec{})
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	got, err := db.Document.Create(ctx, "v")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got == "" {
		t.Fatal("Create returned an empty id")
	}

	second, err := db.Document.Create(ctx, "v")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if second == got {
		t.Errorf("two generated ids collided: %q", got)
	}

	chosen, err := db.Document.Create(ctx, "v", cache.ID("order-42"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if chosen != "order-42" {
		t.Errorf("ID option ignored: got %q", chosen)
	}

	// And the entry is actually reachable under the id that was returned.
	var out string
	if err := db.Document.Get(ctx, chosen, &out); err != nil {
		t.Errorf("get %q: %v", chosen, err)
	}
}

// Indexed resolves the id before filing the index members, so a chosen id has to
// reach both halves — the entry and the index that names it.
func TestIndexedHonorsAChosenID(t *testing.T) {
	db := Build(newFake(), Spec{})
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	got, err := db.Indexed.Create(ctx, "v", cache.ID("order-42"), cache.Index("tenant", "acme"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got != "order-42" {
		t.Fatalf("ID option ignored: got %q", got)
	}

	ids, err := db.Indexed.IDsByIndex(ctx, "tenant", "acme")
	if err != nil {
		t.Fatalf("by index: %v", err)
	}
	if len(ids) != 1 || ids[0] != "order-42" {
		t.Errorf("the index names %v, want [order-42]", ids)
	}
}
