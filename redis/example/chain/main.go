package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/redis"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// user is this program's model. Nothing in the provider knows about it — that
// is the point of there being no document type: adding a field here changes
// nothing downstream.
type user struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A debug-level logger so the provider's own records are visible. Swap
	// telemetry.NewSlogLogger for observability's opentelemetry-backed logger to
	// export instead of printing.
	logger := telemetry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.Level(telemetry.LevelDebug)})))

	// --- THE CHAIN ---
	// One client, then a named database, then the handlers on the manager.
	c, err := redis.New(ctx, redis.Config{
		Address: "localhost",
		Port:    "6379",
		Logger:  logger,
	})
	if err != nil {
		log.Fatalf("Failed to open Redis: %v", err)
	}
	defer func() { _ = c.Close() }()

	const dbName = "example_chain"

	// Creating a database that already exists is reported, not silent — this
	// program re-runs, so the note is expected on the second pass.
	if cerr := c.CreateDatabase(ctx, dbName); cerr != nil {
		log.Printf("database note: %v", cerr)
	}

	mgr, err := c.SetDatabase(ctx, dbName)
	if err != nil {
		log.Fatalf("Failed to select the database: %v", err)
	}
	// A manager holds its own connection; closing the client does not close it.
	defer func() { _ = mgr.Close() }()

	log.Printf("using database %q (redis index %d)", mgr.Name(), mgr.Index())

	runCache(ctx, mgr)
	runKV(ctx, mgr)
	runStream(ctx, mgr)
	runNotify(ctx, mgr)

	log.Println("done")
}

// runCache shows ephemeral, TTL-bound storage.
func runCache(ctx context.Context, mgr *redis.DBManager) {
	log.Println("--- cache ---")

	alice := user{Name: "Alice", Email: "alice@example.com", Age: 30}

	// An empty id has the provider generate one.
	id, err := mgr.Document.Cache.Create(ctx, "", alice, cache.TTL(30*time.Second))
	if err != nil {
		log.Fatalf("cache Create: %v", err)
	}

	var got user
	if gerr := mgr.Document.Cache.Get(ctx, id, &got); gerr != nil {
		log.Fatalf("cache Get: %v", gerr)
	}
	ttl, _ := mgr.Document.Cache.TTL(ctx, id)
	log.Printf("cached %s -> %+v (%v remaining)", id, got, ttl.Round(time.Second))

	// The typed view is a wrapper over the same handler, not a second client.
	users := cache.For[user](mgr.Document.Cache)
	typedID, err := users.Create(ctx, user{Name: "Bob", Age: 25}, cache.TTL(time.Minute))
	if err != nil {
		log.Fatalf("typed Create: %v", err)
	}
	bob, err := users.Get(ctx, typedID) // returns a user, no decoding at the call site
	if err != nil {
		log.Fatalf("typed Get: %v", err)
	}
	log.Printf("typed view read %s -> %+v", typedID, bob)

	if derr := mgr.Document.Cache.Delete(ctx, id); derr != nil {
		log.Fatalf("cache Delete: %v", derr)
	}
	// A miss is a normal cache outcome, reported as ErrNotFound.
	if gerr := mgr.Document.Cache.Get(ctx, id, &got); errors.Is(gerr, cache.ErrNotFound) {
		log.Printf("%s is gone, as expected", id)
	}
}

// runKV shows durable, content-addressed storage.
func runKV(ctx context.Context, mgr *redis.DBManager) {
	log.Println("--- kv ---")

	book := map[string]any{"title": "Dune", "author": "Herbert", "year": 1965}

	id, err := mgr.Document.KV.Create(ctx, "", book)
	if err != nil {
		log.Fatalf("kv Create: %v", err)
	}
	log.Printf("stored %s", id)

	// Identical content resolves to the record already holding it — field order
	// is not content, so this deduplicates.
	same, err := mgr.Document.KV.Create(ctx, "",
		map[string]any{"year": 1965, "title": "Dune", "author": "Herbert"})
	if err != nil {
		log.Fatalf("kv Create (duplicate): %v", err)
	}
	if same == id {
		log.Printf("identical content deduplicated to %s", same)
	}

	// Records come back in a stable order, so Limit and Offset page predictably.
	keys, err := mgr.Document.KV.Keys(ctx, database.Limit(10))
	if err != nil {
		log.Fatalf("kv Keys: %v", err)
	}
	log.Printf("store holds %d record(s)", len(keys))

	if derr := mgr.Document.KV.Delete(ctx, id); derr != nil {
		log.Fatalf("kv Delete: %v", derr)
	}
	// Unlike a cache, a missing record is a genuine surprise.
	if derr := mgr.Document.KV.Delete(ctx, id); errors.Is(derr, database.ErrNotFound) {
		log.Printf("%s is gone, as expected", id)
	}
}

// runStream shows immediate pub/sub delivery.
func runStream(ctx context.Context, mgr *redis.DBManager) {
	log.Println("--- stream ---")

	const subject = "user.created"

	s, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name:        "users",
		Description: "user lifecycle events",
		Subjects:    []string{subject},
	})
	if err != nil {
		log.Fatalf("stream Create: %v", err)
	}
	defer func() { _ = mgr.Channel.Stream.Delete(context.Background(), s.ID) }()

	m, err := mgr.Channel.Stream.Bind(ctx, s.ID)
	if err != nil {
		log.Fatalf("stream Bind: %v", err)
	}

	// Subscribe first: the subscription is live by the time this returns, so
	// nothing published afterwards is raced. Canceling the context is what
	// closes the channel and releases the delivery goroutine.
	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		log.Fatalf("stream Subscribe: %v", err)
	}

	id, err := m.Publish(ctx, subject, user{Name: "Carol", Age: 41})
	if err != nil {
		log.Fatalf("stream Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		var got user
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("decode: %v", derr)
		}
		log.Printf("received %s -> %+v", msg.ID, got)
	case <-time.After(2 * time.Second):
		log.Fatalf("timed out waiting for %s", id)
	}

	// A subject the stream never declared fails at the call that made the typo.
	if _, perr := m.Publish(ctx, "user.typo", user{}); errors.Is(perr, streams.ErrUnknownSubject) {
		log.Println("an undeclared subject was rejected, as expected")
	}
}

// runNotify shows delivery when a TTL expires rather than on publish.
func runNotify(ctx context.Context, mgr *redis.DBManager) {
	log.Println("--- notify ---")

	const subject = "reminder"

	n, err := mgr.Channel.Notify.Create(ctx, streams.Stream{
		Name:     "reminders",
		Subjects: []string{subject},
	})
	if err != nil {
		log.Fatalf("notify Create: %v", err)
	}
	defer func() { _ = mgr.Channel.Notify.Delete(context.Background(), n.ID) }()

	m, err := mgr.Channel.Notify.Bind(ctx, n.ID)
	if err != nil {
		log.Fatalf("notify Bind: %v", err)
	}

	reminders, err := m.Subscribe(ctx, subject)
	if err != nil {
		log.Fatalf("notify Subscribe: %v", err)
	}

	// A TTL is required here: delivery is the expiry, so a message without one
	// could never fire.
	if _, perr := m.Publish(ctx, subject, user{Name: "take a pill"},
		streams.TTL(time.Second)); perr != nil {
		log.Fatalf("notify Publish: %v", perr)
	}
	log.Println("scheduled a reminder for 1s from now")

	select {
	case msg := <-reminders:
		var got user
		if derr := msg.Decode(&got); derr != nil {
			log.Fatalf("decode: %v", derr)
		}
		log.Printf("reminder fired: %+v", got)
	case <-time.After(5 * time.Second):
		log.Fatal("the reminder never fired (is --notify-keyspace-events Ex set?)")
	}
}
