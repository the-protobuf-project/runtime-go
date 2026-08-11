package nats

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/options"
	"github.com/machanirobotics/loom/go/nats/types"
)

var (
	e2eCli     *NatsService
	e2eCliOnce sync.Once
	errBoot    error
)

func requireClient(t *testing.T) {
	t.Helper()

	e2eCliOnce.Do(func() {
		url := os.Getenv("NATS_URL")
		if url == "" {
			errBoot = fmt.Errorf("NATS_URL not set and embedded server not started")
			return
		}
		if err := waitForNATS(url, 10*time.Second); err != nil {
			errBoot = err
			return
		}
		var err error
		e2eCli, err = NewNatsClient(options.NatsClientOptions{
			EnableJetStream: true,
		})
		if err != nil {
			errBoot = err
			return
		}
	})

	if errBoot != nil {
		t.Fatalf("bootstrap error: %v", errBoot)
	}

	if e2eCli == nil || e2eCli.Nats == nil || e2eCli.Nats.Conn() == nil {
		t.Fatalf("client not initialized")
	}

	// Wait up to a short grace for CONNECTED
	deadline := time.Now().Add(3 * time.Second)
	for e2eCli.Nats.Conn().Status() != nats.CONNECTED && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if e2eCli.Nats.Conn().Status() != nats.CONNECTED {
		t.Fatalf("nats not connected: %v", e2eCli.Nats.Conn().Status())
	}
}

/***********************
 * Local test helpers
 ***********************/

func ensureStream(t *testing.T, name string) *StreamHandler {
	t.Helper()
	subjects := []string{name + ".>"} // namespace to avoid overlap
	st, err := e2eCli.Jetstream.Stream.CreateStream(e2eCli.RegularStream(name, subjects))
	if err != nil {
		// If exists/overlap, try update to our subjects
		if strings.Contains(strings.ToLower(err.Error()), "exist") ||
			strings.Contains(strings.ToLower(err.Error()), "overlap") ||
			strings.Contains(err.Error(), "10065") {
			st, err = e2eCli.Jetstream.Stream.UpdateStream(e2eCli.RegularStream(name, subjects))
		}
	}
	if err != nil {
		t.Fatalf("ensure stream failed: %v", err)
	}
	// Clean up this stream after the test
	t.Cleanup(func() {
		_ = e2eCli.Jetstream.Stream.DeleteStream(name)
	})
	return st
}

func publishN(t *testing.T, st *StreamHandler, subj string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ack, err := st.Producer.Publish(types.JetStreamPublishRequest{
			Subject: subj,
			Data:    []byte(fmt.Sprintf("msg-%d", i)),
		})
		if err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		if ack == nil || ack.Sequence == 0 {
			t.Fatalf("bad ack: %+v", ack)
		}
	}
}

// Fallback “not found” check for tests
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not found") || strings.Contains(s, "404")
}

/***************
 *   TESTS
 ***************/

// 1) Ensure + Get + Update + List basics
func TestConsumer_Basics_Ensure_Get_Update_List(t *testing.T) {
	requireClient(t)
	streamName := fmt.Sprintf("cons-e2e-basics-%d", time.Now().UnixNano())
	st := ensureStream(t, streamName)

	consName := "c-basics"
	// Ensure (create or get)
	ops, err := st.Consumer.EnsureConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil {
		t.Fatalf("EnsureConsumer failed: %v", err)
	}
	if ops == nil {
		t.Fatalf("EnsureConsumer returned nil ops")
	}

	// Get should succeed now
	ops2, err := st.Consumer.GetConsumer(consName)
	if err != nil || ops2 == nil {
		t.Fatalf("GetConsumer failed: %v", err)
	}

	// Update (no-op update)
	ops3, err := st.Consumer.UpdateConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil || ops3 == nil {
		t.Fatalf("UpdateConsumer failed: %v", err)
	}

	// List should include our consumer
	names, err := st.Consumer.ListConsumers()
	if err != nil {
		t.Fatalf("ListConsumers failed: %v", err)
	}
	found := false
	for _, n := range names {
		if n == consName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("consumer %q not found in list=%v", consName, names)
	}
}

// 2) Create explicitly + idempotent Ensure + Delete + verify not found
func TestConsumer_Create_Delete_EnsureIdempotent(t *testing.T) {
	requireClient(t)
	streamName := fmt.Sprintf("cons-e2e-create-%d", time.Now().UnixNano())
	st := ensureStream(t, streamName)

	consName := "c-create"

	// Create
	ops, err := st.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil || ops == nil {
		t.Fatalf("CreateConsumer failed: %v", err)
	}

	// Ensure again (should just return existing)
	ops2, err := st.Consumer.EnsureConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil || ops2 == nil {
		t.Fatalf("EnsureConsumer failed: %v", err)
	}

	// Delete
	if err := st.Consumer.DeleteConsumer(consName); err != nil {
		t.Fatalf("DeleteConsumer failed: %v", err)
	}

	// After delete, Get should produce not-found
	if _, err := st.Consumer.GetConsumer(consName); err == nil || !isNotFoundErr(err) {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}

// 3) Pull subscribe with cancel: no noisy errors on shutdown
func TestConsumer_SubscribePull_CancelCleanly(t *testing.T) {
	requireClient(t)
	streamName := fmt.Sprintf("cons-e2e-pull-%d", time.Now().UnixNano())
	st := ensureStream(t, streamName)

	// Seed some messages
	subj := streamName + ".input"
	publishN(t, st, subj, 5)

	// Ensure the consumer
	consName := "c-pull"
	ops, err := st.Consumer.EnsureConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil {
		t.Fatalf("EnsureConsumer failed: %v", err)
	}

	// start a cancelable context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := ops.SubscribePull(helpers.NatsContext{Ctx: ctx})
	if err != nil {
		t.Fatalf("SubscribePull failed: %v", err)
	}

	got := 0
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				// ended cleanly
				if got == 0 {
					t.Fatalf("expected to receive messages")
				}
				return
			}
			got++
			if got >= 3 {
				cancel() // stop gracefully after a few
			}
		case <-time.After(7 * time.Second):
			t.Fatalf("timeout waiting for pulled messages")
		}
	}
}

// 4) Pause and resume the consumer
func TestConsumer_Pause_Resume(t *testing.T) {
	requireClient(t)
	streamName := fmt.Sprintf("cons-e2e-pause-%d", time.Now().UnixNano())
	st := ensureStream(t, streamName)

	consName := "c-pause"
	ops, err := st.Consumer.EnsureConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil {
		t.Fatalf("EnsureConsumer failed: %v", err)
	}

	until := time.Now().Add(500 * time.Millisecond)
	if _, err := ops.PauseConsumer(until); err != nil {
		t.Fatalf("PauseConsumer failed: %v", err)
	}
	if _, err := ops.ResumeConsumer(); err != nil {
		t.Fatalf("ResumeConsumer failed: %v", err)
	}
}

// 5) Ordered (ephemeral) consumer
func TestConsumer_Ordered_EphemeralPull(t *testing.T) {
	requireClient(t)
	streamName := fmt.Sprintf("cons-e2e-ordered-%d", time.Now().UnixNano())
	st := ensureStream(t, streamName)

	// publish a few
	subj := streamName + ".input"
	publishN(t, st, subj, 3)

	ops, err := st.Consumer.OrderedConsumer(types.OrderedConsumerConfig{
		FilterSubjects: []string{streamName + ".>"},
	})
	if err != nil {
		t.Fatalf("OrderedConsumer failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	ch, err := ops.SubscribePull(helpers.NatsContext{Ctx: ctx})
	if err != nil {
		t.Fatalf("SubscribePull failed: %v", err)
	}

	received := 0
loop:
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				break loop
			}
			received++
			if received >= 2 {
				cancel()
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for ordered pull")
		}
	}
	if received == 0 {
		t.Fatalf("expected to receive some messages from ordered consumer")
	}
}

// 6) CreateConsumer + GetConsumer + Delete + Ensure again (recreate)
func TestConsumer_Create_Get_Delete_EnsureAgain(t *testing.T) {
	requireClient(t)
	streamName := fmt.Sprintf("cons-e2e-recreate-%d", time.Now().UnixNano())
	st := ensureStream(t, streamName)

	consName := "c-recreate"
	ops, err := st.Consumer.CreateConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil || ops == nil {
		t.Fatalf("CreateConsumer failed: %v", err)
	}

	// Get should work
	if _, err := st.Consumer.GetConsumer(consName); err != nil {
		t.Fatalf("GetConsumer failed: %v", err)
	}

	// Delete
	if err := st.Consumer.DeleteConsumer(consName); err != nil {
		t.Fatalf("DeleteConsumer failed: %v", err)
	}

	// Ensure again should recreate
	ops2, err := st.Consumer.EnsureConsumer(types.ConsumerConfig{
		Name:          consName,
		FilterSubject: streamName + ".>",
	})
	if err != nil || ops2 == nil {
		t.Fatalf("EnsureConsumer (recreate) failed: %v", err)
	}
}
