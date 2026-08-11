package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// batchOf returns n values to publish.
func batchOf(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = event{User: "ada", Action: "placed"}
	}
	return out
}

func TestPublishBatchDeliversEveryEntry(t *testing.T) {
	s := testStreams(t)
	_, m := declare(t, s, subject)

	b, err := streams.AsBatch(m)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}

	const total = 25
	ids, err := b.PublishBatch(t.Context(), subject, batchOf(total))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(ids) != total {
		t.Fatalf("got %d ids, want one per value (%d)", len(ids), total)
	}

	seen := map[string]bool{}
	for i, id := range ids {
		if id == "" {
			t.Errorf("entry %d has no id", i)
		}
		if seen[id] {
			t.Errorf("id %q was assigned twice", id)
		}
		seen[id] = true
	}

	// Every one of them has to actually arrive.
	p, err := streams.AsPositioned(m)
	if err != nil {
		t.Fatalf("AsPositioned: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := p.ConsumeFrom(ctx, subject, "batch-reader", streams.FromEarliest)
	if err != nil {
		t.Fatalf("ConsumeFrom: %v", err)
	}

	got := 0
	deadline := time.After(30 * time.Second)
	for got < total {
		select {
		case d := <-ch:
			if !seen[d.ID] {
				t.Errorf("received an id nobody published: %q", d.ID)
			}
			got++
			_ = d.Ack(ctx)
		case <-deadline:
			t.Fatalf("received %d of %d published entries", got, total)
		}
	}
}

// One id cannot name several messages, so the option is refused rather than
// silently applied to the first entry or to all of them.
func TestPublishBatchRejectsAnID(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	b, _ := streams.AsBatch(m)

	_, err := b.PublishBatch(t.Context(), subject, batchOf(2), streams.ID("fixed"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an ID = %v, want ErrUnsupported", err)
	}
}

func TestPublishBatchRejectsAnUndeclaredSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	b, _ := streams.AsBatch(m)

	_, err := b.PublishBatch(t.Context(), "typo", batchOf(2))
	if !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("PublishBatch = %v, want ErrUnknownSubject", err)
	}
}

func TestPublishBatchOfNothingIsNotAnError(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	b, _ := streams.AsBatch(m)

	ids, err := b.PublishBatch(t.Context(), subject, nil)
	if err != nil {
		t.Errorf("PublishBatch of nothing: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("got %d ids for an empty batch", len(ids))
	}
}

// The reason this API exists. Publish waits for the broker to acknowledge each
// message, so N values are N round trips; PublishBatch hands them over together
// and waits once.
//
//	go test ./kafka/ -bench BatchVsLoop -benchtime 3x -run '^$'
func BenchmarkBatchVsLoop(b *testing.B) {
	const total = 200

	b.Run("loop", func(b *testing.B) {
		s := testStreams(b)
		_, m := declare(b, s, subject)
		values := batchOf(total)

		b.ResetTimer()
		for range b.N {
			for _, v := range values {
				if _, err := m.Publish(b.Context(), subject, v); err != nil {
					b.Fatalf("Publish: %v", err)
				}
			}
		}
	})

	b.Run("batch", func(b *testing.B) {
		s := testStreams(b)
		_, m := declare(b, s, subject)
		batch, err := streams.AsBatch(m)
		if err != nil {
			b.Fatalf("AsBatch: %v", err)
		}
		values := batchOf(total)

		b.ResetTimer()
		for range b.N {
			if _, err := batch.PublishBatch(b.Context(), subject, values); err != nil {
				b.Fatalf("PublishBatch: %v", err)
			}
		}
	})
}
