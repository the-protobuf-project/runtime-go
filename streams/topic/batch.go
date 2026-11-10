package topic

import (
	"context"
	"time"
)

// BatchSink buffers values for a topic and publishes them in batches, by its
// [BatchPolicy]: the convenience protoc-gen-streams gives a client-streaming
// method.
//
// Values still buffered when it is abandoned are not published; end with
// [BatchSink.Finish], or hand the whole stream to [BatchSink.SendAll]. It is
// not safe for concurrent use.
type BatchSink[T any] struct {
	pub    Publisher[T]
	policy BatchPolicy
	buf    []T
	oldest time.Time
	ids    []string
}

// NewBatchSink returns a sink over p, flushing by its topic's policy.
func NewBatchSink[T any](p Publisher[T]) *BatchSink[T] {
	return NewBatchSinkWithPolicy(p, p.t.spec.Batch)
}

// NewBatchSinkWithPolicy returns a sink over p, flushing by policy.
func NewBatchSinkWithPolicy[T any](p Publisher[T], policy BatchPolicy) *BatchSink[T] {
	return &BatchSink[T]{pub: p, policy: policy}
}

// Send buffers value, publishing the buffer when it is full or its oldest
// value has waited long enough.
//
// The delay is only checked here, when a value arrives: a sink fed by hand
// that goes quiet holds its buffer until the next Send or [BatchSink.Flush].
// [BatchSink.SendAll] flushes on time while waiting.
func (s *BatchSink[T]) Send(ctx context.Context, value T) error {
	if s.oldest.IsZero() {
		s.oldest = time.Now()
	}
	s.buf = append(s.buf, value)
	if len(s.buf) >= max(s.policy.MaxSize, 1) || s.overdue() {
		return s.Flush(ctx)
	}
	return nil
}

// Flush publishes everything buffered.
func (s *BatchSink[T]) Flush(ctx context.Context) error {
	s.oldest = time.Time{}
	if len(s.buf) == 0 {
		return nil
	}
	values := s.buf
	s.buf = nil
	ids, err := s.pub.PublishBatch(ctx, values)
	s.ids = append(s.ids, ids...)
	return err
}

// Finish publishes what is left and returns the id of every value sent, in the
// order they were sent.
func (s *BatchSink[T]) Finish(ctx context.Context) ([]string, error) {
	err := s.Flush(ctx)
	return s.ids, err
}

// SendAll publishes every value received from values in batches, flushing on
// size and on delay even while the channel is idle, and returns every id once
// values is closed. It stops early, with what it has published, when ctx is
// done.
func (s *BatchSink[T]) SendAll(ctx context.Context, values <-chan T) ([]string, error) {
	for {
		var (
			timer   *time.Timer
			timeout <-chan time.Time
		)
		if !s.oldest.IsZero() {
			left := s.policy.MaxDelay - time.Since(s.oldest)
			if left <= 0 {
				if err := s.Flush(ctx); err != nil {
					return s.ids, err
				}
				continue
			}
			timer = time.NewTimer(left)
			timeout = timer.C
		}
		select {
		case v, ok := <-values:
			if timer != nil {
				timer.Stop()
			}
			if !ok {
				return s.Finish(ctx)
			}
			if err := s.Send(ctx, v); err != nil {
				return s.ids, err
			}
		case <-timeout:
			if err := s.Flush(ctx); err != nil {
				return s.ids, err
			}
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return s.ids, ctx.Err()
		}
	}
}

// overdue reports whether the oldest buffered value has waited out the
// policy's delay.
func (s *BatchSink[T]) overdue() bool {
	return !s.oldest.IsZero() && time.Since(s.oldest) >= s.policy.MaxDelay
}
