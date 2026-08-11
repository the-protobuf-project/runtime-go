package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// PublishEach implements [streams.Batch] by publishing one value at a time.
//
// It is the honest fallback for a backend with no batch primitive: the work is
// the same either way, and doing it here means a caller writes one shape of
// code whichever provider is underneath. Providers whose client can actually
// batch — Kafka, JetStream, Redis — do not use this.
//
// A failure does not stop the batch. Publishing three of five values and
// reporting which two failed is more use than stopping at the first and leaving
// the caller to work out what happened to the rest.
func PublishEach(ctx context.Context, p streams.Publisher, subject string, values []any, opts ...streams.Option) ([]string, error) {
	ids := make([]string, len(values))
	var failures []error

	for i, value := range values {
		id, err := p.Publish(ctx, subject, value, opts...)
		if err != nil {
			failures = append(failures, fmt.Errorf("entry %d: %w", i, err))
			continue
		}
		ids[i] = id
	}
	return ids, BatchError(subject, len(values), failures)
}

// BatchError folds per-entry failures into one error, or nil when there were
// none. Providers with their own batching path use it so every provider
// reports a partial failure the same way.
func BatchError(subject string, total int, failures []error) error {
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("streams: %d of %d entries failed to publish on %q: %w",
		len(failures), total, subject, errors.Join(failures...))
}

// CheckBatch rejects the options a batch cannot honor, and is where every
// provider agrees on what those are.
func CheckBatch(o streams.Options) error {
	if o.ID != "" {
		return fmt.Errorf("%w: one id cannot name several messages; drop the ID option from a batch", streams.ErrUnsupported)
	}
	return nil
}
