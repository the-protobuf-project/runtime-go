package topic

import (
	"context"
	"errors"
	"slices"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// errNoStreamID is returned by [Bind] for a stream without an ID: a generated
// one would declare a new stream on every start.
var errNoStreamID = errors.New("topic: Bind needs the stream's ID, so every start finds the stream the last one declared")

// Bind declares stream on provider unless it already exists, binds it, and
// checks that the bound manager has every capability in caps.
//
// A stream that exists but lacks some of stream's subjects, because the proto
// gained a method since it was created, is updated to declare them as well.
// Subjects are only ever added: one this build no longer uses may still have
// publishers and subscribers in a build that does. Nothing else of the existing
// declaration changes, so headers or a description set by an operator survive a
// redeploy.
//
// It lives here rather than on [streams.Streams] because it is a policy, not a
// primitive: whether a running service may widen a shared declaration is the
// caller's decision, and a caller that may not uses [streams.Streams.Bind].
func Bind(ctx context.Context, provider streams.Streams, stream streams.Stream, caps ...streams.Capability) (streams.Manager, error) {
	if stream.ID == "" {
		return nil, errNoStreamID
	}
	id := stream.ID
	existing, err := provider.Get(ctx, id)
	switch {
	case err == nil:
		var missing []string
		for _, subject := range stream.Subjects {
			if !slices.Contains(existing.Subjects, subject) && !slices.Contains(missing, subject) {
				missing = append(missing, subject)
			}
		}
		if len(missing) > 0 {
			wider := existing
			wider.Subjects = append(slices.Clone(existing.Subjects), missing...)
			if _, uerr := provider.Update(ctx, existing.ID, wider); uerr != nil {
				return nil, uerr
			}
		}
		id = existing.ID
	case errors.Is(err, streams.ErrNotFound):
		created, cerr := provider.Create(ctx, stream)
		if cerr != nil {
			return nil, cerr
		}
		id = created.ID
	default:
		return nil, err
	}

	m, err := provider.Bind(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := streams.Require(m, caps...); err != nil {
		return nil, err
	}
	return m, nil
}
