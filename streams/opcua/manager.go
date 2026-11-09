package opcua

import (
	"context"
	"fmt"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// manager publishes to and subscribes from the nodes one stream declares.
type manager struct {
	store    *store
	declared declared
}

var (
	_ streams.Manager = (*manager)(nil)
	_ streams.Batch   = (*manager)(nil)
)

// node resolves a subject to the node it names on the server now, and the
// namespace URI it names it by, if it does.
func (m *manager) node(ctx context.Context, subject string) (*ua.NodeID, *named, error) {
	if n, ok := m.declared.nodes[subject]; ok {
		return n, nil, nil
	}
	if u, ok := m.declared.named[subject]; ok {
		n, err := m.store.hub.resolve(ctx, subject, u)
		if err != nil {
			return nil, nil, err
		}
		return n, &u, nil
	}
	s := m.declared.stream
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": s.ID, "declared": s.Subjects,
	})
	return nil, nil, core.ErrSubject(s.ID, subject, s.Subjects)
}

// checkPublish refuses what a Write cannot do: schedule, expire, retain, or
// carry headers.
func checkPublish(stream streams.Stream, o streams.Options) error {
	if o.TTL > 0 {
		return fmt.Errorf("%w: OPC UA writes a value now and cannot schedule one", streams.ErrUnsupported)
	}
	if o.Expires > 0 {
		return fmt.Errorf("%w: opcua: a Write applies at once or fails, so nothing waits that could expire", streams.ErrUnsupported)
	}
	if o.Retain {
		return fmt.Errorf("%w: opcua: a node always holds its last value and has no retain flag to set; WithInitialValue hands it to a new subscription", streams.ErrUnsupported)
	}
	headers, err := core.Outgoing(stream, o)
	if err != nil {
		return err
	}
	if len(headers) > 0 {
		return errNoHeaders
	}
	return nil
}

// Publish writes a value to the subject's node, and returns once the server
// has answered the Write.
//
// The id is returned either way, but travels with the value only when it went
// out framed; a native value is identified by its timestamp instead. See the
// package documentation.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	o := streams.NewOptions(opts...)
	node, _, err := m.node(ctx, subject)
	if err != nil {
		return "", err
	}
	if err := checkPublish(m.declared.stream, o); err != nil {
		return "", err
	}
	id := o.ID
	if id == "" {
		id = core.NewID()
	}
	if err := m.write(ctx, subject, node, id, value); err != nil {
		return id, err
	}
	return id, nil
}

// PublishBatch writes several values to the subject's node, one Write per
// value, each answered before the next is sent.
//
// A batch's values are all for one node, and a server applies the values of
// one Write request in whatever order it chooses ("If a Client requires
// sequential processing the Client needs separate Service calls", Part 4,
// 5.11.4.1), so a single request could leave the node holding any of them
// rather than the last. A value that fails fails alone, and every id is still
// returned. A subscriber sees each value only if the monitored item's queue
// holds them all; the last is what the node keeps.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	node, _, err := m.node(ctx, subject)
	if err != nil {
		return nil, err
	}
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	if err := checkPublish(m.declared.stream, o); err != nil {
		return nil, err
	}

	ids := make([]string, len(values))
	var failures []error
	for i, value := range values {
		ids[i] = core.NewID()
		if werr := m.write(ctx, subject, node, ids[i], value); werr != nil {
			failures = append(failures, fmt.Errorf("entry %d: %w", i, werr))
		}
	}
	return ids, core.BatchError(subject, len(values), failures)
}

// write sends one value to node and turns the server's answer into the
// contract's error for it.
func (m *manager) write(ctx context.Context, subject string, node *ua.NodeID, id string, value any) error {
	dv, err := dataValue(m.store.codec, id, value)
	if err != nil {
		return fmt.Errorf("opcua: cannot write to %q: %w", subject, err)
	}
	req := &ua.WriteRequest{NodesToWrite: []*ua.WriteValue{{
		NodeID:      node,
		AttributeID: ua.AttributeIDValue,
		Value:       dv,
	}}}

	res, err := m.store.client.Write(ctx, req)
	if err != nil {
		m.store.log.Error(ctx, "could not write", err, telemetry.Fields{"subject": subject})
		return fmt.Errorf("opcua: cannot write to %q: %w", subject, err)
	}
	if len(res.Results) != 1 {
		return fmt.Errorf("opcua: the server answered %d results for the write to %q", len(res.Results), subject)
	}
	if err := writeStatus(subject, res.Results[0]); err != nil {
		return err
	}
	m.store.log.Debug(ctx, "written", telemetry.Fields{"subject": subject})
	return nil
}

// writeStatus turns a Write result into the contract's error for it.
//
// Statuses retrying cannot change (the node is unknown, not writable, or not
// writable by this user, or the server refuses a write of this form) wrap
// [streams.ErrUnsupported], so a retrying publisher gives up on them at once.
// Every error wraps the status itself.
func writeStatus(subject string, status ua.StatusCode) error {
	if uint32(status)>>30 < 2 {
		// Good and Uncertain both mean the value was written.
		return nil
	}
	switch status {
	case ua.StatusBadNodeIDUnknown, ua.StatusBadNodeIDInvalid, ua.StatusBadNotWritable,
		ua.StatusBadUserAccessDenied, ua.StatusBadWriteNotSupported, ua.StatusBadAttributeIDInvalid:
		return fmt.Errorf("%w: opcua: the server refused the write to %q: %w", streams.ErrUnsupported, subject, status)
	}
	return fmt.Errorf("opcua: the server refused the write to %q: %w", subject, status)
}
