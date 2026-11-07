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

// node resolves a subject to the node id it was declared as.
func (m *manager) node(ctx context.Context, subject string) (*ua.NodeID, error) {
	if n, ok := m.declared.nodes[subject]; ok {
		return n, nil
	}
	s := m.declared.stream
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": s.ID, "declared": s.Subjects,
	})
	return nil, core.ErrSubject(s.ID, subject, s.Subjects)
}

// Publish writes a value to the subject's node.
//
// The id is returned either way, but travels with the value only when it went
// out framed; a native value is identified by its timestamp instead. See the
// package documentation.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	ids, err := m.write(ctx, subject, []any{value}, streams.NewOptions(opts...))
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// PublishBatch writes several values to the subject's node in one request.
//
// OPC UA's Write takes a list, so this is one round trip however many values
// there are. They land in order, and a subscriber sees each only if the
// monitored item's queue holds them all — this is a series of states, and the
// last is what the node keeps.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	return m.write(ctx, subject, values, o)
}

// write sends values to one node in a single Write request and reports any
// that the server refused, by index.
func (m *manager) write(ctx context.Context, subject string, values []any, o streams.Options) ([]string, error) {
	node, err := m.node(ctx, subject)
	if err != nil {
		return nil, err
	}
	if o.TTL > 0 {
		return nil, fmt.Errorf("%w: OPC UA writes a value now and cannot schedule one", streams.ErrUnsupported)
	}

	if len(values) == 0 {
		return []string{}, nil
	}

	ids := make([]string, len(values))
	req := &ua.WriteRequest{NodesToWrite: make([]*ua.WriteValue, len(values))}
	for i, value := range values {
		ids[i] = o.ID
		if ids[i] == "" {
			ids[i] = core.NewID()
		}
		v, eerr := encode(m.store.codec, ids[i], value)
		if eerr != nil {
			return ids, fmt.Errorf("opcua: entry %d on %q: %w", i, subject, eerr)
		}
		req.NodesToWrite[i] = &ua.WriteValue{
			NodeID:      node,
			AttributeID: ua.AttributeIDValue,
			Value:       &ua.DataValue{EncodingMask: ua.DataValueValue, Value: v},
		}
	}

	res, err := m.store.client.Write(ctx, req)
	if err != nil {
		m.store.log.Error(ctx, "could not write", err, telemetry.Fields{"subject": subject, "count": len(values)})
		return ids, fmt.Errorf("opcua: cannot write to %q: %w", subject, err)
	}
	if len(res.Results) != len(values) {
		// Without one status per value there is no saying which landed.
		return ids, fmt.Errorf("opcua: the server answered %d statuses for %d values written to %q", len(res.Results), len(values), subject)
	}

	var failures []error
	for i, status := range res.Results {
		if status != ua.StatusOK {
			failures = append(failures, fmt.Errorf("entry %d: server answered %w", i, status))
		}
	}
	if len(values) == 1 && len(failures) == 1 {
		// One value is a publish, not a batch, and reads better without the
		// batch's framing around its one failure.
		return ids, fmt.Errorf("opcua: cannot write to %q: server answered %w", subject, res.Results[0])
	}
	if err := core.BatchError(subject, len(values), failures); err != nil {
		return ids, err
	}

	m.store.log.Debug(ctx, "written", telemetry.Fields{"subject": subject, "count": len(values)})
	return ids, nil
}
