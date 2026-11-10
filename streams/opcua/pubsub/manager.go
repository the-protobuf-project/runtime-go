package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// manager publishes and subscribes the DataSetWriters one stream declares.
type manager struct {
	p      *Provider
	stream streams.Stream
	group  group
}

var (
	_ streams.Manager = (*manager)(nil)
	_ streams.Batch   = (*manager)(nil)
)

// split is a subject's DataSetWriter, and its field when it names one.
func split(subject string) (writer, field string, hasField bool) {
	return strings.Cut(subject, "/")
}

// checkSubject splits a subject the stream declares: literally, or as a
// field of a writer it declares whole.
func (m *manager) checkSubject(ctx context.Context, subject string) (writer, field string, err error) {
	writer, field, hasField := split(subject)
	declared := false
	for _, s := range m.stream.Subjects {
		if s == subject || hasField && s == writer {
			declared = true
			break
		}
	}
	if declared && writer != "" && (!hasField || field != "") {
		return writer, field, nil
	}
	m.p.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return "", "", core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// checkPublish is the writer and field a publish on subject goes to and the
// PUBLISH properties o asks for, or why nothing can be published with them.
func (m *manager) checkPublish(ctx context.Context, subject string, o streams.Options) (string, string, *paho.PublishProperties, error) {
	writer, field, err := m.checkSubject(ctx, subject)
	if err != nil {
		return "", "", nil, err
	}
	if refused := checkOwn(m.stream.ID, m.group); refused != nil {
		return "", "", nil, refused
	}
	if o.TTL > 0 {
		return "", "", nil, fmt.Errorf("%w: opcua pubsub: MQTT delivers on publish and cannot schedule a DataSetMessage", streams.ErrUnsupported)
	}
	if o.Retain {
		return "", "", nil, fmt.Errorf("%w: opcua pubsub: a ua-data NetworkMessage is not retained (Part 14 retains "+
			"ua-metadata, which PublishMetadata sends), and subscribers skip retained data as not published "+
			"after they joined", streams.ErrUnsupported)
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return "", "", nil, err
	}
	return writer, field, properties(headers, o.Expires), nil
}

// fields are the fields a value publishes: one field's value, or a whole
// DataSet.
func fields(field string, value any) ([]Field, error) {
	if field != "" {
		dv, err := DataValueOf(value)
		if err != nil {
			return nil, err
		}
		return []Field{{Name: field, Value: dv}}, nil
	}
	set, err := dataSetOf(value)
	if err != nil {
		return nil, err
	}
	return set.Fields, nil
}

// messages are DataSetMessages for datasets, numbered from first.
func (m *manager) messages(writer string, first uint32, datasets [][]Field) []DataSetMessage {
	version := m.p.version(m.group.name, writer)
	now := time.Now()
	id := WriterID(writer)
	out := make([]DataSetMessage, len(datasets))
	for n, payload := range datasets {
		seq := first + uint32(n)
		out[n] = DataSetMessage{
			DataSetWriterID:   id,
			DataSetWriterName: writer,
			SequenceNumber:    &seq,
			MetaDataVersion:   version,
			Timestamp:         now,
			Payload:           payload,
		}
	}
	return out
}

// network wraps messages in a NetworkMessage headed with this group's
// publisher and name.
func (m *manager) network(id string, messages []DataSetMessage) *NetworkMessage {
	return &NetworkMessage{
		MessageID:       id,
		PublisherID:     m.group.publisher,
		WriterGroupName: m.group.name,
		Messages:        messages,
	}
}

// Publish sends one NetworkMessage holding one DataSetMessage on the
// writer's data topic, never retained, and returns once the broker has
// acknowledged it. The id returned is the NetworkMessage's MessageId, and it
// travels.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	o := streams.NewOptions(opts...)
	writer, field, props, err := m.checkPublish(ctx, subject, o)
	if err != nil {
		return "", err
	}
	payload, err := fields(field, value)
	if err != nil {
		return "", err
	}
	id := o.ID
	if id == "" {
		id = core.NewID()
	}
	first := m.p.reserve(m.group.name, writer, 1)
	body, err := m.network(id, m.messages(writer, first, [][]Field{payload})).JSON()
	if err != nil {
		return "", err
	}
	topic := m.p.topic("data", m.group, writer)
	if err := m.p.send(ctx, &paho.Publish{Topic: topic, QoS: m.p.cfg.qos, Properties: props, Payload: body}); err != nil {
		m.p.log.Error(ctx, "opcua pubsub: could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("opcua pubsub: cannot publish on %q: %w", subject, err)
	}
	m.p.log.Debug(ctx, "opcua pubsub: published", telemetry.Fields{
		"subject": subject, "id": id, "sequence": first, "bytes": len(body),
	})
	return id, nil
}

// PublishBatch publishes the values as DataSetMessages of one
// NetworkMessage, or of several when there are more than the configured
// maximum, in order. Each entry's id is `<MessageId>.<n>`, n counting the
// DataSetMessages of its NetworkMessage; a NetworkMessage holding one is
// just its MessageId. A value that is not one this subject takes fails
// alone; a NetworkMessage the broker refuses fails every entry in it.
//
// The NetworkMessages are sent one after another, each acknowledged before
// the next, so the writer's sequence reaches the broker in order.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	writer, field, props, err := m.checkPublish(ctx, subject, o)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(values))
	failed := make([]error, len(values))
	type entry struct {
		index  int
		fields []Field
	}
	good := make([]entry, 0, len(values))
	for i, v := range values {
		f, err := fields(field, v)
		if err != nil {
			ids[i] = core.NewID()
			failed[i] = fmt.Errorf("entry %d: %w", i, err)
			continue
		}
		good = append(good, entry{i, f})
	}

	sequence := m.p.reserve(m.group.name, writer, uint32(len(good)))
	topic := m.p.topic("data", m.group, writer)
	for start := 0; start < len(good); start += m.p.cfg.maxMessages {
		part := good[start:min(start+m.p.cfg.maxMessages, len(good))]
		id := core.NewID()
		datasets := make([][]Field, len(part))
		for n, e := range part {
			datasets[n] = e.fields
			ids[e.index] = id
			if len(part) > 1 {
				ids[e.index] = id + "." + strconv.Itoa(n)
			}
		}
		body, err := m.network(id, m.messages(writer, sequence, datasets)).JSON()
		sequence += uint32(len(part))
		if err == nil {
			err = m.p.send(ctx, &paho.Publish{Topic: topic, QoS: m.p.cfg.qos, Properties: props, Payload: body})
			if err != nil {
				err = fmt.Errorf("opcua pubsub: cannot publish on %q: %w", subject, err)
			}
		}
		if err != nil {
			for _, e := range part {
				failed[e.index] = fmt.Errorf("entry %d: %w", e.index, err)
			}
		}
	}

	var failures []error
	for _, err := range failed {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		m.p.log.Error(ctx, "opcua pubsub: could not publish every message of a batch", failures[0], telemetry.Fields{
			"subject": subject, "failed": len(failures), "total": len(values),
		})
	} else {
		m.p.log.Debug(ctx, "opcua pubsub: published a batch", telemetry.Fields{"subject": subject, "count": len(values)})
	}
	return ids, core.BatchError(subject, len(values), failures)
}

// Subscribe subscribes to the writer's topic and to the writer-less group
// topic, where a DataSetMessage names its writer, on a clean connection of
// its own, and returns once the broker has granted both.
//
// Every field of every DataSetMessage of the writer arrives as its own
// message on `<writer>/<field>`, coded [CodecName]; a subscription to
// `<writer>/<field>` delivers that field only. The fields of one
// DataSetMessage share its id. Retained data, which was not published after
// the subscription, is skipped, as are keep-alives.
//
// [streams.Group] is an MQTT shared subscription, `$share/<group>/...`: the
// broker hands each NetworkMessage to one member. Each member then sees a
// share of the sequence, so gaps are not tracked for a group.
//
// A lost connection closes the channel rather than redialing: a clean
// session dialed again has lost what was published meanwhile, and an ended
// stream says so.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	writer, field, err := m.checkSubject(ctx, subject)
	if err != nil {
		return nil, err
	}
	o := streams.NewOptions(opts...)
	topics := []string{m.p.topic("data", m.group, writer), m.p.topic("data", m.group, "")}
	filters := make([]string, len(topics))
	for i, t := range topics {
		filters[i] = t
		if o.Group != "" {
			filters[i] = "$share/" + o.Group + "/" + t
		}
	}

	out := make(chan streams.Message, core.Prefetch(o))
	r := &reader{
		p: m.p, writer: writer, writerID: WriterID(writer), field: field,
		groupTopic: topics[1], track: o.Group == "",
		last: map[string]uint32{}, subjects: map[string]string{},
		out: out, ctx: ctx, stop: make(chan struct{}),
	}
	cm, err := m.p.dial(ctx, connection{
		clientID:  "runtime-go-pubsub-sub-" + core.NewID(),
		onPublish: r.receive,
		onDown:    r.end,
	})
	if err != nil {
		return nil, fmt.Errorf("opcua pubsub: cannot subscribe to %q: %w", subject, err)
	}
	if err := m.p.subscribeAll(ctx, cm, filters); err != nil {
		disconnect(cm)
		return nil, fmt.Errorf("opcua pubsub: cannot subscribe to %q: %w", subject, err)
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-r.stop:
		}
		r.end()
		// ctx is done by now; the unsubscribe still gets its moment.
		unsub, cancel := context.WithTimeout(context.WithoutCancel(ctx), linger)
		_, _ = cm.Unsubscribe(unsub, &paho.Unsubscribe{Topics: filters})
		cancel()
		disconnect(cm)
	}()
	m.p.log.Debug(ctx, "opcua pubsub: subscribed", telemetry.Fields{"subject": subject, "stream": m.stream.ID})
	return out, nil
}

// step is what a SequenceNumber says about the one before it.
type step int

const (
	// inOrder is the first seen, or the next.
	inOrder step = iota
	// gap means messages were missed.
	gap
	// repeat is the last one again: a QoS 1 redelivery.
	repeat
	// restart is behind the last: the publisher restarted and counts again.
	restart
)

// stepOf compares sequence with the last one seen, in UInt32 arithmetic that
// wraps, as Part 14's sequence numbers do, and says how many were missed.
func stepOf(last *uint32, sequence uint32) (step, uint32) {
	if last == nil {
		return inOrder, 0
	}
	switch d := int32(sequence - *last); {
	case d == 1:
		return inOrder, 0
	case d == 0:
		return repeat, 0
	case d > 1:
		return gap, uint32(d - 1)
	default:
		return restart, 0
	}
}

// reader turns one subscription's NetworkMessages into messages.
type reader struct {
	p        *Provider
	writer   string
	writerID uint16
	// field is the one field delivered, or empty for all of them.
	field string
	// groupTopic is the writer-less topic, where a DataSetMessage names its
	// writer.
	groupTopic string
	track      bool
	// last is the last SequenceNumber per publisher. Only paho's reading
	// goroutine touches it.
	last map[string]uint32
	// subjects is `<writer>/<field>` per field, built once.
	subjects map[string]string

	ctx  context.Context
	out  chan streams.Message
	stop chan struct{}

	// mu is held while a message is handed over, so the channel is never
	// closed under a send.
	mu       sync.Mutex
	closed   bool
	stopOnce sync.Once
}

// end closes the channel once, whichever of the context, a lost connection
// or the close comes first.
func (r *reader) end() {
	r.stopOnce.Do(func() { close(r.stop) })
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		close(r.out)
	}
}

// ours reports whether a DataSetMessage that arrived is this writer's.
func (r *reader) ours(m *DataSetMessage, onGroupTopic bool) bool {
	switch {
	case m.DataSetWriterName != "":
		return m.DataSetWriterName == r.writer
	case m.DataSetWriterID != 0 && onGroupTopic:
		return m.DataSetWriterID == r.writerID
	default:
		// The writer's own topic names it.
		return !onGroupTopic
	}
}

// receive hands this writer's DataSetMessages from one PUBLISH to the
// subscriber, dropping retained, malformed, keep-alive and repeated ones.
func (r *reader) receive(pr paho.PublishReceived) (bool, error) {
	msgs := r.convert(pr.Packet)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, msg := range msgs {
		if r.closed {
			return true, nil
		}
		select {
		case r.out <- msg:
		case <-r.ctx.Done():
			return true, nil
		case <-r.stop:
			return true, nil
		}
	}
	return true, nil
}

// convert is the messages one PUBLISH yields.
func (r *reader) convert(pub *paho.Publish) []streams.Message {
	if pub.Retain {
		// Retained before this subscription: not published after it.
		return nil
	}
	network, err := ParseNetworkMessage(pub.Payload)
	if err != nil {
		r.p.log.Warn(r.ctx, "opcua pubsub: dropping a malformed message", telemetry.Fields{"writer": r.writer, "error": err.Error()})
		return nil
	}
	onGroupTopic := pub.Topic == r.groupTopic
	// The NetworkMessage's MQTT 5 user properties, on every message it
	// yields.
	headers := headersOf(pub)
	id := network.MessageID
	if id == "" {
		id = core.NewID()
	}
	several := len(network.Messages) > 1
	var out []streams.Message
	for i := range network.Messages {
		m := &network.Messages[i]
		if m.IsKeepalive() || !r.ours(m, onGroupTopic) {
			continue
		}
		if r.track && m.SequenceNumber != nil {
			publisher := network.PublisherOf(m)
			seq := *m.SequenceNumber
			var last *uint32
			if l, ok := r.last[publisher]; ok {
				last = &l
			}
			switch s, missed := stepOf(last, seq); s {
			case repeat:
				r.p.log.Debug(r.ctx, "opcua pubsub: dropping a repeated DataSetMessage", telemetry.Fields{"writer": r.writer, "sequence": seq})
				continue
			case gap:
				r.p.log.Warn(r.ctx, "opcua pubsub: DataSetMessages are missing from the sequence", telemetry.Fields{
					"publisher": publisher, "writer": r.writer, "sequence": seq, "missed": missed,
				})
				r.p.recordGap(publisher, r.writer, missed)
			case restart:
				r.p.log.Info(r.ctx, "opcua pubsub: the writer's sequence started again", telemetry.Fields{
					"publisher": publisher, "writer": r.writer, "sequence": seq,
				})
			}
			r.last[publisher] = seq
		}
		messageID := id
		if several {
			messageID = id + "." + strconv.Itoa(i)
		}
		for _, f := range m.Payload {
			if r.field != "" && r.field != f.Name {
				continue
			}
			subject, ok := r.subjects[f.Name]
			if !ok {
				subject = r.writer + "/" + f.Name
				r.subjects[f.Name] = subject
			}
			dv := f.Value
			if dv == nil {
				dv = &ua.DataValue{}
			}
			data, err := encodeDataValue(dv)
			if err != nil {
				r.p.log.Warn(r.ctx, "opcua pubsub: dropping a field that does not encode", telemetry.Fields{
					"writer": r.writer, "field": f.Name, "error": err.Error(),
				})
				continue
			}
			msg := streams.NewMessage(messageID, subject, data, DataValueCodec)
			if len(headers) > 0 {
				msg.Headers = headers.Clone()
			}
			out = append(out, msg)
		}
	}
	return out
}
