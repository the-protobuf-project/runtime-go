package pubsub

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Provider is streams over OPC UA PubSub on an MQTT broker. Build one with
// [Connect]; it is safe for concurrent use.
type Provider struct {
	cfg    config
	server *url.URL
	log    telemetry.Logger
	links  []*autopaho.ConnectionManager
	closed atomic.Bool

	mu       sync.RWMutex
	declared map[string]streams.Stream

	// counters guards sequences and versions, both keyed by one of this
	// provider's own writers. No publisher in the key, because a provider
	// publishes under exactly one PublisherId: a stream naming another is
	// read-only, and both Publish and PublishMetadata refuse it (checkOwn).
	counters  sync.Mutex
	sequences map[writerKey]uint32
	versions  map[writerKey]ua.ConfigurationVersionDataType

	gaps       atomic.Uint64
	gapCounter telemetry.Counter
}

// writerKey is a writer of one of this provider's WriterGroups.
type writerKey struct{ group, writer string }

// group is the WriterGroup a stream is, and whose it is.
type group struct {
	publisher string
	name      string
	// own is whether this provider is the group's publisher.
	own bool
}

var (
	_ streams.Streams = (*Provider)(nil)
	_ streams.Closer  = (*Provider)(nil)
)

// Connect dials the publishing connections to the broker at address and
// returns a provider publishing under its PublisherId.
//
// The address is host:port, or a URL — mqtt://, tls://, mqtts://, ws:// or
// wss://. Every connection is made before Connect returns, so a broker that
// is unreachable or refuses the credentials fails it; after that each
// publishing connection keeps itself up. The provider owns them, so it must
// be closed.
func Connect(ctx context.Context, address string, opts ...Option) (*Provider, error) {
	cfg := newConfig(opts...)
	server, err := parseServer(address, cfg)
	if err != nil {
		return nil, err
	}
	if err := checkLevel("the PublisherId", cfg.publisherID); err != nil {
		return nil, err
	}
	if cfg.prefix == "" || strings.ContainsAny(cfg.prefix, "+#") {
		return nil, fmt.Errorf("%w: opcua pubsub: the topic prefix %q must be non-empty and free of wildcards",
			streams.ErrUnsupported, cfg.prefix)
	}
	if cfg.clientID == "" {
		cfg.clientID = "runtime-go-pubsub-" + core.NewID()
	}

	p := &Provider{
		cfg: cfg, server: server, log: cfg.log,
		declared:  map[string]streams.Stream{},
		sequences: map[writerKey]uint32{},
		versions:  map[writerKey]ua.ConfigurationVersionDataType{},
		gapCounter: cfg.meter.Counter("streams_opcua_pubsub_sequence_gaps_total",
			telemetry.WithDescription("DataSetMessages a subscriber missed, by SequenceNumber"),
			telemetry.WithUnit("1")),
	}
	for n := 0; n < cfg.connections; n++ {
		id := cfg.clientID
		if n > 0 {
			id += "-" + strconv.Itoa(n)
		}
		cm, err := p.dial(ctx, connection{clientID: id, redial: true})
		if err != nil {
			for _, open := range p.links {
				disconnect(open)
			}
			return nil, err
		}
		p.links = append(p.links, cm)
	}
	return p, nil
}

// PublisherID is the PublisherId this provider sends under.
func (p *Provider) PublisherID() string { return p.cfg.publisherID }

// SequenceGaps is how many DataSetMessages this provider's subscriptions
// found missing from a writer's sequence since it was built.
func (p *Provider) SequenceGaps() uint64 { return p.gaps.Load() }

// DataTopic is the topic a stream's writer publishes data on.
func (p *Provider) DataTopic(stream, writer string) (string, error) {
	s, err := p.Get(context.Background(), stream)
	if err != nil {
		return "", err
	}
	return p.topic("data", p.groupOf(s.ID), writer), nil
}

// PublishMetadata announces a writer's DataSetMetaData as a retained
// ua-metadata message on its metadata topic, so a reader that arrives later
// still learns the fields' names and types, and stamps the writer's
// DataSetMessages with its ConfigurationVersion from then on. A version of
// zero is replaced with the current time, as Part 14's VersionTime counts
// it. [DataSet.Metadata] builds one from a DataSet's values.
//
// The stream's own headers are stamped on the message as user properties.
func (p *Provider) PublishMetadata(ctx context.Context, stream, writer string, meta ua.DataSetMetaDataType) (ua.ConfigurationVersionDataType, error) {
	s, err := p.Get(ctx, stream)
	if err != nil {
		return ua.ConfigurationVersionDataType{}, err
	}
	g := p.groupOf(s.ID)
	if refused := checkOwn(s.ID, g); refused != nil {
		return ua.ConfigurationVersionDataType{}, refused
	}
	known := false
	for _, subject := range s.Subjects {
		if w, _, _ := strings.Cut(subject, "/"); subject == writer || w == writer {
			known = true
			break
		}
	}
	if !known {
		return ua.ConfigurationVersionDataType{}, core.ErrSubject(s.ID, writer, s.Subjects)
	}
	if meta.ConfigurationVersion == nil || meta.ConfigurationVersion.MajorVersion == 0 {
		now := versionTime(time.Now())
		meta.ConfigurationVersion = &ua.ConfigurationVersionDataType{MajorVersion: now, MinorVersion: now}
	}
	version := *meta.ConfigurationVersion
	message := MetaDataMessage{
		MessageID:         core.NewID(),
		PublisherID:       g.publisher,
		DataSetWriterID:   WriterID(writer),
		WriterGroupName:   g.name,
		DataSetWriterName: writer,
		Timestamp:         time.Now(),
		MetaData:          meta,
	}
	body, err := message.JSON()
	if err != nil {
		return ua.ConfigurationVersionDataType{}, err
	}
	topic := p.topic("metadata", g, writer)
	if err := p.send(ctx, &paho.Publish{
		Topic: topic, QoS: p.cfg.qos, Retain: true,
		Properties: properties(s.Headers, 0), Payload: body,
	}); err != nil {
		return ua.ConfigurationVersionDataType{}, fmt.Errorf("opcua pubsub: cannot publish metadata on %q: %w", topic, err)
	}
	p.counters.Lock()
	p.versions[writerKey{g.name, writer}] = version
	p.counters.Unlock()
	p.log.Info(ctx, "opcua pubsub: metadata published", telemetry.Fields{"stream": stream, "writer": writer, "topic": topic})
	return version, nil
}

// Metadata reads the retained DataSetMetaData of a stream's writer, failing
// with an error wrapping [streams.ErrNotFound] when the broker holds none
// within the metadata timeout.
func (p *Provider) Metadata(ctx context.Context, stream, writer string) (MetaDataMessage, error) {
	s, err := p.Get(ctx, stream)
	if err != nil {
		return MetaDataMessage{}, err
	}
	topic := p.topic("metadata", p.groupOf(s.ID), writer)
	got := make(chan *paho.Publish, 1)
	cm, err := p.dial(ctx, connection{
		clientID: "runtime-go-pubsub-meta-" + core.NewID(),
		onPublish: func(r paho.PublishReceived) (bool, error) {
			select {
			case got <- r.Packet:
			default:
			}
			return true, nil
		},
	})
	if err != nil {
		return MetaDataMessage{}, fmt.Errorf("opcua pubsub: cannot read %q: %w", topic, err)
	}
	defer disconnect(cm)
	if err := p.subscribeAll(ctx, cm, []string{topic}); err != nil {
		return MetaDataMessage{}, fmt.Errorf("opcua pubsub: cannot read %q: %w", topic, err)
	}
	wait, cancel := context.WithTimeout(ctx, p.cfg.metadataTimeout)
	defer cancel()
	select {
	case msg := <-got:
		return ParseMetaDataMessage(msg.Payload)
	case <-wait.Done():
		if ctx.Err() != nil {
			return MetaDataMessage{}, ctx.Err()
		}
		return MetaDataMessage{}, fmt.Errorf("%w: opcua pubsub: no metadata is retained on %q", streams.ErrNotFound, topic)
	}
}

// versionTime is Part 14's VersionTime: seconds since 2000-01-01T00:00:00Z.
func versionTime(now time.Time) uint32 {
	secs := now.Unix() - 946_684_800
	switch {
	case secs < 0:
		return 0
	case secs > int64(^uint32(0)):
		return ^uint32(0)
	}
	return uint32(secs)
}

// WriterID is a DataSetWriter's numeric id: 32-bit FNV-1a over its name,
// folded to 16 bits, never 0 (which Part 14 reserves). Two processes
// declaring the same writer agree on it without coordination, and it is the
// id runtime-rs gives the same name.
func WriterID(name string) uint16 {
	hash := uint32(0x811c9dc5)
	for i := 0; i < len(name); i++ {
		hash = (hash ^ uint32(name[i])) * 0x01000193
	}
	if id := uint16((hash >> 16) ^ (hash & 0xffff)); id != 0 {
		return id
	}
	return 1
}

// groupOf is the WriterGroup a declared stream id names: `<group>` on this
// publisher, or `<publisher>/<group>`.
func (p *Provider) groupOf(id string) group {
	if publisher, name, ok := strings.Cut(id, "/"); ok {
		return group{publisher: publisher, name: name, own: publisher == p.cfg.publisherID}
	}
	return group{publisher: p.cfg.publisherID, name: id, own: true}
}

// topic is `<prefix>/json/<kind>/<publisher>/<group>[/<writer>]`.
func (p *Provider) topic(kind string, g group, writer string) string {
	t := p.cfg.prefix + "/json/" + kind + "/" + g.publisher + "/" + g.name
	if writer != "" {
		t += "/" + writer
	}
	return t
}

// reserve reserves n consecutive SequenceNumbers for a writer and returns
// the first, counting in UInt32 arithmetic that wraps, as Part 14's do.
func (p *Provider) reserve(g, writer string, n uint32) uint32 {
	p.counters.Lock()
	defer p.counters.Unlock()
	key := writerKey{g, writer}
	last := p.sequences[key]
	p.sequences[key] = last + n
	return last + 1
}

// version is the ConfigurationVersion last announced for a writer through
// PublishMetadata, if any.
func (p *Provider) version(g, writer string) *ua.ConfigurationVersionDataType {
	p.counters.Lock()
	defer p.counters.Unlock()
	if v, ok := p.versions[writerKey{g, writer}]; ok {
		return &v
	}
	return nil
}

// recordGap adds to the in-process gap total and the gap counter.
func (p *Provider) recordGap(publisher, writer string, missed uint32) {
	p.gaps.Add(uint64(missed))
	p.gapCounter.Add(context.Background(), float64(missed), telemetry.Labels{"publisher": publisher, "writer": writer})
}

// checkLevel refuses a topic level that is empty or carries MQTT structure.
func checkLevel(what, level string) error {
	if level == "" || strings.ContainsAny(level, "/+#") {
		return fmt.Errorf("%w: opcua pubsub: %s %q is a topic level, so it must be non-empty and free of '/', '+' and '#'",
			streams.ErrUnsupported, what, level)
	}
	return nil
}

// checkOwn refuses publishing as a publisher this provider is not.
func checkOwn(stream string, g group) error {
	if g.own {
		return nil
	}
	return fmt.Errorf("%w: opcua pubsub: stream %q is publisher %q's WriterGroup, and publishing on it "+
		"would send as that publisher; subscribe to it, or declare a stream of your own",
		streams.ErrUnsupported, stream, g.publisher)
}

// declaration checks a stream's id, subjects and default headers, and
// returns it stored under id, active.
func declaration(id string, s streams.Stream) (streams.Stream, error) {
	if err := s.Headers.Validate(); err != nil {
		return streams.Stream{}, err
	}
	if publisher, name, ok := strings.Cut(id, "/"); ok {
		if err := checkLevel("the PublisherId", publisher); err != nil {
			return streams.Stream{}, err
		}
		if err := checkLevel("the WriterGroup", name); err != nil {
			return streams.Stream{}, err
		}
	} else if err := checkLevel("the WriterGroup", id); err != nil {
		return streams.Stream{}, err
	}
	ids := map[uint16]string{}
	for _, subject := range s.Subjects {
		writer, field, hasField := strings.Cut(subject, "/")
		if err := checkLevel("the DataSetWriter", writer); err != nil {
			return streams.Stream{}, err
		}
		if hasField && (field == "" || strings.ContainsAny(field, "+#")) {
			return streams.Stream{}, fmt.Errorf("%w: opcua pubsub: the field of subject %q must be non-empty and free of wildcards",
				streams.ErrUnsupported, subject)
		}
		wid := WriterID(writer)
		if other, ok := ids[wid]; ok && other != writer {
			return streams.Stream{}, fmt.Errorf("%w: opcua pubsub: the DataSetWriters %q and %q hash to the same DataSetWriterId %d; rename one",
				streams.ErrUnsupported, other, writer, wid)
		}
		ids[wid] = writer
	}
	s.ID, s.Active = id, true
	return s, nil
}

// safeName replaces what MQTT reads as structure in a generated id.
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '+' || r == '#' {
			return '-'
		}
		return r
	}, s)
}

func notFound(id string) error {
	return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
}

// Create checks the id and subjects as topic levels and refuses two writers
// whose ids collide. Declarations are local, as on MQTT: the broker keeps no
// stream catalog.
func (p *Provider) Create(ctx context.Context, s streams.Stream) (streams.Stream, error) {
	id := s.ID
	if id == "" {
		id = safeName(core.NewStreamID(s.Name))
	}
	out, err := declaration(id, s)
	if err != nil {
		return streams.Stream{}, err
	}
	p.mu.Lock()
	p.declared[id] = out
	p.mu.Unlock()
	p.log.Info(ctx, "opcua pubsub: stream declared", telemetry.Fields{"stream": id, "subjects": out.Subjects})
	return out, nil
}

// Get reads the in-process declaration.
func (p *Provider) Get(_ context.Context, id string) (streams.Stream, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.declared[id]
	if !ok {
		return streams.Stream{}, notFound(id)
	}
	return s, nil
}

// Bind returns a manager over a copy of the declaration taken now.
func (p *Provider) Bind(ctx context.Context, id string) (streams.Manager, error) {
	s, err := p.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &manager{p: p, stream: s, group: p.groupOf(s.ID)}, nil
}

// Update revalidates as Create does; managers already bound keep the old
// declaration.
func (p *Provider) Update(_ context.Context, id string, s streams.Stream) (streams.Stream, error) {
	out, err := declaration(id, s)
	if err != nil {
		return streams.Stream{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.declared[id]; !ok {
		return streams.Stream{}, notFound(id)
	}
	p.declared[id] = out
	return out, nil
}

// Delete forgets the declaration; managers already bound and their
// subscriptions are untouched.
func (p *Provider) Delete(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.declared[id]; !ok {
		return notFound(id)
	}
	delete(p.declared, id)
	return nil
}

// List returns the streams this process declared, in id order.
func (p *Provider) List(context.Context) ([]streams.Stream, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]streams.Stream, 0, len(p.declared))
	for _, s := range p.declared {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Close disconnects the publishing connections. Subscriptions end with their
// contexts, so cancel those first.
func (p *Provider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	var errs []error
	for _, cm := range p.links {
		ctx, cancel := context.WithTimeout(context.Background(), linger)
		if err := cm.Disconnect(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			errs = append(errs, err)
		}
		cancel()
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("opcua pubsub: cannot disconnect: %w", err)
	}
	return nil
}
