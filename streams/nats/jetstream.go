package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// JetStream keeps a stream's name, subjects and description of its own accord,
// but has nowhere in its config for the rest of [streams.Stream]. These carry
// the remainder in the stream's metadata, where they survive a restart with it.
const (
	metaName   = "runtime-go.name"
	metaUser   = "runtime-go.user_id"
	metaActive = "runtime-go.active"

	// metaHeaders holds [streams.Stream.Headers] as one JSON object
	// ({"lang":"go"}), and is absent when there are none, so a stream without
	// headers has exactly the metadata it always had. One key rather than one
	// per header keeps the set whole: an update replaces it and never leaves a
	// removed header behind. runtime-rs reads and writes the same key.
	metaHeaders = "runtime-go.headers"
)

// The publish path. A JetStream publish is sent with [jetstream.JetStream.PublishMsgAsync],
// whose acks all come back on one wildcard reply subscription, rather than as
// a request of its own; this is what runtime-rs does too, and what keeps many
// publishers from meeting on per-request locks.
const (
	// ackTimeout is how long a publish waits for its PubAck before it fails.
	ackTimeout = 5 * time.Second

	// maxPending is how many publishes may await their PubAcks through one
	// provider at once, nats.go's PublishAsyncMaxPending default. Above
	// roughly ten thousand a stream's inbound queue overflows and the server
	// answers 429. The server sets no per-connection limit and its 429 is per
	// stream, so the limit belongs to the provider, not the connection.
	maxPending = 4000

	// chunk is how many permits under maxPending a batch takes at a time, so
	// that publishers on different goroutines meet on the budget once per
	// chunk rather than once per message.
	chunk = 64

	// batchInFlight is how many publishes of one batch await their acks at
	// once.
	batchInFlight = 1024
)

// errClosed is what a publish on a closed provider, or one still awaiting its
// ack when the provider closed, reports.
var errClosed = errors.New("nats: the JetStream provider is closed")

// jsStreams is JetStream: the same subjects as core NATS, backed by a log.
type jsStreams struct {
	js       jetstream.JetStream
	codec    streams.Codec
	registry *streams.Registry
	metrics  *core.Metrics
	log      telemetry.Logger

	// owned is the connection this package dialed, if it did. One handed in
	// through UseJetStream belongs to the caller and is left alone.
	owned *gonats.Conn

	// Zero leaves ackWait and maxDeliver to the server.
	ackWait           time.Duration
	maxDeliver        int
	inactiveThreshold time.Duration

	// budget bounds the publishes awaiting their acks to maxPending, across
	// every manager bound from this provider.
	budget *budget

	// closed is closed by Close. A publish refuses to start after it, and one
	// awaiting its ack stops waiting, so none is left waiting on a provider
	// its caller has finished with.
	closed    chan struct{}
	closeOnce sync.Once
}

var (
	_ streams.Streams = (*jsStreams)(nil)
	_ streams.Closer  = (*jsStreams)(nil)
)

// Close releases the connection, if this package made it.
//
// On a provider built by [UseJetStream] the connection is left alone, so a
// caller may close either kind without having to remember which it has. On
// both, a publish still awaiting its ack fails at once and a later one is
// refused: nothing is left waiting on a provider that is gone.
func (p *jsStreams) Close() error {
	p.closeOnce.Do(func() { close(p.closed) })
	if p.owned != nil {
		p.owned.Close()
	}
	return nil
}

// Create declares a stream, generating an id when one is not supplied.
//
// Every stream it creates allows per-message TTLs, so [streams.Expires] works
// on it (NATS 2.11 or later). Default headers that are invalid or use the
// server's Nats- prefix are refused with [streams.ErrInvalidHeader].
func (p *jsStreams) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	if err := checkStreamHeaders(in); err != nil {
		return streams.Stream{}, err
	}
	id := in.ID
	if id == "" {
		id = core.NewStreamID(in.Name)
	}
	id = safeName(id)

	p.log.Debug(ctx, "creating stream", telemetry.Fields{
		"id": id, "name": in.Name, "subjects": in.Subjects,
	})

	if _, err := p.js.CreateStream(ctx, p.configFor(id, in)); err != nil {
		p.log.Error(ctx, "could not create the stream", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("nats: cannot create stream %s: %w", id, err)
	}

	p.log.Info(ctx, "stream created", telemetry.Fields{"id": id, "name": in.Name})
	return p.streamFrom(id, in), nil
}

// Get retrieves a stream by id.
func (p *jsStreams) Get(ctx context.Context, id string) (streams.Stream, error) {
	s, err := p.js.Stream(ctx, safeName(id))
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("nats: cannot read stream %s: %w", id, err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("nats: cannot read stream %s: %w", id, err)
	}
	return p.read(ctx, &info.Config), nil
}

// Bind returns a publisher and subscriber for an existing stream.
//
// The stream is read here so an unknown id fails at Bind rather than at the
// first publish, and so the subject list is available to validate against
// without a round trip on every call.
func (p *jsStreams) Bind(ctx context.Context, id string) (streams.Manager, error) {
	name := safeName(id)

	handle, err := p.js.Stream(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return nil, fmt.Errorf("nats: cannot bind stream %s: %w", id, err)
	}

	info, err := handle.Info(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return nil, fmt.Errorf("nats: cannot bind stream %s: %w", id, err)
	}

	stream := p.read(ctx, &info.Config)
	p.log.Debug(ctx, "bound to stream", telemetry.Fields{"id": name, "subjects": stream.Subjects})
	return &jsManager{p: p, handle: handle, stream: stream, allowTTL: info.Config.AllowMsgTTL}, nil
}

// Update replaces a stream's configuration, preserving its id.
//
// It turns per-message TTLs on, as Create does. The server never turns them
// back off, so it refuses an update that leaves them unset, as one from an
// older runtime-go would.
func (p *jsStreams) Update(ctx context.Context, id string, in streams.Stream) (streams.Stream, error) {
	if err := checkStreamHeaders(in); err != nil {
		return streams.Stream{}, err
	}
	name := safeName(id)

	if _, err := p.js.UpdateStream(ctx, p.configFor(name, in)); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("nats: cannot update stream %s: %w", id, err)
	}

	p.log.Info(ctx, "stream updated", telemetry.Fields{"id": name})
	return p.streamFrom(name, in), nil
}

// Delete removes a stream.
func (p *jsStreams) Delete(ctx context.Context, id string) error {
	if err := p.js.DeleteStream(ctx, safeName(id)); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return fmt.Errorf("nats: cannot delete stream %s: %w", id, err)
	}

	p.log.Info(ctx, "stream deleted", telemetry.Fields{"id": id})
	return nil
}

// List returns every stream on the server.
func (p *jsStreams) List(ctx context.Context) ([]streams.Stream, error) {
	lister := p.js.ListStreams(ctx)

	var out []streams.Stream
	for info := range lister.Info() {
		out = append(out, p.read(ctx, &info.Config))
	}
	if err := lister.Err(); err != nil {
		p.log.Error(ctx, "could not list streams", err, nil)
		return nil, fmt.Errorf("nats: cannot list streams: %w", err)
	}

	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	p.log.Debug(ctx, "listed streams", telemetry.Fields{"count": len(out)})
	return out, nil
}

// configFor builds the JetStream config for a declaration.
func (p *jsStreams) configFor(name string, in streams.Stream) jetstream.StreamConfig {
	metadata := map[string]string{
		metaName:   in.Name,
		metaUser:   in.UserID,
		metaActive: "true",
	}
	if len(in.Headers) > 0 {
		// A string map always marshals, with its keys sorted.
		encoded, _ := json.Marshal(in.Headers)
		metadata[metaHeaders] = string(encoded)
	}
	return jetstream.StreamConfig{
		Name:        name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		Metadata:    metadata,
		AllowMsgTTL: true,
	}
}

// read maps a JetStream config back to the contract's shape, warning about
// stream headers another tool wrote that do not parse rather than making the
// stream unreadable.
func (p *jsStreams) read(ctx context.Context, cfg *jetstream.StreamConfig) streams.Stream {
	if _, err := storedHeaders(cfg); err != nil {
		p.log.Warn(ctx, "ignoring unreadable stream headers",
			telemetry.Fields{"stream": cfg.Name, "error": err.Error()})
	}
	return toStream(cfg)
}

// streamFrom is the declaration as it now stands on the server.
func (p *jsStreams) streamFrom(id string, in streams.Stream) streams.Stream {
	return streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
		Headers:     in.Headers.Clone(),
	}
}

// toStream maps a JetStream config back to the contract's shape.
func toStream(cfg *jetstream.StreamConfig) streams.Stream {
	headers, _ := storedHeaders(cfg)
	return streams.Stream{
		ID:          cfg.Name,
		Name:        cfg.Metadata[metaName],
		Description: cfg.Description,
		Subjects:    slices.Clone(cfg.Subjects),
		UserID:      cfg.Metadata[metaUser],
		Active:      cfg.Metadata[metaActive] == "true",
		Headers:     headers,
	}
}

// storedHeaders returns the stream headers kept under metaHeaders, nil when
// there are none or they do not parse.
func storedHeaders(cfg *jetstream.StreamConfig) (streams.Headers, error) {
	encoded, ok := cfg.Metadata[metaHeaders]
	if !ok {
		return nil, nil //nolint:nilnil // nil headers are the valid answer "the stream has none"
	}
	var headers streams.Headers
	if err := json.Unmarshal([]byte(encoded), &headers); err != nil {
		return nil, fmt.Errorf("nats: stream %s has unreadable headers under %s: %w", cfg.Name, metaHeaders, err)
	}
	return headers.Clone(), nil
}

// safeName makes an id usable as a JetStream stream name.
//
// JetStream forbids whitespace and the characters that mean something in a
// subject, and a generated id carries the caller's stream name for readability
// — which may contain any of them.
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '>', '/', '\\', ' ', '\t', '\n':
			return '-'
		}
		return r
	}, s)
}

// jsManager publishes to and consumes from one JetStream stream.
type jsManager struct {
	p      *jsStreams
	handle jetstream.Stream
	stream streams.Stream

	// allowTTL is the stream's allow_msg_ttl as it was when bound, which an
	// expiring publish needs.
	allowTTL bool
}

var (
	_ streams.Manager    = (*jsManager)(nil)
	_ streams.Durable    = (*jsManager)(nil)
	_ streams.Positioned = (*jsManager)(nil)
)

// checkSubject rejects a subject the stream does not declare.
func (m *jsManager) checkSubject(ctx context.Context, subject string) error {
	if core.DeclaresPattern(m.stream.Subjects, subject) {
		return nil
	}
	m.p.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// Publish appends a value to the stream under a subject and waits for the
// server to store it.
//
// The message id is sent as the JetStream message id, so a publish retried
// after an ambiguous failure is collapsed by the server rather than appended
// twice. That is the one thing [streams.WithPublisherRetry] cannot do on its
// own: it can retry safely only because the backend can recognize the repeat.
//
// Headers go out as NATS headers beside the id, and [streams.Expires] as
// Nats-TTL. [streams.TTL] is refused, because JetStream delivers when it is
// read, not on a timer, and [streams.Retain], because its only last-value form
// would cap the stream at one message per subject.
//
// At most maxPending publishes from one provider await their acks at once; a
// publish past that waits for room. One whose ack has not come within five
// seconds fails. A publish canceled while it waits returns at once, and its
// room is given back only when its ack can no longer arrive, so canceling
// never lets more than maxPending stand at once.
func (m *jsManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	if strings.ContainsAny(subject, "*>") {
		return "", fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}

	o := streams.NewOptions(opts...)
	headers, err := m.checkPublish(o)
	if err != nil {
		return "", err
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.p.codec, id, value)
	if err != nil {
		return "", err
	}

	c := m.p.credit()
	defer c.release()
	if terr := c.take(ctx, 1); terr != nil {
		return "", m.publishFailed(ctx, subject, id, terr)
	}
	c.spend()
	future, err := m.send(subject, id, headers, body)
	if err != nil {
		c.finish()
		return "", m.publishFailed(ctx, subject, id, err)
	}
	if err := m.p.stored(ctx, future, c); err != nil {
		return "", m.publishFailed(ctx, subject, id, err)
	}

	m.p.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// checkPublish refuses what no JetStream publish can do, before anything is
// sent, and returns the headers to send beside Nats-Msg-Id, nil when there are
// none.
func (m *jsManager) checkPublish(o streams.Options) (gonats.Header, error) {
	if o.TTL > 0 {
		return nil, fmt.Errorf("%w: JetStream delivers when it is read, not on a timer", streams.ErrUnsupported)
	}
	if err := refuseRetain(o, "JetStream's last-per-subject form caps the stream at one message per subject, which would discard the log durable consumers read"); err != nil {
		return nil, err
	}
	var ttl int64
	if o.Expires > 0 {
		if !m.allowTTL {
			return nil, fmt.Errorf("%w: stream %s was created without allow_msg_ttl, so it cannot expire a message; update it through this provider to enable it", streams.ErrUnsupported, m.stream.ID)
		}
		seconds, err := ttlSeconds(o.Expires)
		if err != nil {
			return nil, err
		}
		ttl = seconds
	}
	headers, err := outgoing(m.stream, o)
	if err != nil {
		return nil, err
	}
	if len(headers) == 0 && ttl == 0 {
		return nil, nil //nolint:nilnil // a nil header block is the valid answer "send none"
	}
	h := toHeader(headers)
	if ttl > 0 {
		h.Set(jetstream.MsgTTLHeader, strconv.FormatInt(ttl, 10))
	}
	return h, nil
}

// ttlSeconds returns an expiry as Nats-TTL seconds. The server counts whole
// seconds and refuses less than one, so a fraction rounds up (the message
// lives at most that much longer, never shorter) and under a second is
// refused here, by name, rather than by the server.
func ttlSeconds(expires time.Duration) (int64, error) {
	if expires < time.Second {
		return 0, fmt.Errorf("%w: JetStream expires messages in whole seconds, at least one; %s is shorter", streams.ErrUnsupported, expires)
	}
	return int64((expires + time.Second - 1) / time.Second), nil
}

// send hands one frame to the connection under id, with headers beside
// Nats-Msg-Id, and returns once it is on its way. The future resolves when
// the server has stored it, refused it, or ackTimeout has passed.
func (m *jsManager) send(subject, id string, headers gonats.Header, body []byte) (jetstream.PubAckFuture, error) {
	h := make(gonats.Header, len(headers)+1)
	for k, v := range headers {
		h[k] = v
	}
	h.Set(jetstream.MsgIDHeader, id)
	return m.p.js.PublishMsgAsync(&gonats.Msg{Subject: subject, Data: body, Header: h})
}

// publishFailed logs a publish that was not stored and wraps the reason with
// its subject.
func (m *jsManager) publishFailed(ctx context.Context, subject, id string, err error) error {
	m.p.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
	return fmt.Errorf("nats: cannot publish on %q: %w", subject, err)
}

// stored waits for a sent publish's ack and settles its permit on c.
//
// When the wait ends without an answer (ctx is done, or the provider closed),
// the permit is handed to c.abandon rather than returned, since the ack may
// still be on its way.
func (p *jsStreams) stored(ctx context.Context, future jetstream.PubAckFuture, c *credit) error {
	select {
	case <-future.Ok():
		c.finish()
		return nil
	case err := <-future.Err():
		c.finish()
		return err
	case <-ctx.Done():
		c.abandon(future)
		return ctx.Err()
	case <-p.closed:
		c.abandon(future)
		return errClosed
	}
}

// credit starts an empty holder of permits from this provider's budget; its
// first take refuses a closed provider.
func (p *jsStreams) credit() *credit {
	return &credit{b: p.budget, closed: p.closed}
}

// budget is a counting semaphore over maxPending permits, taken and returned
// by count.
type budget struct {
	mu    sync.Mutex
	avail int
	// wake is closed and replaced whenever permits come back, so every
	// waiter looks again.
	wake chan struct{}
}

// newBudget returns a budget of n permits.
func newBudget(n int) *budget {
	return &budget{avail: n, wake: make(chan struct{})}
}

// tryTake takes n permits if they are free now.
func (b *budget) tryTake(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.avail < n {
		return false
	}
	b.avail -= n
	return true
}

// take takes n permits, waiting for them until ctx is done or closed is.
func (b *budget) take(ctx context.Context, closed <-chan struct{}, n int) error {
	for {
		b.mu.Lock()
		if b.avail >= n {
			b.avail -= n
			b.mu.Unlock()
			return nil
		}
		wake := b.wake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return errClosed
		}
	}
}

// give returns n permits.
func (b *budget) give(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.avail += n
	close(b.wake)
	b.wake = make(chan struct{})
	b.mu.Unlock()
}

// available is how many permits are free, for tests.
func (b *budget) available() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.avail
}

// credit is the permits under maxPending one publish or batch holds, given
// back by release.
//
// spare are held but not yet spent on a send; the rest are spent on sends
// still awaiting acks. A caller calls finish for every send it spent a permit
// on once that send's wait is over, or abandon when it stops waiting first.
// An abandoned permit is returned only once its ack can no longer arrive, so
// a canceled caller never lets more than maxPending waits stand at once.
type credit struct {
	b      *budget
	closed <-chan struct{}

	held, spare, finished int
}

// tryTake takes up to want permits, at most chunk, without waiting.
func (c *credit) tryTake(want int) bool {
	want = max(1, min(want, chunk))
	if !c.b.tryTake(want) {
		return false
	}
	c.held += want
	c.spare += want
	return true
}

// take takes up to want permits, at most chunk, waiting for them. Whatever
// finished is returned first, so a caller never waits on permits it holds
// itself.
func (c *credit) take(ctx context.Context, want int) error {
	c.flush()
	select {
	case <-c.closed:
		return errClosed
	default:
	}
	want = max(1, min(want, chunk))
	if err := c.b.take(ctx, c.closed, want); err != nil {
		return err
	}
	c.held += want
	c.spare += want
	return nil
}

// hasSpare reports whether a send may go out now.
func (c *credit) hasSpare() bool { return c.spare > 0 }

// spend spends one spare permit on a send.
func (c *credit) spend() { c.spare-- }

// finish returns the permit of a send whose wait is over, or which never went
// out, in chunks.
func (c *credit) finish() {
	c.finished++
	if c.finished >= chunk {
		c.flush()
	}
}

// abandon hands the permit of a send whose wait the caller gave up on to a
// goroutine that returns it once the ack has come or failed. ackTimeout
// bounds that, so the goroutine always ends.
func (c *credit) abandon(future jetstream.PubAckFuture) {
	c.held--
	go func() {
		// The outcome belonged to the caller that gave up on it; all that
		// matters here is that the wait is over.
		select {
		case <-future.Ok():
		case err := <-future.Err():
			_ = err
		}
		c.b.give(1)
	}()
}

// flush returns the permits of finished sends in one call.
func (c *credit) flush() {
	if c.finished > 0 {
		c.held -= c.finished
		c.b.give(c.finished)
		c.finished = 0
	}
}

// release returns what is free now: the spare permits and the finished ones.
// Every spent permit has been finished or abandoned by then.
func (c *credit) release() {
	c.b.give(c.spare + c.finished)
	c.held -= c.spare + c.finished
	c.spare, c.finished = 0, 0
}

// Subscribe returns a channel of messages for a subject, starting at the end of
// the log.
//
// This is the undurable half: an ordered consumer that acknowledges nothing and
// that the server discards once it has gone unpulled for the inactive
// threshold ([WithInactiveThreshold], 30 seconds by default) after ctx is done.
// Nothing about what it saw survives it. Reach for [jsManager.Consume] when
// that matters.
func (m *jsManager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	consumer, err := m.handle.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{subject},
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		InactiveThreshold: m.p.inactiveThreshold,
	})
	if err != nil {
		m.p.log.Error(ctx, "could not create an ordered consumer", err,
			telemetry.Fields{"subject": subject})
		return nil, fmt.Errorf("nats: cannot subscribe to %q: %w", subject, err)
	}

	it, err := consumer.Messages()
	if err != nil {
		return nil, fmt.Errorf("nats: cannot subscribe to %q: %w", subject, err)
	}

	m.p.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)
		// Next blocks with no context of its own, so cancellation has to reach
		// it by closing the iterator underneath it.
		stop := context.AfterFunc(ctx, it.Stop)
		defer stop()

		delivered := 0
		defer func() {
			m.p.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			raw, err := it.Next()
			if err != nil {
				// The iterator closing is how a canceled subscription ends.
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) && ctx.Err() == nil {
					m.p.log.Error(ctx, "could not read the stream", err,
						telemetry.Fields{"subject": subject})
				}
				return
			}

			msg, derr := core.Unpack(m.p.registry, raw.Subject(), raw.Data())
			if derr != nil {
				m.p.log.Warn(ctx, "dropping a malformed message",
					telemetry.Fields{"subject": raw.Subject(), "error": derr.Error()})
				continue
			}
			msg.Headers = fromHeader(raw.Headers())
			delivered++
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Consume delivers messages under a named consumer, starting at new ones.
func (m *jsManager) Consume(ctx context.Context, subject, consumer string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	return m.ConsumeFrom(ctx, subject, consumer, streams.FromNew, opts...)
}

// ConsumeFrom delivers messages under a named consumer, starting at a chosen
// position.
//
// The name is a JetStream durable consumer: two processes consuming under one
// name share its position and split the work, and a process that dies and comes
// back resumes where the name left off rather than where the process did.
//
// The position applies when the consumer is created and not after. A consumer
// that already exists keeps the position it has — resetting it on every attach
// would replay the log every time a process restarted, which is the opposite of
// what a durable consumer is for. [WithAckWait] and [WithMaxDeliver] are
// written when it is created, and [streams.Prefetch] bounds how many messages
// each pull asks for.
func (m *jsManager) ConsumeFrom(ctx context.Context, subject, consumer string, at streams.Position, opts ...streams.Option) (<-chan streams.Delivery, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("%w: a durable consumer needs a name, because the name is the position that survives a restart", streams.ErrUnsupported)
	}

	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: the consumer name %q is already the group on JetStream; drop the Group option", streams.ErrUnsupported, consumer)
	}

	name := safeName(consumer)
	policy := jetstream.DeliverNewPolicy
	if at == streams.FromEarliest {
		policy = jetstream.DeliverAllPolicy
	}

	// Fetch before create, rather than CreateOrUpdate: an update would try to
	// move an existing consumer's delivery policy, which JetStream refuses and
	// which would be the wrong thing to ask for anyway.
	c, err := m.handle.Consumer(ctx, name)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		c, err = m.handle.CreateConsumer(ctx, jetstream.ConsumerConfig{
			Name:          name,
			Durable:       name,
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: policy,
			AckWait:       m.p.ackWait,
			MaxDeliver:    m.p.maxDeliver,
		})
	}
	if err != nil {
		m.p.log.Error(ctx, "could not attach the consumer", err,
			telemetry.Fields{"subject": subject, "consumer": name})
		return nil, fmt.Errorf("nats: cannot consume %q as %q: %w", subject, consumer, err)
	}

	it, err := c.Messages(jetstream.PullMaxMessages(max(1, core.Prefetch(o))))
	if err != nil {
		return nil, fmt.Errorf("nats: cannot consume %q as %q: %w", subject, consumer, err)
	}

	m.p.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": name, "from": policy,
	})

	out := make(chan streams.Delivery, core.Prefetch(o))
	go func() {
		defer close(out)
		stop := context.AfterFunc(ctx, it.Stop)
		defer stop()

		delivered := 0
		defer func() {
			m.p.log.Info(ctx, "consumer stopped", telemetry.Fields{
				"subject": subject, "consumer": name, "delivered": delivered,
			})
		}()

		for {
			raw, err := it.Next()
			if err != nil {
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) && ctx.Err() == nil {
					m.p.log.Error(ctx, "could not read as the consumer", err,
						telemetry.Fields{"subject": subject, "consumer": name})
				}
				return
			}

			msg, derr := core.Unpack(m.p.registry, raw.Subject(), raw.Data())
			if derr != nil {
				// Nothing will ever decode this, so a handler will never
				// acknowledge it and it would redeliver forever. Terminate it:
				// unlike Ack, that records the message as undeliverable rather
				// than as handled.
				m.p.log.Warn(ctx, "terminating a malformed message",
					telemetry.Fields{"subject": raw.Subject(), "error": derr.Error()})
				_ = raw.Term()
				continue
			}
			msg.Headers = fromHeader(raw.Headers())

			attempt := 1
			if md, merr := raw.Metadata(); merr == nil && md.NumDelivered > 0 {
				attempt = int(md.NumDelivered)
			}

			delivered++
			m.p.metrics.Delivered(ctx, subject, name)
			select {
			case out <- streams.NewDelivery(msg, attempt, &jsAck{
				msg: raw, metrics: m.p.metrics, subject: subject, consumer: name,
			}):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// PublishBatch sends several values on a subject.
//
// It sends every value without waiting, then collects the acks, so a batch
// costs about one round trip rather than one per message. Frames are sent one
// after another, in order, so the stream stores them in order; only the waits
// for their acks overlap. Up to batchInFlight wait at once, within the
// provider's own limit of maxPending across every publisher. While that limit
// is reached, the batch settles its own oldest waits before it waits on anyone
// else, so it never holds room it is itself waiting for.
//
// Each message carries its own id as Nats-Msg-Id, beside the same headers and
// expiry as every other message of the batch, so a retried batch is collapsed
// by the duplicate window just as a retried single publish is. A message that
// fails is reported by index and its id left empty; the others stand. What no
// message of the batch could do (an undeclared subject, a refused option) fails
// every entry before anything is sent.
func (m *jsManager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	if err := m.checkSubject(ctx, subject); err != nil {
		return make([]string, len(values)), err
	}
	if strings.ContainsAny(subject, "*>") {
		return make([]string, len(values)), fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}
	headers, err := m.checkPublish(o)
	if err != nil {
		return make([]string, len(values)), err
	}

	ids := make([]string, len(values))
	failures := make([]error, len(values))
	fail := func(i int, err error) {
		failures[i] = fmt.Errorf("entry %d: %w", i, m.publishFailed(ctx, subject, ids[i], err))
		ids[i] = ""
	}

	type wait struct {
		index  int
		future jetstream.PubAckFuture
	}
	waiting := make([]wait, 0, min(len(values), batchInFlight))
	settleOldest := func(c *credit) {
		w := waiting[0]
		waiting = waiting[1:]
		if err := m.p.stored(ctx, w.future, c); err != nil {
			fail(w.index, err)
		}
	}

	c := m.p.credit()
	defer c.release()
	for i, value := range values {
		ids[i] = core.NewID()
		if err := ctx.Err(); err != nil {
			for j := i; j < len(values); j++ {
				fail(j, err)
			}
			break
		}
		body, err := core.Pack(m.p.codec, ids[i], value)
		if err != nil {
			ids[i] = ""
			failures[i] = fmt.Errorf("entry %d: %w", i, err)
			continue
		}

		if len(waiting) >= batchInFlight {
			settleOldest(c)
		}
		// Room under the provider's in-flight limit: take it without waiting
		// if there is some, else settle this batch's own oldest wait, and only
		// wait on other publishers when nothing of ours is outstanding.
		left := len(values) - i
		var starved error
		for !c.hasSpare() && !c.tryTake(left) {
			if len(waiting) > 0 {
				settleOldest(c)
				continue
			}
			if terr := c.take(ctx, left); terr != nil {
				starved = terr
				break
			}
		}
		if starved != nil {
			for j := i; j < len(values); j++ {
				fail(j, starved)
			}
			break
		}

		c.spend()
		future, err := m.send(subject, ids[i], headers, body)
		if err != nil {
			c.finish()
			fail(i, err)
			continue
		}
		waiting = append(waiting, wait{index: i, future: future})
	}
	for len(waiting) > 0 {
		settleOldest(c)
	}

	m.p.log.Debug(ctx, "published a batch", telemetry.Fields{"subject": subject, "entries": len(values)})
	return ids, core.BatchError(subject, len(values), compact(failures))
}

// compact drops the nil entries, so BatchError counts only real failures.
func compact(errs []error) []error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// jsAck settles one JetStream delivery. JetStream has a true negative
// acknowledgement, so Nak returns the message rather than waiting out a timeout,
// and a delayed one has the server hold the redelivery back.
type jsAck struct {
	msg               jetstream.Msg
	metrics           *core.Metrics
	subject, consumer string
}

var _ streams.DelayedAcker = jsAck{}

// Ack is a +ACK: the server will not redeliver this message to the consumer.
func (a jsAck) Ack(ctx context.Context) error {
	if err := a.msg.Ack(); err != nil {
		return err
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "ack")
	return nil
}

// Nak is a -NAK: the server redelivers at once rather than after the ack wait.
func (a jsAck) Nak(ctx context.Context) error {
	if err := a.msg.Nak(); err != nil {
		return err
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "nak")
	return nil
}

// NakAfter is a -NAK carrying the delay: the server redelivers no sooner than
// delay from when it reads it. A zero delay is a plain Nak.
func (a jsAck) NakAfter(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return a.Nak(ctx)
	}
	if err := a.msg.NakWithDelay(delay); err != nil {
		return err
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "nak")
	return nil
}

// MaxNakDelay is zero: the server holds a delayed nak back for as long as it
// is asked to.
func (a jsAck) MaxNakDelay() time.Duration { return 0 }
