//go:build live

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// start is the instant every send time is measured from. Publisher and
// receiver share one process, so one monotonic clock gives an honest
// end-to-end latency.
var start = time.Now()

// args is what a run is asked to do.
type args struct {
	backend       string
	profile       string
	scenario      string
	messages      uint64
	subjects      int
	publishers    int
	consumers     int
	batch         int
	payload       int
	message       string
	subjectPrefix string
	rate          uint64
	timeout       time.Duration
	label         string
	drain         bool
}

// fatalf reports a run that cannot go on and exits non-zero, writing no
// result, as the Rust harness's panics do.
func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "throughput: "+format+"\n", a...)
	os.Exit(2)
}

// parseArgs reads --key value pairs from the command line, failing on anything
// unknown or inconsistent so a bad run fails before it connects.
func parseArgs(argv []string) args {
	a := args{
		backend:    "redis",
		profile:    "plain",
		scenario:   "fanout",
		messages:   20_000,
		subjects:   10,
		publishers: 32,
		consumers:  1,
		batch:      100,
		payload:    256,
		timeout:    300 * time.Second,
	}
	for i := 0; i < len(argv); i += 2 {
		key := argv[i]
		if i+1 >= len(argv) {
			fatalf("%s needs a value", key)
		}
		value := argv[i+1]
		n := func() uint64 {
			v, err := strconv.ParseUint(strings.ReplaceAll(value, "_", ""), 10, 64)
			if err != nil {
				fatalf("%s: %q is not a number", key, value)
			}
			return v
		}
		switch key {
		case "--backend":
			a.backend = value
		case "--profile":
			a.profile = value
		case "--scenario":
			a.scenario = value
		case "--messages":
			a.messages = n()
		case "--subjects":
			a.subjects = int(n())
		case "--publishers":
			a.publishers = int(n())
		case "--consumers":
			a.consumers = int(n())
		case "--batch":
			a.batch = int(n())
		case "--payload":
			a.payload = int(n())
		case "--message":
			a.message = value
		case "--subject-prefix":
			a.subjectPrefix = value
		case "--rate":
			a.rate = n()
		case "--timeout":
			a.timeout = time.Duration(n()) * time.Second
		case "--label":
			a.label = value
		case "--drain":
			a.drain = n() != 0
		default:
			fatalf("unknown argument %s", key)
		}
	}
	if a.subjects <= 0 || a.publishers <= 0 || a.batch <= 0 {
		fatalf("--subjects, --publishers and --batch must be positive")
	}
	switch a.scenario {
	case "fanout", "batch", "durable", "group":
	default:
		fatalf("--scenario is fanout, batch, durable, or group")
	}
	if a.drain && a.scenario != "durable" {
		fatalf("--drain reads a published log back, so it needs --scenario durable")
	}
	// A batch of full-size bodies writes the sequence padded to --payload
	// bytes; a sequence with more digits than that would overflow the body and
	// fail the receiver's length check as undecodable.
	last := uint64(0)
	if a.messages > 0 {
		last = a.messages - 1
	}
	widest := len(strconv.FormatUint(last, 10))
	if a.scenario == "batch" && a.message == "string" && a.payload < widest {
		fatalf("--payload %d cannot hold sequence numbers up to %d (%d digits)", a.payload, last, widest)
	}
	// A batch cannot pin ids, so by default it sends Int64Value and reads the
	// sequence from it; --message string sends a full-size StringValue
	// instead, whose body ends with the sequence.
	if a.message == "" {
		a.message = "string"
		if a.scenario == "batch" {
			a.message = "int64"
		}
	}
	// Subjects on the client/server backend are node ids in the simulator's
	// namespace; PubSub maps plain subjects to its own topics.
	if a.backend == "opcua" && a.subjectPrefix == "" {
		a.subjectPrefix = "nsu=http://microsoft.com/Opc/OpcPlc/;s="
	}
	switch a.message {
	case "string", "int64", "opcua":
	default:
		fatalf("--message is string, int64, or opcua")
	}
	return a
}

// receiversPerSubject is how many receivers each subject gets. --consumers 0
// attaches none, for a publish-only run to set beside a broker tool that only
// publishes.
func (a args) receiversPerSubject() int {
	switch a.scenario {
	case "durable", "group":
		return a.consumers
	default:
		return min(a.consumers, 1)
	}
}

// payload builds message seq as one of codec/protobuf's test messages.
//
// body empty means the id cannot carry the sequence (a batch), so a
// StringValue body is --payload bytes ending in the sequence instead.
func payload(a args, seq uint64, body string) any {
	switch a.message {
	case "opcua":
		// OPC UA writes a sized integer as the node's own Int64.
		return int64(seq)
	case "int64":
		return wrapperspb.Int64(int64(seq))
	}
	if body == "" {
		s := strconv.FormatUint(seq, 10)
		return wrapperspb.String(strings.Repeat("x", max(a.payload-len(s), 0)) + s)
	}
	return wrapperspb.String(body)
}

// pinnedID is the id carrying sequence and send time.
func pinnedID(seq uint64) string {
	return strconv.FormatUint(seq, 10) + "." + strconv.FormatInt(time.Since(start).Nanoseconds(), 10)
}

// readMessage decodes a delivered message as a real consumer would, returning
// its sequence and, when the id carries a send time, its end-to-end latency
// (negative when there is none).
func readMessage(a args, msg streams.Message) (seq uint64, latency time.Duration, ok bool) {
	latency = -1
	pinnedSeq, sent, pinned := uint64(0), time.Duration(0), false
	if s, t, found := strings.Cut(msg.ID, "."); found {
		ps, perr := strconv.ParseUint(s, 10, 64)
		pt, terr := strconv.ParseInt(t, 10, 64)
		if perr == nil && terr == nil {
			pinnedSeq, sent, pinned = ps, time.Duration(pt), true
		}
	}
	since := func(sent time.Duration) time.Duration {
		return max(time.Since(start)-sent, 0)
	}
	switch a.message {
	case "opcua":
		return opcuaRead(msg)
	case "int64":
		var v wrapperspb.Int64Value
		if msg.Decode(&v) != nil || v.Value < 0 {
			return 0, latency, false
		}
		seq = uint64(v.Value)
		switch {
		case pinned && pinnedSeq != seq:
			return 0, latency, false
		case pinned:
			return seq, since(sent), true
		default:
			return seq, latency, true
		}
	default:
		var v wrapperspb.StringValue
		if msg.Decode(&v) != nil || len(v.Value) != a.payload {
			return 0, latency, false
		}
		if pinned {
			return pinnedSeq, since(sent), true
		}
		// A batch: the sequence is the end of the body.
		seq, err := strconv.ParseUint(strings.TrimLeft(v.Value, "x"), 10, 64)
		if err != nil {
			return 0, latency, false
		}
		return seq, latency, true
	}
}

// opcuaRead reads the sequence of an OPC UA value the bench wrote and, from
// the source timestamp the provider hands over as the message id, its latency.
func opcuaRead(msg streams.Message) (uint64, time.Duration, bool) {
	var seq int64
	if msg.Decode(&seq) != nil || seq < 0 {
		return 0, -1, false
	}
	latency := time.Duration(-1)
	if sent, err := time.Parse(time.RFC3339Nano, msg.ID); err == nil {
		latency = max(time.Since(sent), 0)
	}
	return uint64(seq), latency, true
}

// received is what one receiver saw. Kept behind a mutex the run holds, so the
// results survive the receiver being stopped at the end.
type received struct {
	mu          sync.Mutex
	seqs        []uint64
	latenciesUS []uint32
	undecodable uint64
	ackErrors   uint64
}

// shared holds the counters every goroutine in a run shares.
type shared struct {
	published     atomic.Uint64
	publishErrors atomic.Uint64
	received      atomic.Uint64
	// unique is the distinct sequence numbers delivered so far. The wait for
	// delivery stops on this, not on received: a backend that redelivers
	// (Kafka during a rebalance) reaches the published count with duplicates
	// while the last unique messages are still on their way.
	unique atomic.Uint64
	// seen is one bit per sequence number, set on its first delivery.
	seen []atomic.Uint64

	errMu      sync.Mutex
	firstError *string
}

// newShared returns the counters for a run of messages sequence numbers.
func newShared(messages uint64) *shared {
	return &shared{seen: make([]atomic.Uint64, (messages+63)/64)}
}

// mark records a delivery of seq, counting it once however often it arrives.
func (s *shared) mark(seq uint64) {
	word := seq / 64
	if word >= uint64(len(s.seen)) {
		return
	}
	bit := uint64(1) << (seq % 64)
	if s.seen[word].Or(bit)&bit == 0 {
		s.unique.Add(1)
	}
}

// error counts n failed publishes, keeping only the first error's text for
// the report.
func (s *shared) error(n uint64, err error) {
	s.publishErrors.Add(n)
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.firstError == nil {
		text := err.Error()
		s.firstError = &text
	}
}

// subject is the ith bench subject, zero-padded so subjects sort in order.
func subject(i int) string {
	return fmt.Sprintf("bench.s%02d", i)
}

// resultsPath is where reports are appended: BENCH_RESULTS, or results.jsonl
// beside this harness's directory.
func resultsPath() string {
	if p := os.Getenv("BENCH_RESULTS"); p != "" {
		return p
	}
	if _, file, _, ok := runtime.Caller(0); ok && filepath.IsAbs(file) {
		return filepath.Join(filepath.Dir(file), "..", "results.jsonl")
	}
	return filepath.Join("benches", "results.jsonl")
}

// main runs one benchmark, prints its report, and appends it as one JSON line
// to the results file.
func main() {
	a := parseArgs(os.Args[1:])
	report := run(a)
	pretty, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(pretty))
	fmt.Fprintln(os.Stderr, verdict(report))
	line, err := json.Marshal(report)
	if err != nil {
		fatalf("encode report: %v", err)
	}
	path := resultsPath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fatalf("open results file %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(append(line, '\n')); err != nil {
		fatalf("write results file %s: %v", path, err)
	}
}

// run declares the stream, publishes and receives everything, then deletes the
// stream and returns the report. Receivers attach before publishing unless
// --drain asks to read the finished log back.
func run(a args) map[string]any {
	ctx := context.Background()
	conn, err := connect(ctx, a.backend, a.profile)
	if err != nil {
		fatalf("connect %s/%s: %v", a.backend, a.profile, err)
	}
	subjects := make([]string, a.subjects)
	for i := range subjects {
		subjects[i] = a.subjectPrefix + subject(i)
	}
	stream, err := conn.publisher.Create(ctx, streams.Stream{
		Name:     "bench-" + a.scenario,
		Subjects: subjects,
		Active:   true,
	})
	if err != nil {
		fatalf("create stream: %v", err)
	}
	if conn.subscriber != nil {
		// Brokerless backends keep stream declarations on each side.
		if _, err = conn.subscriber.Create(ctx, stream); err != nil {
			fatalf("declare stream on the subscriber side: %v", err)
		}
	}
	publisher, err := conn.publisher.Bind(ctx, stream.ID)
	if err != nil {
		fatalf("bind publisher: %v", err)
	}
	receiver := publisher
	if conn.subscriber != nil {
		if receiver, err = conn.subscriber.Bind(ctx, stream.ID); err != nil {
			fatalf("bind subscriber: %v", err)
		}
	}

	// Draining publishes before any receiver exists, so only a backend that
	// keeps a log a consumer can start from the beginning of measures a drain;
	// elsewhere (a RabbitMQ queue or an MQTT session created on consume)
	// everything published first would be dropped and counted lost.
	if a.drain {
		if _, err := streams.AsPositioned(receiver); err != nil {
			fatalf("--drain needs a backend that can replay its log from the earliest message: %v", err)
		}
	}

	// Receivers stop when this is cancelled, as the Rust harness aborts its
	// receiver tasks.
	recvCtx, stopReceivers := context.WithCancel(ctx)
	defer stopReceivers()
	sh := newShared(a.messages)
	var slots []*received
	var receivers sync.WaitGroup
	if !a.drain {
		slots = attachAll(recvCtx, a, receiver, subjects, sh, &receivers)
		time.Sleep(500 * time.Millisecond)
	}

	// Publish.
	body := strings.Repeat("x", a.payload)
	var next atomic.Uint64
	pubStart := time.Now()
	var pubs sync.WaitGroup
	step := uint64(1)
	if a.scenario == "batch" {
		step = uint64(a.batch)
	}
	for range a.publishers {
		pubs.Go(func() {
			for {
				first := next.Add(step) - step
				if first >= a.messages {
					return
				}
				last := min(first+step, a.messages)
				if a.rate > 0 {
					due := pubStart.Add(time.Duration(float64(first) / float64(a.rate) * float64(time.Second)))
					if wait := time.Until(due); wait > 0 {
						time.Sleep(wait)
					}
				}
				subj := subjects[int((first/step)%uint64(len(subjects)))]
				if a.scenario == "batch" {
					values := make([]any, 0, last-first)
					for seq := first; seq < last; seq++ {
						values = append(values, payload(a, seq, ""))
					}
					b, err := streams.AsBatch(publisher)
					var ids []string
					if err == nil {
						ids, err = b.PublishBatch(ctx, subj, values)
					}
					if err != nil {
						sh.error(last-first, err)
					} else {
						sh.published.Add(uint64(len(ids)))
					}
				} else {
					_, err := publisher.Publish(ctx, subj, payload(a, first, body), streams.ID(pinnedID(first)))
					if err != nil {
						sh.error(1, err)
					} else {
						sh.published.Add(1)
					}
				}
			}
		})
	}
	pubs.Wait()
	// A publish-only run stops the clock here, so the publishing connection
	// must first have sent what it buffered.
	if a.receiversPerSubject() == 0 && conn.flush != nil {
		if err := conn.flush(ctx); err != nil {
			// Counted as a failure, so the run is not reported ok when its
			// messages may never have left the client.
			sh.error(1, fmt.Errorf("flush: %w", err))
		}
	}
	publishSecs := time.Since(pubStart).Seconds()
	published := sh.published.Load()

	// Wait until everything published has arrived, or nothing has for 5s.
	// Draining: everything is in the log now; time reading it back alone.
	recvStart := pubStart
	if a.drain {
		recvStart = time.Now()
		slots = attachAll(recvCtx, a, receiver, subjects, sh, &receivers)
	}
	deadline := recvStart.Add(a.timeout)
	lastCount, quietSince := uint64(0), time.Now()
	for a.receiversPerSubject() > 0 {
		got := sh.unique.Load()
		if got >= published || time.Now().After(deadline) {
			break
		}
		if got != lastCount {
			lastCount, quietSince = got, time.Now()
		} else if time.Since(quietSince) > 5*time.Second {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	e2eSecs := time.Since(recvStart).Seconds()

	// Stop the receivers. A provider closes its channel when the context
	// ends; one that is slow to is not waited on past a few seconds, since
	// every result is already behind its slot's mutex.
	stopReceivers()
	done := make(chan struct{})
	go func() { receivers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	all := &received{}
	for _, slot := range slots {
		slot.mu.Lock()
		all.seqs = append(all.seqs, slot.seqs...)
		all.latenciesUS = append(all.latenciesUS, slot.latenciesUS...)
		all.undecodable += slot.undecodable
		all.ackErrors += slot.ackErrors
		slot.mu.Unlock()
	}
	var cleanup *string
	if err := conn.publisher.Delete(ctx, stream.ID); err != nil {
		text := err.Error()
		cleanup = &text
	}
	conn.close()
	sh.errMu.Lock()
	firstError := sh.firstError
	sh.errMu.Unlock()
	return summarize(a, sh, published, publishSecs, e2eSecs, all, firstError, cleanup)
}

// attachAll attaches every receiver of the run, each read by a goroutine of
// its own.
func attachAll(ctx context.Context, a args, receiver streams.Manager, subjects []string, sh *shared, wg *sync.WaitGroup) []*received {
	slots := make([]*received, 0, a.receiversPerSubject()*len(subjects))
	for _, s := range subjects {
		for range a.receiversPerSubject() {
			slot := &received{}
			r := attach(ctx, a, receiver, s)
			wg.Go(func() { r.receive(ctx, a, slot, sh) })
			slots = append(slots, slot)
		}
	}
	return slots
}

// source is one receiver: deliveries lent by a durable consumer, or messages
// handed over by a subscription. Exactly one is set; each is read straight
// from the provider's channel, with no relay of the harness's own between.
type source struct {
	lent  <-chan streams.Delivery
	plain <-chan streams.Message
}

// attach opens one receiver on subject for the scenario: a durable consumer
// from the earliest offset where the backend can replay, a group member, or a
// plain subscription.
func attach(ctx context.Context, a args, m streams.Manager, subj string) source {
	opts := []streams.Option{streams.Prefetch(1024)}
	switch a.scenario {
	case "durable":
		name := "bench-" + strings.ReplaceAll(subj, ".", "-")
		var deliveries <-chan streams.Delivery
		var err error
		if p, perr := streams.AsPositioned(m); perr == nil {
			deliveries, err = p.ConsumeFrom(ctx, subj, name, streams.FromEarliest, opts...)
		} else if d, derr := streams.AsDurable(m); derr == nil {
			deliveries, err = d.Consume(ctx, subj, name, opts...)
		} else {
			err = derr
		}
		if err != nil {
			fatalf("consume %s: %v", subj, err)
		}
		return source{lent: deliveries}
	case "group":
		messages, err := m.Subscribe(ctx, subj, append(opts, streams.Group("bench-group"))...)
		if err != nil {
			fatalf("subscribe %s in a group: %v", subj, err)
		}
		return source{plain: messages}
	default:
		messages, err := m.Subscribe(ctx, subj, opts...)
		if err != nil {
			fatalf("subscribe %s: %v", subj, err)
		}
		return source{plain: messages}
	}
}

// receive reads the source until it closes, recording each sequence and
// latency in slot; a durable delivery is acked only after it is decoded.
func (r source) receive(ctx context.Context, a args, slot *received, sh *shared) {
	if r.lent != nil {
		// The ack is not bound to ctx, so one in flight when the run stops
		// still completes.
		settle := context.WithoutCancel(ctx)
		for d := range r.lent {
			seq, latency, ok := readMessage(a, d.Message)
			// Settle after the work, as a real consumer should.
			record(slot, sh, seq, latency, ok, d.Ack(settle) == nil)
		}
		return
	}
	for msg := range r.plain {
		seq, latency, ok := readMessage(a, msg)
		record(slot, sh, seq, latency, ok, true)
	}
}

// record notes one received message in slot and the shared counters.
func record(slot *received, sh *shared, seq uint64, latency time.Duration, ok, acked bool) {
	slot.mu.Lock()
	if ok {
		sh.mark(seq)
		slot.seqs = append(slot.seqs, seq)
		if latency >= 0 {
			slot.latenciesUS = append(slot.latenciesUS, uint32(min(latency.Microseconds(), math.MaxUint32)))
		}
	} else {
		slot.undecodable++
	}
	if !acked {
		slot.ackErrors++
	}
	slot.mu.Unlock()
	sh.received.Add(1)
}

// summarize builds the report. ok holds only when nothing was lost, failed,
// undecodable or unacked, and a group delivered no duplicates.
//
// The keys and their meaning are the Rust harness's exactly; encoding/json
// writes a map's keys sorted, as serde_json does, so the lines match too.
func summarize(a args, sh *shared, published uint64, publishSecs, e2eSecs float64, all *received, firstError, cleanupError *string) map[string]any {
	delivered := uint64(len(all.seqs))
	set := make(map[uint64]struct{}, len(all.seqs))
	for _, s := range all.seqs {
		set[s] = struct{}{}
	}
	unique := uint64(len(set))
	// A publish-only run has nothing to lose.
	lost := uint64(0)
	if a.receiversPerSubject() > 0 && published > unique {
		lost = published - unique
	}
	publishErrors := sh.publishErrors.Load()
	slices.Sort(all.latenciesUS)
	pct := func(p float64) any {
		l := all.latenciesUS
		if len(l) == 0 {
			return nil
		}
		return float64(l[int(math.Round(float64(len(l)-1)*p))]) / 1000.0
	}
	ok := lost == 0 && publishErrors == 0 && all.undecodable == 0 && all.ackErrors == 0 &&
		(a.scenario != "group" || delivered == unique)
	message := "google.protobuf.StringValue"
	payloadBytes := a.payload
	switch a.message {
	case "int64":
		message, payloadBytes = "google.protobuf.Int64Value", 0
	case "opcua":
		message, payloadBytes = "opcua.DataValue(Int64)", 0
	}
	batch := 1
	if a.scenario == "batch" {
		batch = a.batch
	}
	return map[string]any{
		"label":                 a.label,
		"backend":               a.backend,
		"profile":               a.profile,
		"scenario":              a.scenario,
		"drain":                 a.drain,
		"message":               message,
		"payload_bytes":         payloadBytes,
		"messages":              a.messages,
		"subjects":              a.subjects,
		"publishers":            a.publishers,
		"receivers_per_subject": a.receiversPerSubject(),
		"batch":                 batch,
		"rate_target":           a.rate,
		"published":             published,
		"publish_errors":        publishErrors,
		"first_error":           firstError,
		"publish_secs":          round(publishSecs),
		"publish_rate":          round(float64(published) / max(publishSecs, 1e-9)),
		"delivered":             delivered,
		"unique":                unique,
		"duplicates":            delivered - unique,
		"lost":                  lost,
		"undecodable":           all.undecodable,
		"ack_errors":            all.ackErrors,
		"e2e_secs":              round(e2eSecs),
		"delivered_rate":        round(float64(unique) / max(e2eSecs, 1e-9)),
		"latency_ms":            map[string]any{"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1.0)},
		"cleanup_error":         cleanupError,
		"ok":                    ok,
	}
}

// held is the judgment the results page makes of a run: it holds its target
// rate when it is ok and delivered at least 95% of that rate. An unpaced run
// (target 0) holds when it is ok.
func held(report map[string]any) bool {
	ok, _ := report["ok"].(bool)
	target, _ := report["rate_target"].(uint64)
	rate, _ := report["delivered_rate"].(float64)
	return ok && (target == 0 || rate >= 0.95*float64(target))
}

// verdict is the one line written to stderr after the report: whether the run
// held its target.
func verdict(report map[string]any) string {
	target, _ := report["rate_target"].(uint64)
	what := "unpaced"
	if target > 0 {
		what = fmt.Sprintf("target %d/s", target)
	}
	if held(report) {
		return what + ": held"
	}
	return what + ": missed"
}

// round rounds to two decimals, so the report stays readable.
func round(x float64) float64 {
	return math.Round(x*100) / 100
}
