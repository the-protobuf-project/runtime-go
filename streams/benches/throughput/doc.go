//go:build live

// Command throughput is the throughput and latency benchmark for every stream
// provider.
//
// One harness drives every backend through the streams contract, so the
// numbers compare like with like: the same messages, the same subjects, the
// same verification. It is the Go port of runtime-rs's
// streams/benches/throughput, and writes the same report, field for field, so
// one report generator reads both and Go and Rust compare run for run. Run it
// from the streams module root:
//
//	go run -tags live ./benches/throughput --backend redis --scenario fanout --messages 100000
//
// # Messages
//
// Payloads are the messages codec/protobuf's tests use:
// google.protobuf.StringValue (a body of --payload bytes) or
// google.protobuf.Int64Value (the sequence number), published through
// codec/protobuf, so the bytes on the wire are what the Rust harness sends.
// Every message is decoded on receipt, so decode cost is part of what is
// measured.
//
// --message opcua is for the OPC UA providers, whose payload is a typed OPC UA
// value rather than a protobuf message: each message is the sequence as an
// Int64. runtime-go writes a node's value without a source timestamp, so the
// server stamps it, and the delivered message's id is that timestamp; latency
// is measured from it on the wall clock. Subjects are node ids: the bench.sNN
// names behind --subject-prefix, which defaults to opc-plc's namespace
// (nsu=http://microsoft.com/Opc/OpcPlc/;s=) for the opcua backend.
//
// Single publishes pin the message id to <seq>.<nanos since start>, which
// carries the sequence and send time without changing the payload. A batch
// cannot pin ids, so batch publishes Int64Value and reads the sequence back
// out of the payload; it reports throughput and loss, not latency.
//
// # Scenarios
//
//   - fanout: one plain subscriber per subject; publishers spread messages
//     round-robin over the subjects. A plain subscription may lose under load:
//     that is reported as lost, not hidden.
//   - batch: fanout, publishing through Batch in groups of --batch.
//   - durable: --consumers durable consumers per subject sharing one name,
//     every message acked after it is read; every sequence must arrive.
//   - group: --consumers plain subscribers per subject in one group; every
//     message must arrive once across the group.
//
// --drain 1 (durable only) publishes everything first and then attaches the
// consumers from the earliest offset, so delivered_rate is the rate of reading
// a full log back alone. Its latencies include the wait, and mean nothing.
//
// --consumers 0 attaches no receiver at all: a publish-only run, to set beside
// a broker tool's publish benchmark, which reads nothing back.
//
// Each run prints one JSON object and appends it to benches/results.jsonl
// (BENCH_RESULTS overrides the path).
package main
