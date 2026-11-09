# Running the benchmarks

[Streams guide](../README.md) · [Local brokers](../docker/README.md)

`throughput/` drives every provider through the `streams` contract, so each
backend is measured the same way: the same messages, subjects and checks.
Messages are `codec/protobuf`'s test messages (`google.protobuf.StringValue`
with a 256-byte body, or `Int64Value` for batches), framed as the providers
frame them and decoded on every receipt.

It is the Go port of runtime-rs's `streams/benches/throughput`: the same
flags, scenarios, messages, checks and report, field for field, so the
results of the two read side by side, run for run.

Every file under `benches/` is built only with the `live` tag, so
`go build ./...`, `go vet ./...` and `go test ./...` never see it.

## Running one scenario

Start the broker from [docker/](../docker/README.md) (and, for `--profile tls`,
generate the test certificates with `docker/tls/generate.sh`), then from the
streams module root:

```bash
go run -tags live ./benches/throughput --backend redis-durable --profile plain \
    --scenario durable --messages 250000 --rate 50000 --publishers 256 --consumers 2
```

| Flag | Meaning |
| --- | --- |
| `--backend` | `kafka`, `nats`, `jetstream`, `rabbitmq`, `redis`, `redis-durable`, `mqtt`, `zeromq`, `opcua` (OPC UA client/server on opc-plc; run with `--message opcua`), `opcua-pubsub` (OPC UA PubSub on the `mqtt` brokers; run with `--message opcua`) |
| `--profile` | `plain`, or `tls` for the secure broker (mutual TLS and the backend's authentication) |
| `--scenario` | `fanout`, `batch`, `durable` or `group` |
| `--rate` | target messages a second across all publishers; `0` runs unpaced |
| `--messages`, `--subjects`, `--publishers`, `--consumers`, `--batch`, `--payload` | run shape; `--consumers 0` publishes only, flushing the publishing connection before the clock stops |
| `--message` | `string` (256-byte `StringValue`, the default), `int64`, or `opcua`; `batch` defaults to `int64`, and `--message string` makes it send full-size bodies that end in the sequence |
| `--subject-prefix` | put before every `bench.sNN` subject; defaults to opc-plc's namespace for `opcua` |
| `--timeout` | seconds to wait for delivery (default 300) |
| `--label` | free text carried into the report, to group runs |
| `--drain 1` | `durable` only: publish everything, then attach the consumers from the earliest offset, so `delivered_rate` is reading the log back alone (latencies then include the wait) |

Numbers take underscores (`--messages 100_000`).

Each run prints a JSON summary and appends it to `benches/results.jsonl`
(`BENCH_RESULTS` overrides the path; the file is not committed). The fields
are the Rust harness's `target/bench-results.jsonl` fields exactly, so the
same report generator reads both. A run is `ok` when nothing was lost,
undecodable or unacknowledged (and, for `group`, nothing arrived twice). It
holds a target rate when it is `ok` and delivers at least 95% of that rate;
the harness writes that verdict to stderr after the report.

Completion is counted by distinct sequence numbers, one bit each: a backend
that redelivers (Kafka during a rebalance) does not finish a run early on
duplicates. Single publishes pin the message id to `<seq>.<nanos since start>`,
which is where latency comes from; a batch cannot pin ids, so it reports
throughput and loss, not latency. On `opcua` the latency is measured from the
source timestamp the server gives the written value, on the wall clock.

An acknowledged publish waits about one round trip, so single-publish
scenarios need hundreds of publishers to show what a broker can do; with the
default 32 the harness measures itself.

### Environment

| Variable | Backend | Meaning |
| --- | --- | --- |
| `KAFKA_ADDR`, `KAFKA_E2E_ADDR` | kafka | plain (`127.0.0.1:19092`) and mTLS (`127.0.0.1:19093`) brokers |
| `KAFKA_PRODUCERS` | kafka | producer pool, `kafka.WithProducers` (default 8) |
| `NATS_URL`, `NATS_E2E_URL`, `NATS_E2E_CERTS` | nats, jetstream | plain and secure servers, and the secure profile's certificates |
| `AMQP_URL`, `RABBITMQ_E2E_ADDR` | rabbitmq | plain URL and AMQPS address |
| `RABBITMQ_PUBLISH_CHANNELS` | rabbitmq | publishing channel pool, `rabbitmq.WithPublishChannels` (default: the driver's) |
| `REDIS_ADDR`, `REDIS_TLS_ADDR`, `REDIS_TLS_CERTS` | redis, redis-durable | plain and TLS servers, and the TLS certificates |
| `REDIS_CONFIRM_ACKS` | redis-durable | `1` makes an ack wait for XACK, `redis.WithConfirmAcks(true)`; the harness default is `0`, as in runtime-rs |
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_TLS_ADDR` | mqtt, opcua-pubsub | plain and TLS brokers |
| `MQTT_PUBLISH_CONNECTIONS` | mqtt | publishing connections, `mqtt.WithPublishConnections` (default: the driver's, 1) |
| `OPCUA_URL`, `OPCUA_E2E_URL`, `OPCUA_PUBLISHING_MS` | opcua | plain and secure servers, and the subscription's publishing interval (default 20 ms) |
| `STREAMS_E2E_TLS_DIR` | all | test certificates (default `docker/tls/out`) |

## Running the whole matrix

```bash
benches/run-matrix.sh            # every backend
benches/run-matrix.sh kafka mqtt # some of them
```

It builds the harness once, then runs one backend at a time, starting and
removing its brokers, through the plaintext, TLS with production durability
(`STREAMS_DURABILITY=prod`) and TLS syncing every message (`strict`) profiles,
at 20k, 50k and 100k msg/s plus an unpaced run (`MATRIX_RATES` changes the
targets, `MATRIX_LABEL` the label). Run nothing else on the machine meanwhile,
and nothing else that uses the `tpp-streams-live-*` and `tpp-streams-e2e-*`
compose projects: the matrix takes them down and up. It exits non-zero if any
run failed or did not come back `ok`, after taking its brokers down.

## The broker's own tools

```bash
benches/native-baseline.sh             # nats, kafka, rabbitmq, mqtt
benches/native-baseline.sh kafka
```

Runs each project's official load tool (`nats bench`, `kafka-producer-perf-test`
and `kafka-consumer-perf-test`, RabbitMQ `perf-test`, `emqtt-bench`) through
Docker Desktop's port forward, the path a client on the host takes, so the
driver can be compared with it directly. It is the same script as runtime-rs's,
so one baseline serves both.
