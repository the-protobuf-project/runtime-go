#!/usr/bin/env bash
# Runs the official throughput matrix, one backend at a time so runs never
# compete for the machine. Results append to benches/results.jsonl in the same
# schema as runtime-rs's target/bench-results.jsonl, so one report generator
# reads both.
#
#   benches/run-matrix.sh [backend ...]      # default: every backend
#
# Each paced run sends 5 seconds' worth of messages at the target rate (at
# least 100,000); the unpaced run sends 500,000 to find the ceiling. Profiles:
#   plain        plaintext broker (docker/compose.<b>.yaml)
#   tls-prod     secure broker, mTLS + auth, production durability
#   tls-strict   secure broker, mTLS + auth, sync every message to disk
#
# MATRIX_LABEL names the run (default go-matrix-<date>), MATRIX_RATES the
# paced targets (default "20000 50000 100000 0", 0 being unpaced).
#
# It starts and removes each backend's brokers under the project names
# tpp-streams-live-<b> and tpp-streams-e2e-<b>, so run nothing else that uses
# those (the live and e2e test suites do) while it runs.
set -uo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
docker_dir="$root/docker"
bench_dir="$root/benches"
results="${BENCH_RESULTS:-$bench_dir/results.jsonl}"
export BENCH_RESULTS="$results"
label="${MATRIX_LABEL:-go-matrix-$(date +%Y%m%d-%H%M)}"
rates=(${MATRIX_RATES:-20000 50000 100000 0})

# backend -> "<compose name> <scenarios>". A function, not an associative
# array, because macOS ships bash 3.2.
spec() {
  case "$1" in
    zeromq) echo "- fanout batch" ;;
    nats) echo "nats fanout batch group" ;;
    jetstream) echo "nats fanout batch durable" ;;
    redis) echo "redis fanout batch" ;;
    redis-durable) echo "redis fanout batch durable" ;;
    kafka) echo "kafka fanout batch durable group" ;;
    rabbitmq) echo "rabbitmq fanout batch durable" ;;
    mqtt) echo "mqtt fanout batch durable group" ;;
    opcua) echo "opcua fanout batch" ;;
    opcua-pubsub) echo "mqtt fanout batch group" ;;
    *) echo "unknown backend $1" >&2; return 2 ;;
  esac
}
order=(zeromq nats jetstream redis redis-durable kafka rabbitmq mqtt opcua opcua-pubsub)
[ $# -gt 0 ] && order=("$@")

# Any failure, startup or run, is recorded here and becomes the exit status
# once everything is torn down, so cleanup that succeeds cannot hide a run
# that did not.
failed=0
fail() { echo "FAILED: $*" >&2; failed=1; }

# The compose name whose brokers are up, torn down on exit or interrupt.
current=-
up() {   # compose-name profile
  local c="$1" p="$2"
  current="$c"
  case "$p" in
    plain) docker compose -p "tpp-streams-live-$c" -f "$docker_dir/compose.$c.yaml" up -d --wait >/dev/null ;;
    tls-prod) if [ "$c" = opcua ]; then bash "$docker_dir/compose.opcua.secure.sh" >/dev/null 2>&1; fi
      STREAMS_DURABILITY=prod docker compose -p "tpp-streams-e2e-$c" -f "$docker_dir/compose.$c.secure.yaml" up -d --wait --force-recreate >/dev/null ;;
    tls-strict) STREAMS_DURABILITY=strict docker compose -p "tpp-streams-e2e-$c" -f "$docker_dir/compose.$c.secure.yaml" up -d --wait --force-recreate >/dev/null ;;
  esac
}

down() { # compose-name
  docker compose -p "tpp-streams-live-$1" -f "$docker_dir/compose.$1.yaml" down -v >/dev/null 2>&1
  docker compose -p "tpp-streams-e2e-$1" -f "$docker_dir/compose.$1.secure.yaml" down -v >/dev/null 2>&1
  current=-
}
cleanup() {
  [ "$current" != - ] && down "$current"
  rm -rf "$bin_dir"
}
bin_dir="$(mktemp -d "${TMPDIR:-/tmp}/tpp-streams-bench.XXXXXX")"
trap cleanup EXIT
trap 'fail "interrupted"; exit 130' INT TERM

# Arguments a backend needs beyond the common ones. The OPC UA backends write
# typed Int64 values instead of protobuf messages.
extra() { # backend
  case "$1" in opcua | opcua-pubsub) echo "--message opcua" ;; *) echo "" ;; esac
}

# Publishers that keep enough publishes in flight. Every confirmed publish waits
# about a round trip, so 32 publishers cap a backend near 32k/s whatever it
# could do; batches need far fewer because each call carries --batch messages.
publishers() { # backend scenario
  if [ "$2" = batch ]; then echo 32; return; fi
  case "$1" in mqtt|kafka) echo 512 ;; *) echo 256 ;; esac
}

run() {  # backend profile scenario rate
  local backend="$1" profile="$2" scenario="$3" rate="$4" messages
  if [ "$rate" = 0 ]; then messages=500000; else messages=$(( rate * 5 > 100000 ? rate * 5 : 100000 )); fi
  # Syncing every message to disk runs at hundreds to a few thousand a
  # second, so strict gets one unpaced measurement of a smaller run.
  [ "$profile" = tls-strict ] && messages=20000
  local cli_profile="$profile"; [ "$profile" != plain ] && cli_profile=tls
  local consumers=1; { [ "$scenario" = group ] || [ "$scenario" = durable ]; } && consumers=2
  printf '%-14s %-10s %-8s %7s/s ... ' "$backend" "$profile" "$scenario" "$rate"
  "$bin_dir/throughput" \
    --backend "$backend" --profile "$cli_profile" --scenario "$scenario" \
    --messages "$messages" --rate "$rate" --publishers "$(publishers "$backend" "$scenario")" --batch 500 --consumers "$consumers" \
    $(extra "$backend") --timeout 180 --label "$label/$profile" >/dev/null 2>"$bench_dir/last.log" \
    || { echo "FAIL (no result; see benches/last.log)"; fail "$backend $profile $scenario $rate"; return; }
  tail -1 "$results" | python3 -c '
import sys, json
d = json.loads(sys.stdin.read())
l = d["latency_ms"]
t = d["rate_target"]
held = d["ok"] and (t == 0 or d["delivered_rate"] >= 0.95 * t)
status = "OK  " if d["ok"] else "FAIL"
line = "%s %s delivered %10.0f/s  lost %d  dup %d  p50 %s p99 %s ms" % (
    status, "held  " if held else "missed", d["delivered_rate"], d["lost"], d["duplicates"], l["p50"], l["p99"])
if d["first_error"]:
    line += "  err: " + d["first_error"]
print(line)
sys.exit(0 if d["ok"] else 1)' || fail "$backend $profile $scenario $rate"
}

cd "$root"
# Built once, so no run pays for compiling and every run measures one binary.
go build -tags live -o "$bin_dir/throughput" ./benches/throughput || { fail "build"; exit 1; }
for backend in "${order[@]}"; do
  line="$(spec "$backend")" || { fail "unknown backend $backend"; continue; }
  read -r compose scenarios <<<"$line"
  profiles=(plain)
  [ "$compose" != - ] && profiles+=(tls-prod tls-strict)
  # The OPC PLC simulator has no durability setting, so strict would only repeat tls-prod.
  [ "$backend" = opcua ] && profiles=(plain tls-prod)
  for profile in "${profiles[@]}"; do
    if [ "$compose" != - ]; then
      down "$compose"
      up "$compose" "$profile" || { fail "starting $compose ($profile)"; down "$compose"; continue; }
    fi
    for scenario in $scenarios; do
      profile_rates=("${rates[@]}")
      [ "$profile" = tls-strict ] && profile_rates=(0)
      for rate in "${profile_rates[@]}"; do
        run "$backend" "$profile" "$scenario" "$rate"
      done
    done
  done
  [ "$compose" != - ] && down "$compose"
done
echo "results: $results (label $label)"
exit "$failed"
