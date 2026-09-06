#!/usr/bin/env bash
set -euo pipefail

# LEDGERD batch load/bench driver.
#
# Runs CONCURRENCY concurrent BatchLoadGenerator processes against the deployed
# cluster, aggregates legs/s and batches/s throughput into a CSV, then verifies
# the read replica converges to the cumulative total supply.
#
# Configuration is read from /etc/ledgerd/bench.env (written by the Ansible
# bench role). Tunables:
#   LEDGERD_TOTAL                transfer legs per client        (default 1000000)
#   LEDGERD_LEGS_PER_BATCH       legs per transfer batch          (default 256)
#   LEDGERD_MAX_BATCH_IN_FLIGHT  in-flight batch window           (default 16)
#   LEDGERD_CONCURRENCY          number of concurrent clients     (default 4)
#   SWEEP_LEGS_PER_BATCH         optional comma list, e.g. "16,64,256,1024";
#                                when set, the whole run repeats per value.

source /etc/ledgerd/bench.env

TOTAL_LEGS="${LEDGERD_TOTAL:-1000000}"
LEGS_PER_BATCH="${LEDGERD_LEGS_PER_BATCH:-256}"
MAX_BATCH_IN_FLIGHT="${LEDGERD_MAX_BATCH_IN_FLIGHT:-16}"
CONCURRENCY="${LEDGERD_CONCURRENCY:-4}"
RESULTS_DIR="${LEDGERD_RESULTS_DIR:-/var/lib/ledgerd/bench}"
RESULTS_FILE="$RESULTS_DIR/batch-results.csv"
SUPPLY_STATE="$RESULTS_DIR/supply_state"

JAVA="${LEDGERD_JAVA:-/opt/jdk-21/bin/java}"
CP="${LEDGERD_CP:-/opt/ledgerd/examples/lib/*}"

mkdir -p "$RESULTS_DIR"

# Extract one integer field from a BatchLoadGenerator summary line. Latency
# fields carry a trailing "us" suffix, which the [0-9]+ capture strips.
field() {
  sed -nE "s/.* ${2}=([0-9]+).*/\1/p" <<<"$1"
}

# The cluster supply only grows; persist the running expected supply so both the
# single-command and batch harnesses continue correctly across re-runs.
expected_supply=$(cat "$SUPPLY_STATE" 2>/dev/null || echo 0)

# The engine dedups by (clientId, clientSeq); a restarted client with the same
# clientId reuses clientSeq from 0, so re-running would collide with the cached
# results. Give every run a fresh clientId range.
CLIENT_ID_STATE="$RESULTS_DIR/client_id_state"
client_id_base=$(cat "$CLIENT_ID_STATE" 2>/dev/null || echo 1000)

run_batch_once() {
  local total=$1 legs_per_batch=$2 max_bif=$3 concurrency=$4
  local num_batches=$((total / legs_per_batch))
  local actual_legs=$((num_batches * legs_per_batch))
  echo ""
  echo "==> batch: TOTAL=$total LEGS_PER_BATCH=$legs_per_batch MAX_BATCH_IN_FLIGHT=$max_bif CONCURRENCY=$concurrency"

  local pids=()
  for ((i = 1; i <= concurrency; i++)); do
    local log="$RESULTS_DIR/batch-client-$i.log"
    echo "   batch client $i -> $log"
    LEDGERD_CLIENT_ID="$((client_id_base + i))" \
      LEDGERD_TOTAL="$total" \
      LEDGERD_LEGS_PER_BATCH="$legs_per_batch" \
      LEDGERD_MAX_BATCH_IN_FLIGHT="$max_bif" \
      LEDGERD_INGRESS_ENDPOINTS="$LEDGERD_INGRESS_ENDPOINTS" \
      LEDGERD_EGRESS_ENDPOINT="$LEDGERD_EGRESS_ENDPOINT" \
      "$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
      --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
      -cp "$CP" io.justrade.ledgerd.examples.BatchLoadGenerator >"$log" 2>&1 &
    pids+=("$!")
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  if ((fail)); then
    echo "   ERROR: one or more batch clients failed" >&2
    for ((i = 1; i <= concurrency; i++)); do
      echo "   --- batch client $i tail ---" >&2
      tail -n 5 "$RESULTS_DIR/batch-client-$i.log" >&2 || true
    done
    exit 1
  fi

  if [[ ! -f "$RESULTS_FILE" ]]; then
    echo "timestamp,total_legs,legs_per_batch,max_batch_in_flight,concurrency,batches,legs,legs_per_sec,batches_per_sec,p50_us,p99_us,p999_us,max_us,backpressure,leader_changes" >"$RESULTS_FILE"
  fi

  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local agg_batches=0 agg_legs=0 agg_lps=0 agg_bps=0 agg_p50=0 agg_p99=0 agg_p999=0 agg_max=0 agg_bp=0 agg_lc=0

  for ((i = 1; i <= concurrency; i++)); do
    local line
    line="$(grep -oE 'batchload: .*' "$RESULTS_DIR/batch-client-$i.log" | tail -n1)"
    if [[ -z "$line" ]]; then
      echo "   ERROR: no batchload summary in batch-client-$i.log" >&2
      exit 1
    fi

    local b l lps bps p50 p99 p999 m bp lc
    b="$(field "$line" 'batches')"
    l="$(field "$line" 'legs')"
    lps="$(field "$line" 'legsPerSec')"
    bps="$(field "$line" 'batchesPerSec')"
    p50="$(field "$line" 'p50')"
    p99="$(field "$line" 'p99')"
    p999="$(field "$line" 'p99\.9')"
    m="$(field "$line" 'max')"
    bp="$(field "$line" 'backpressure')"
    lc="$(field "$line" 'leaderChanges')"

    echo "$ts,$total,$legs_per_batch,$max_bif,$concurrency,$b,$l,$lps,$bps,$p50,$p99,$p999,$m,$bp,$lc" >>"$RESULTS_FILE"

    agg_batches=$((agg_batches + b))
    agg_legs=$((agg_legs + l))
    agg_lps=$((agg_lps + lps))
    agg_bps=$((agg_bps + bps))
    agg_bp=$((agg_bp + bp))
    agg_lc=$((agg_lc + lc))
    ((p50 > agg_p50)) && agg_p50=$p50
    ((p99 > agg_p99)) && agg_p99=$p99
    ((p999 > agg_p999)) && agg_p999=$p999
    ((m > agg_max)) && agg_max=$m
  done

  echo "   aggregate: batches=$agg_batches legs=$agg_legs legsPerSec=$agg_lps batchesPerSec=$agg_bps"
  echo "              p50<=${agg_p50}us p99<=${agg_p99}us p99.9<=${agg_p999}us max=${agg_max}us"
  echo "              backpressure=$agg_bp leaderChanges=$agg_lc"

  expected_supply=$((expected_supply + concurrency * actual_legs))
  if [[ "${SKIP_READCHECK:-0}" == "1" ]]; then
    echo "==> skip readcheck (supply now $expected_supply)"
  else
    echo "==> verify read replica converges to supply=$expected_supply"
    LEDGERD_EXPECT_SUPPLY="$expected_supply" \
      LEDGERD_EXPECT_BALANCE_A=0 \
      LEDGERD_EXPECT_BALANCE_B="$expected_supply" \
      LEDGERD_QUERY_HOST="$LEDGERD_QUERY_HOST" \
      LEDGERD_RESPONSE_HOST="$LEDGERD_RESPONSE_HOST" \
      "$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
      --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
      -cp "$CP" io.justrade.ledgerd.examples.ReadCheck
  fi
}

if [[ -n "${SWEEP_LEGS_PER_BATCH:-}" ]]; then
  IFS=',' read -ra values <<<"$SWEEP_LEGS_PER_BATCH"
  for lpb in "${values[@]}"; do
    run_batch_once "$TOTAL_LEGS" "$lpb" "$MAX_BATCH_IN_FLIGHT" "$CONCURRENCY"
    echo "$expected_supply" >"$SUPPLY_STATE"
    client_id_base=$((client_id_base + CONCURRENCY))
    echo "$client_id_base" >"$CLIENT_ID_STATE"
  done
else
  run_batch_once "$TOTAL_LEGS" "$LEGS_PER_BATCH" "$MAX_BATCH_IN_FLIGHT" "$CONCURRENCY"
  echo "$expected_supply" >"$SUPPLY_STATE"
  client_id_base=$((client_id_base + CONCURRENCY))
  echo "$client_id_base" >"$CLIENT_ID_STATE"
fi

echo ""
echo "==> batch bench complete. results: $RESULTS_FILE"
