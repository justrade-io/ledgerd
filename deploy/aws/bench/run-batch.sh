#!/usr/bin/env bash
set -euo pipefail

# LEDGERD batch load/bench driver.
#
# Runs CONCURRENCY concurrent BatchLoadGenerator processes against the deployed
# cluster, aggregates legs/s and batches/s throughput into a CSV, merges the
# per-client batch-latency histograms into a true combined percentile, verifies
# the read replica converges, and reconciles the server-side command counter.
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
MERGED_FILE="$RESULTS_DIR/batch-merged-results.csv"
RECONCILE_FILE="$RESULTS_DIR/batch-reconcile.csv"
SUPPLY_STATE="$RESULTS_DIR/supply_state"

JAVA="${LEDGERD_JAVA:-/opt/jdk-21/bin/java}"
CP="${LEDGERD_CP:-/opt/ledgerd/examples/lib/*}"

mkdir -p "$RESULTS_DIR"

# Max of a named Prometheus metric across the node metric endpoints. The core
# counters are replicated state, so every node reports the same cumulative value;
# take the max to read the leader's (most up-to-date) number rather than 3x it.
metric_max() {
  local metric=$1 max=0 value
  for endpoint in ${LEDGERD_NODE_METRICS//,/ }; do
    value="$(curl -s "http://$endpoint/metrics" | awk -v m="$metric" '$0 ~ "^" m " " {print $2; exit}')"
    value=${value:-0}
    ((value > max)) && max=$value
  done
  echo "$max"
}

# Read a named Prometheus metric from the read replica endpoint.
read_metric() {
  local metric=$1
  curl -s "http://$LEDGERD_READ_METRICS/metrics" | awk -v m="$metric" '$0 ~ "^" m " " {print $2; exit}'
}

# Snapshot the four counters reconciled per run as a single whitespace line.
node_counters() {
  echo "$(metric_max ledgerd_commands_processed) $(metric_max ledgerd_duplicates_detected) \
$(metric_max ledgerd_leader_elections) $(metric_max ledgerd_backpressure_events)"
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

  # Scoped per run so the aggregator only merges histograms from the clients below.
  rm -f "$RESULTS_DIR"/batch-client-*.hlog

  local base_commands=0 base_dups=0 base_lc=0 base_bp=0
  if [[ -n "${LEDGERD_NODE_METRICS:-}" ]]; then
    read -r base_commands base_dups base_lc base_bp <<<"$(node_counters)"
  fi

  local pids=()
  for ((i = 1; i <= concurrency; i++)); do
    local log="$RESULTS_DIR/batch-client-$i.log"
    echo "   batch client $i -> $log"
    LEDGERD_CLIENT_ID="$((client_id_base + i))" \
      LEDGERD_TOTAL="$total" \
      LEDGERD_LEGS_PER_BATCH="$legs_per_batch" \
      LEDGERD_MAX_BATCH_IN_FLIGHT="$max_bif" \
      LEDGERD_HISTOGRAM_PATH="$RESULTS_DIR/batch-client-$i.hlog" \
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
  local rows="" agg_batches=0 agg_legs=0 agg_lps=0 agg_bps=0 agg_p50=0 agg_p99=0 agg_p999=0 agg_max=0 agg_bp=0 agg_lc=0

  for ((i = 1; i <= concurrency; i++)); do
    local line
    line="$(grep '"event":"summary"' "$RESULTS_DIR/batch-client-$i.log" | tail -n1)"
    if [[ -z "$line" ]]; then
      echo "   ERROR: no summary in batch-client-$i.log" >&2
      exit 1
    fi

    local b l lps bps p50 p99 p999 m bp lc
    b="$(jq -r '.batches' <<<"$line")"
    l="$(jq -r '.legs' <<<"$line")"
    lps="$(jq -r '.legsPerSec' <<<"$line")"
    bps="$(jq -r '.batchesPerSec' <<<"$line")"
    p50="$(jq -r '.p50_us' <<<"$line")"
    p99="$(jq -r '.p99_us' <<<"$line")"
    p999="$(jq -r '.p999_us' <<<"$line")"
    m="$(jq -r '.max_us' <<<"$line")"
    bp="$(jq -r '.backpressure' <<<"$line")"
    lc="$(jq -r '.leaderChanges' <<<"$line")"

    rows+="$ts,$total,$legs_per_batch,$max_bif,$concurrency,$b,$l,$lps,$bps,$p50,$p99,$p999,$m,$bp,$lc"$'\n'

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

  # Append the whole run's rows in one write so the exporter never observes a
  # partially written row group mid-run.
  printf '%s' "$rows" >>"$RESULTS_FILE"

  echo "   aggregate: batches=$agg_batches legs=$agg_legs legsPerSec=$agg_lps batchesPerSec=$agg_bps"
  echo "              p50<=${agg_p50}us p99<=${agg_p99}us p99.9<=${agg_p999}us max=${agg_max}us"
  echo "              backpressure=$agg_bp leaderChanges=$agg_lc"

  merge_histograms "$total" "$legs_per_batch" "$max_bif" "$concurrency" "$num_batches" "$actual_legs"

  if [[ -n "${LEDGERD_NODE_METRICS:-}" ]]; then
    reconcile "$legs_per_batch" "$num_batches" "$concurrency" "$base_commands" "$base_dups" "$base_lc" "$base_bp"
  else
    echo "   skip reconcile (LEDGERD_NODE_METRICS unset)"
  fi

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

# Merges this run's per-client histograms into one combined percentile set.
merge_histograms() {
  local total=$1 legs_per_batch=$2 max_bif=$3 concurrency=$4 num_batches=$5 actual_legs=$6
  local merged
  merged="$("$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" io.justrade.ledgerd.examples.BenchAggregator "$RESULTS_DIR" 'batch-client-*.hlog' "$RESULTS_DIR/batch-merged.hgrm")"

  if [[ ! -f "$MERGED_FILE" ]]; then
    echo "timestamp,total_legs,legs_per_batch,max_batch_in_flight,concurrency,batches,legs,samples,p50_us,p99_us,p999_us,max_us" >"$MERGED_FILE"
  fi
  local ts samples p50 p99 p999 m
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  samples="$(jq -r '.samples' <<<"$merged")"
  p50="$(jq -r '.p50_us' <<<"$merged")"
  p99="$(jq -r '.p99_us' <<<"$merged")"
  p999="$(jq -r '.p999_us' <<<"$merged")"
  m="$(jq -r '.max_us' <<<"$merged")"
  echo "$ts,$total,$legs_per_batch,$max_bif,$concurrency,$num_batches,$actual_legs,$samples,$p50,$p99,$p999,$m" >>"$MERGED_FILE"
  echo "   merged: samples=$samples p50=${p50}us p99=${p99}us p99.9=${p999}us max=${m}us"
  echo "   plot data: $RESULTS_DIR/batch-merged.hgrm"
  rm -f "$RESULTS_DIR"/batch-client-*.hlog
}

# Reconciles the server-side command delta and read-replica integrity against
# the batches driven by this run.
reconcile() {
  local legs_per_batch=$1 num_batches=$2 concurrency=$3 base_commands=$4 base_dups=$5 base_lc=$6 base_bp=$7
  local after_commands=0 after_dups=0 after_lc=0 after_bp=0
  read -r after_commands after_dups after_lc after_bp <<<"$(node_counters)"

  local applied integrity
  applied="$(read_metric ledgerd_read_applied_position)"
  integrity="$(read_metric ledgerd_read_integrity_failures)"

  local expected_commands=$(((num_batches + 1) * concurrency))
  local delta_commands=$((after_commands - base_commands))
  local delta_dups=$((after_dups - base_dups))
  local delta_lc=$((after_lc - base_lc))
  local delta_bp=$((after_bp - base_bp))

  if [[ ! -f "$RECONCILE_FILE" ]]; then
    echo "timestamp,legs_per_batch,concurrency,expected_commands,server_commands_delta,server_duplicates,server_leader_elections,server_backpressure,read_applied_position,read_integrity_failures" >"$RECONCILE_FILE"
  fi
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "$ts,$legs_per_batch,$concurrency,$expected_commands,$delta_commands,$delta_dups,$delta_lc,$delta_bp,${applied:-0},${integrity:-0}" >>"$RECONCILE_FILE"

  echo "   reconcile: expected_commands=$expected_commands server_commands_delta=$delta_commands"
  echo "              duplicates=$delta_dups leaderElections=$delta_lc serverBackpressure=$delta_bp"
  echo "              readAppliedPosition=$applied readIntegrityFailures=$integrity"

  if ((delta_commands != expected_commands)); then
    echo "   ERROR: server command delta $delta_commands != expected $expected_commands" >&2
    exit 1
  fi
  if [[ -n "$integrity" && "$integrity" != "0" ]]; then
    echo "   ERROR: read replica integrity failures $integrity != 0" >&2
    exit 1
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
