#!/usr/bin/env bash
set -euo pipefail

# LEDGERD load/bench driver.
#
# Runs CONCURRENCY concurrent LoadGenerator processes against the deployed
# cluster, aggregates the latency summary into a CSV, merges the per-client
# latency histograms into a true combined percentile, verifies the read replica
# converges, and reconciles the server-side command counter against the load.
#
# Configuration is read from /etc/ledgerd/bench.env (written by the Ansible
# bench role). Tunables:
#   LEDGERD_TOTAL          commands per client          (default 100000)
#   LEDGERD_MAX_IN_FLIGHT  per-client in-flight window  (default 1024)
#   LEDGERD_CONCURRENCY    number of concurrent clients (default 4)
#   SWEEP_IN_FLIGHT        optional comma list, e.g. "128,512,1024,4096";
#                          when set, the whole run repeats per value.

source /etc/ledgerd/bench.env

TOTAL="${LEDGERD_TOTAL:-100000}"
MAX_IN_FLIGHT="${LEDGERD_MAX_IN_FLIGHT:-1024}"
CONCURRENCY="${LEDGERD_CONCURRENCY:-4}"
RESULTS_DIR="${LEDGERD_RESULTS_DIR:-/var/lib/ledgerd/bench}"
RESULTS_FILE="$RESULTS_DIR/results.csv"
MERGED_FILE="$RESULTS_DIR/merged-results.csv"
RECONCILE_FILE="$RESULTS_DIR/reconcile.csv"
SUMMARY_FILE="$RESULTS_DIR/summary.md"

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

# Format a throughput value with K/M suffix for the summary table.
fmt_rate() {
  local n=$1
  if ((n >= 1000000)); then
    awk -v n="$n" 'BEGIN { printf "%.2fM", n / 1000000 }'
  elif ((n >= 1000)); then
    awk -v n="$n" 'BEGIN { printf "%.0fK", n / 1000 }'
  else
    echo "$n"
  fi
}

# Format a microsecond latency value as us (sub-ms) or ms (>= 1ms).
fmt_latency() {
  local us=$1
  if ((us >= 1000)); then
    awk -v u="$us" 'BEGIN { printf "%.2fms", u / 1000 }'
  else
    echo "${us}us"
  fi
}

run_once() {
  local total=$1 max_in_flight=$2 concurrency=$3 expected_supply=$4
  echo ""
  echo "==> run: TOTAL=$total MAX_IN_FLIGHT=$max_in_flight CONCURRENCY=$concurrency"

  # Scoped per run so the aggregator only merges histograms from the clients below.
  rm -f "$RESULTS_DIR"/client-*.hlog

  local base_commands=0 base_dups=0 base_lc=0 base_bp=0
  if [[ -n "${LEDGERD_NODE_METRICS:-}" ]]; then
    read -r base_commands base_dups base_lc base_bp <<<"$(node_counters)"
  fi

  local pids=()
  for ((i = 1; i <= concurrency; i++)); do
    local log="$RESULTS_DIR/client-$i.log"
    echo "   client $i -> $log"
    LEDGERD_CLIENT_ID="$((client_id_base + i))" \
      LEDGERD_TOTAL="$total" \
      LEDGERD_MAX_IN_FLIGHT="$max_in_flight" \
      LEDGERD_HISTOGRAM_PATH="$RESULTS_DIR/client-$i.hlog" \
      LEDGERD_INGRESS_ENDPOINTS="$LEDGERD_INGRESS_ENDPOINTS" \
      LEDGERD_EGRESS_ENDPOINT="$LEDGERD_EGRESS_ENDPOINT" \
      "$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
      --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
      -cp "$CP" io.justrade.ledgerd.examples.LoadGenerator >"$log" 2>&1 &
    pids+=("$!")
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  if ((fail)); then
    echo "   ERROR: one or more load clients failed" >&2
    for ((i = 1; i <= concurrency; i++)); do
      echo "   --- client $i tail ---" >&2
      tail -n 5 "$RESULTS_DIR/client-$i.log" >&2 || true
    done
    exit 1
  fi

  if [[ ! -f "$RESULTS_FILE" ]]; then
    echo "timestamp,total,max_in_flight,concurrency,submitted,completed,p50_us,p99_us,p999_us,max_us,backpressure,leader_changes" >"$RESULTS_FILE"
  fi

  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local rows="" agg_submitted=0 agg_completed=0 agg_p50=0 agg_p99=0 agg_p999=0 agg_max=0 agg_bp=0 agg_lc=0 agg_rate=0

  for ((i = 1; i <= concurrency; i++)); do
    local line
    line="$(grep '"event":"summary"' "$RESULTS_DIR/client-$i.log" | tail -n1)"
    if [[ -z "$line" ]]; then
      echo "   ERROR: no summary in client-$i.log" >&2
      exit 1
    fi

    local s c p50 p99 p999 m bp lc rate
    s="$(jq -r '.submitted' <<<"$line")"
    c="$(jq -r '.completed' <<<"$line")"
    p50="$(jq -r '.p50_us' <<<"$line")"
    p99="$(jq -r '.p99_us' <<<"$line")"
    p999="$(jq -r '.p999_us' <<<"$line")"
    m="$(jq -r '.max_us' <<<"$line")"
    bp="$(jq -r '.backpressure' <<<"$line")"
    lc="$(jq -r '.leaderChanges' <<<"$line")"
    rate="$(jq -r '.commandsPerSec' <<<"$line")"

    rows+="$ts,$total,$max_in_flight,$concurrency,$s,$c,$p50,$p99,$p999,$m,$bp,$lc"$'\n'

    agg_submitted=$((agg_submitted + s))
    agg_completed=$((agg_completed + c))
    agg_bp=$((agg_bp + bp))
    agg_lc=$((agg_lc + lc))
    agg_rate=$((agg_rate + rate))
    ((p50 > agg_p50)) && agg_p50=$p50
    ((p99 > agg_p99)) && agg_p99=$p99
    ((p999 > agg_p999)) && agg_p999=$p999
    ((m > agg_max)) && agg_max=$m
  done

  # Append the whole run's rows in one write so the exporter never observes a
  # partially written row group mid-run.
  printf '%s' "$rows" >>"$RESULTS_FILE"

  echo "   aggregate: submitted=$agg_submitted completed=$agg_completed rate=${agg_rate}cmd/s"
  echo "              p50<=${agg_p50}us p99<=${agg_p99}us p99.9<=${agg_p999}us max=${agg_max}us"
  echo "              backpressure=$agg_bp leaderChanges=$agg_lc"

  merge_histograms "$total" "$max_in_flight" "$concurrency" "$agg_rate"

  if [[ -n "${LEDGERD_NODE_METRICS:-}" ]]; then
    reconcile "$total" "$max_in_flight" "$concurrency" "$base_commands" "$base_dups" "$base_lc" "$base_bp"
  else
    echo "   skip reconcile (LEDGERD_NODE_METRICS unset)"
  fi

  echo "==> verify read replica converges to supply=$expected_supply"
  LEDGERD_EXPECT_SUPPLY="$expected_supply" \
    LEDGERD_EXPECT_BALANCE_A=0 \
    LEDGERD_EXPECT_BALANCE_B="$expected_supply" \
    LEDGERD_QUERY_HOST="$LEDGERD_QUERY_HOST" \
    LEDGERD_RESPONSE_HOST="$LEDGERD_RESPONSE_HOST" \
    "$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" io.justrade.ledgerd.examples.ReadCheck
}

# Merges this run's per-client histograms into one combined percentile set and
# appends a row to the consolidated latency summary table.
merge_histograms() {
  local total=$1 max_in_flight=$2 concurrency=$3 rate=$4
  local merged
  local hgrm_name="inflight-$max_in_flight-rate-$(fmt_rate "$rate").hgrm"
  merged="$("$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" io.justrade.ledgerd.examples.BenchAggregator "$RESULTS_DIR" 'client-*.hlog' "$RESULTS_DIR/$hgrm_name")"

  if [[ ! -f "$MERGED_FILE" ]]; then
    echo "timestamp,total,max_in_flight,concurrency,samples,p50_us,p90_us,p95_us,p99_us,p999_us,p9999_us,max_us" >"$MERGED_FILE"
  fi
  local ts samples p50 p90 p95 p99 p999 p9999 m
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  samples="$(jq -r '.samples' <<<"$merged")"
  p50="$(jq -r '.p50_us' <<<"$merged")"
  p90="$(jq -r '.p90_us' <<<"$merged")"
  p95="$(jq -r '.p95_us' <<<"$merged")"
  p99="$(jq -r '.p99_us' <<<"$merged")"
  p999="$(jq -r '.p999_us' <<<"$merged")"
  p9999="$(jq -r '.p9999_us' <<<"$merged")"
  m="$(jq -r '.max_us' <<<"$merged")"
  echo "$ts,$total,$max_in_flight,$concurrency,$samples,$p50,$p90,$p95,$p99,$p999,$p9999,$m" >>"$MERGED_FILE"
  echo "   merged: samples=$samples p50=${p50}us p99=${p99}us p99.9=${p999}us max=${m}us"
  echo "   plot data: $RESULTS_DIR/$hgrm_name"
  rm -f "$RESULTS_DIR"/client-*.hlog

  printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$(fmt_rate "$rate")" \
    "$(fmt_latency "$p50")" "$(fmt_latency "$p90")" "$(fmt_latency "$p95")" \
    "$(fmt_latency "$p99")" "$(fmt_latency "$p999")" "$(fmt_latency "$p9999")" \
    "$(fmt_latency "$m")" >>"$SUMMARY_FILE"
}

# Reconciles the server-side command delta and read-replica integrity against
# the load driven by this run.
reconcile() {
  local total=$1 max_in_flight=$2 concurrency=$3 base_commands=$4 base_dups=$5 base_lc=$6 base_bp=$7
  local after_commands=0 after_dups=0 after_lc=0 after_bp=0
  read -r after_commands after_dups after_lc after_bp <<<"$(node_counters)"

  local applied integrity
  applied="$(read_metric ledgerd_read_applied_position)"
  integrity="$(read_metric ledgerd_read_integrity_failures)"

  local expected_commands=$(((total + 1) * concurrency))
  local delta_commands=$((after_commands - base_commands))
  local delta_dups=$((after_dups - base_dups))
  local delta_lc=$((after_lc - base_lc))
  local delta_bp=$((after_bp - base_bp))

  if [[ ! -f "$RECONCILE_FILE" ]]; then
    echo "timestamp,total,max_in_flight,concurrency,expected_commands,server_commands_delta,server_duplicates,server_leader_elections,server_backpressure,read_applied_position,read_integrity_failures" >"$RECONCILE_FILE"
  fi
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "$ts,$total,$max_in_flight,$concurrency,$expected_commands,$delta_commands,$delta_dups,$delta_lc,$delta_bp,${applied:-0},${integrity:-0}" >>"$RECONCILE_FILE"

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

SUPPLY_STATE="$RESULTS_DIR/supply_state"

# The cluster supply only grows: each LoadGenerator run credits TOTAL per client,
# so every run (sweep iteration or single run) adds TOTAL * CONCURRENCY. Persist
# the running expected supply so re-running the harness continues correctly.
expected_supply=$(cat "$SUPPLY_STATE" 2>/dev/null || echo 0)

# The engine dedups by (clientId, clientSeq); a restarted client with the same
# clientId reuses clientSeq from 0, so re-running would collide with the cached
# results. Give every run a fresh clientId range.
CLIENT_ID_STATE="$RESULTS_DIR/client_id_state"
client_id_base=$(cat "$CLIENT_ID_STATE" 2>/dev/null || echo 1000)

# Consolidated latency table: one row per sweep iteration. The rate column is
# the achieved transfer throughput (commands/s).
printf '| rate (cmd/s) | p50 | p90 | p95 | p99 | p99.9 | p99.99 | max |\n' >"$SUMMARY_FILE"
printf '|---|---|---|---|---|---|---|---|\n' >>"$SUMMARY_FILE"

if [[ -n "${SWEEP_IN_FLIGHT:-}" ]]; then
  IFS=',' read -ra values <<<"$SWEEP_IN_FLIGHT"
  for mif in "${values[@]}"; do
    expected_supply=$((expected_supply + TOTAL * CONCURRENCY))
    run_once "$TOTAL" "$mif" "$CONCURRENCY" "$expected_supply"
    echo "$expected_supply" >"$SUPPLY_STATE"
    client_id_base=$((client_id_base + CONCURRENCY))
    echo "$client_id_base" >"$CLIENT_ID_STATE"
  done
else
  expected_supply=$((expected_supply + TOTAL * CONCURRENCY))
  run_once "$TOTAL" "$MAX_IN_FLIGHT" "$CONCURRENCY" "$expected_supply"
  echo "$expected_supply" >"$SUPPLY_STATE"
  client_id_base=$((client_id_base + CONCURRENCY))
  echo "$client_id_base" >"$CLIENT_ID_STATE"
fi

echo ""
echo "==> latency summary:"
cat "$SUMMARY_FILE"
echo ""
echo "==> bench complete. results: $RESULTS_FILE"
