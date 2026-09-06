#!/usr/bin/env bash
set -euo pipefail

# LEDGERD load/bench driver.
#
# Runs CONCURRENCY concurrent LoadGenerator processes against the deployed
# cluster, aggregates the latency summary into a CSV, then verifies the read
# replica converges to the expected total supply (CONCURRENCY * TOTAL).
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

JAVA="${LEDGERD_JAVA:-/opt/jdk-21/bin/java}"
CP="${LEDGERD_CP:-/opt/ledgerd/examples/lib/*}"

mkdir -p "$RESULTS_DIR"

# Extract one integer field from a LoadGenerator summary line. Latency fields
# carry a trailing "us" suffix, which the [0-9]+ capture naturally strips.
field() {
  sed -nE "s/.* ${2}=([0-9]+).*/\1/p" <<<"$1"
}

run_once() {
  local total=$1 max_in_flight=$2 concurrency=$3
  echo ""
  echo "==> run: TOTAL=$total MAX_IN_FLIGHT=$max_in_flight CONCURRENCY=$concurrency"

  local pids=()
  for ((i = 1; i <= concurrency; i++)); do
    local log="$RESULTS_DIR/client-$i.log"
    echo "   client $i -> $log"
    LEDGERD_CLIENT_ID="$i" \
      LEDGERD_TOTAL="$total" \
      LEDGERD_MAX_IN_FLIGHT="$max_in_flight" \
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
  local agg_submitted=0 agg_completed=0 agg_p50=0 agg_p99=0 agg_p999=0 agg_max=0 agg_bp=0 agg_lc=0

  for ((i = 1; i <= concurrency; i++)); do
    local line
    line="$(grep -oE 'load: .*' "$RESULTS_DIR/client-$i.log" | tail -n1)"
    if [[ -z "$line" ]]; then
      echo "   ERROR: no load summary in client-$i.log" >&2
      exit 1
    fi

    local s c p50 p99 p999 m bp lc
    s="$(field "$line" 'submitted')"
    c="$(field "$line" 'completed')"
    p50="$(field "$line" 'p50')"
    p99="$(field "$line" 'p99')"
    p999="$(field "$line" 'p99\.9')"
    m="$(field "$line" 'max')"
    bp="$(field "$line" 'backpressure')"
    lc="$(field "$line" 'leaderChanges')"

    echo "$ts,$total,$max_in_flight,$concurrency,$s,$c,$p50,$p99,$p999,$m,$bp,$lc" >>"$RESULTS_FILE"

    agg_submitted=$((agg_submitted + s))
    agg_completed=$((agg_completed + c))
    agg_bp=$((agg_bp + bp))
    agg_lc=$((agg_lc + lc))
    ((p50 > agg_p50)) && agg_p50=$p50
    ((p99 > agg_p99)) && agg_p99=$p99
    ((p999 > agg_p999)) && agg_p999=$p999
    ((m > agg_max)) && agg_max=$m
  done

  echo "   aggregate: submitted=$agg_submitted completed=$agg_completed"
  echo "              p50<=${agg_p50}us p99<=${agg_p99}us p99.9<=${agg_p999}us max=${agg_max}us"
  echo "              backpressure=$agg_bp leaderChanges=$agg_lc"

  local expected=$((total * concurrency))
  echo "==> verify read replica converges to supply=$expected"
  LEDGERD_EXPECT_SUPPLY="$expected" \
    LEDGERD_EXPECT_BALANCE_A=0 \
    LEDGERD_EXPECT_BALANCE_B="$expected" \
    LEDGERD_QUERY_HOST="$LEDGERD_QUERY_HOST" \
    LEDGERD_RESPONSE_HOST="$LEDGERD_RESPONSE_HOST" \
    "$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" io.justrade.ledgerd.examples.ReadCheck
}

if [[ -n "${SWEEP_IN_FLIGHT:-}" ]]; then
  IFS=',' read -ra values <<<"$SWEEP_IN_FLIGHT"
  for mif in "${values[@]}"; do
    run_once "$TOTAL" "$mif" "$CONCURRENCY"
  done
else
  run_once "$TOTAL" "$MAX_IN_FLIGHT" "$CONCURRENCY"
fi

echo ""
echo "==> bench complete. results: $RESULTS_FILE"
