#!/usr/bin/env bash
set -euo pipefail

# LEDGERD capacity-fill driver.
#
# Runs CONCURRENCY concurrent CapacityFillDriver processes that credit distinct
# accounts across LEDGERD_ASSETS assets (hot asset 0 + cold assets 1..N-1), then
# reconciles the server-side balance count and records the JVM heap growth so the
# capacity model in docs/CAPACITY-PLANNING.md can be validated against real usage.
#
# Configuration is read from /etc/ledgerd/bench.env (written by the Ansible
# bench role). Tunables:
#   LEDGERD_ASSETS         number of assets to touch            (default 32)
#   LEDGERD_HOT_ACCOUNTS   distinct accounts on the hot asset    (default 681000)
#   LEDGERD_COLD_ACCOUNTS  distinct accounts per cold asset      (default 1000)
#   LEDGERD_CONCURRENCY    number of concurrent clients          (default 4)

source /etc/ledgerd/bench.env

ASSETS="${LEDGERD_ASSETS:-32}"
HOT_ACCOUNTS="${LEDGERD_HOT_ACCOUNTS:-681000}"
COLD_ACCOUNTS="${LEDGERD_COLD_ACCOUNTS:-1000}"
CONCURRENCY="${LEDGERD_CONCURRENCY:-4}"
MAX_IN_FLIGHT="${LEDGERD_MAX_IN_FLIGHT:-1024}"
RESULTS_DIR="${LEDGERD_RESULTS_DIR:-/var/lib/ledgerd/bench}"
RESULTS_FILE="$RESULTS_DIR/fill-results.csv"

JAVA="${LEDGERD_JAVA:-/opt/jdk-21/bin/java}"
CP="${LEDGERD_CP:-/opt/ledgerd/examples/lib/*}"

mkdir -p "$RESULTS_DIR"

# Max of a named Prometheus metric across the node metric endpoints. Balance
# count is replicated state (same on every node); heap used is per-node JVM heap
# but all nodes run the same deterministic state machine, so the max reads the
# leader's most up-to-date number.
metric_max() {
  local metric=$1 max=0 value
  for endpoint in ${LEDGERD_NODE_METRICS//,/ }; do
    value="$(curl -s "http://$endpoint/metrics" | awk -v m="$metric" '$0 ~ "^" m " " {print $2; exit}')"
    value=${value:-0}
    ((value > max)) && max=$value
  done
  echo "$max"
}

balance_count() {
  metric_max ledgerd_balance_count
}

heap_used() {
  metric_max ledgerd_heap_used_bytes
}

# Fresh client-id range per run so a re-run does not collide with dedup state.
CLIENT_ID_STATE="$RESULTS_DIR/fill_client_id_state"
client_id_base=$(cat "$CLIENT_ID_STATE" 2>/dev/null || echo 2000)

# Distinct (asset, account) pairs credited by this run, summed across all clients.
expected_accounts=$((HOT_ACCOUNTS + (ASSETS - 1) * COLD_ACCOUNTS))

before_balance=$(balance_count)
before_heap=$(heap_used)

echo ""
echo "==> fill: ASSETS=$ASSETS HOT_ACCOUNTS=$HOT_ACCOUNTS COLD_ACCOUNTS=$COLD_ACCOUNTS CONCURRENCY=$CONCURRENCY"
echo "   before: balance_count=$before_balance heap_used=$before_heap"

pids=()
for ((i = 1; i <= CONCURRENCY; i++)); do
  log="$RESULTS_DIR/fill-client-$i.log"
  echo "   fill client $i -> $log"
  LEDGERD_CLIENT_ID="$((client_id_base + i))" \
    LEDGERD_CLIENT_INDEX="$((i - 1))" \
    LEDGERD_CLIENT_COUNT="$CONCURRENCY" \
    LEDGERD_ASSETS="$ASSETS" \
    LEDGERD_HOT_ACCOUNTS="$HOT_ACCOUNTS" \
    LEDGERD_COLD_ACCOUNTS="$COLD_ACCOUNTS" \
    LEDGERD_MAX_IN_FLIGHT="$MAX_IN_FLIGHT" \
    LEDGERD_INGRESS_ENDPOINTS="$LEDGERD_INGRESS_ENDPOINTS" \
    LEDGERD_EGRESS_ENDPOINT="$LEDGERD_EGRESS_ENDPOINT" \
    "$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" io.justrade.ledgerd.examples.CapacityFillDriver >"$log" 2>&1 &
  pids+=("$!")
done

fail=0
for pid in "${pids[@]}"; do
  wait "$pid" || fail=1
done
if ((fail)); then
  echo "   ERROR: one or more fill clients failed" >&2
  for ((i = 1; i <= CONCURRENCY; i++)); do
    echo "   --- fill client $i tail ---" >&2
    tail -n 5 "$RESULTS_DIR/fill-client-$i.log" >&2 || true
  done
  exit 1
fi

after_balance=$(balance_count)
after_heap=$(heap_used)
delta_balance=$((after_balance - before_balance))
delta_heap=$((after_heap - before_heap))

if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "timestamp,assets,hot_accounts,cold_accounts,concurrency,expected_accounts,balance_count_before,balance_count_after,balance_count_delta,heap_used_before,heap_used_after,heap_growth_bytes" >"$RESULTS_FILE"
fi
ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$ts,$ASSETS,$HOT_ACCOUNTS,$COLD_ACCOUNTS,$CONCURRENCY,$expected_accounts,$before_balance,$after_balance,$delta_balance,$before_heap,$after_heap,$delta_heap" >>"$RESULTS_FILE"

echo ""
echo "   after:  balance_count=$after_balance heap_used=$after_heap"
echo "   delta:  balance_count=$delta_balance (expected $expected_accounts) heap_growth=$delta_heap bytes"
echo ""
echo "==> fill complete. results: $RESULTS_FILE"

if ((delta_balance != expected_accounts)); then
  echo "   ERROR: balance count delta $delta_balance != expected $expected_accounts" >&2
  exit 1
fi

echo "   balance_count delta matches expected accounts."

# Persist a fresh client-id range so a re-run does not collide with dedup state.
client_id_base=$((client_id_base + CONCURRENCY))
echo "$client_id_base" >"$CLIENT_ID_STATE"
