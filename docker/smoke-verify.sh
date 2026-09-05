#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke verification of the containerized LEDGERD topology:
#   1. build the image
#   2. start the 3-node write cluster + one read replica
#   3. drive a deterministic load and assert the read replica converges
#   4. trigger a snapshot (best-effort)
#   5. kill node 0 and assert the surviving 2-node quorum still commits and the
#      read replica fails over to another member's Archive (ADR 0008)
#
# The load scenario credits account 100 with N units then transfers N unit legs
# to account 200, so after each phase balance(100)=0, balance(200)=N, supply=N.

cd "$(dirname "$0")/.."

TOTAL_A=10000
TOTAL_B=1000
COMPOSE_FILE="docker/docker-compose.yml"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

on_error() {
  echo "" >&2
  echo "smoke verification FAILED." >&2
  echo "Containers are left running for inspection:" >&2
  compose ps >&2 || true
  echo "To clean up: docker compose -f $COMPOSE_FILE down -v" >&2
}
trap on_error ERR

wait_for_http() {
  local url=$1
  local deadline=$((SECONDS + 120))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then
      echo "timeout waiting for $url" >&2
      return 1
    fi
    sleep 2
  done
}

echo "==> build image"
compose build

echo "==> start 3-node cluster + read replica"
compose up -d ledgerd-node-0 ledgerd-node-1 ledgerd-node-2 ledgerd-read-0

echo "==> wait for node metrics endpoints"
wait_for_http "http://localhost:9100/healthz"
wait_for_http "http://localhost:9101/healthz"
wait_for_http "http://localhost:9102/healthz"

echo "==> wait for leader election"
sleep 10

echo "==> phase A: drive ${TOTAL_A} unit transfers (3-node cluster)"
compose run --rm --no-deps -e LEDGERD_TOTAL="$TOTAL_A" ledgerd-client

echo "==> phase A: read replica converges to supply=${TOTAL_A}"
compose run --rm --no-deps \
  -e LEDGERD_EXPECT_SUPPLY="$TOTAL_A" \
  -e LEDGERD_EXPECT_BALANCE_A=0 \
  -e LEDGERD_EXPECT_BALANCE_B="$TOTAL_A" \
  ledgerd-readcheck

echo "==> trigger a snapshot on the leader (best-effort)"
snapshot_ok=false
for node in ledgerd-node-0 ledgerd-node-1 ledgerd-node-2; do
  out="$(compose exec -T "$node" java -cp "/app/lib/*" io.aeron.cluster.ClusterTool /data/cluster snapshot 2>&1 || true)"
  echo "$out"
  if echo "$out" | grep -q "SNAPSHOT applied successfully"; then
    snapshot_ok=true
    break
  fi
done
if [[ "$snapshot_ok" != "true" ]]; then
  echo "   (snapshot not applied; continuing)"
fi

echo "==> kill node 0 to exercise quorum 2/3 + read failover"
compose stop ledgerd-node-0

echo "==> wait for a new leader to be elected among the surviving nodes"
sleep 15

echo "==> phase B: drive ${TOTAL_B} more unit transfers (2 surviving nodes)"
compose run --rm --no-deps -e LEDGERD_TOTAL="$TOTAL_B" ledgerd-client

echo "==> phase B: read replica fails over and converges to supply=$((TOTAL_A + TOTAL_B))"
compose run --rm --no-deps \
  -e LEDGERD_EXPECT_SUPPLY="$((TOTAL_A + TOTAL_B))" \
  -e LEDGERD_EXPECT_BALANCE_A=0 \
  -e LEDGERD_EXPECT_BALANCE_B="$((TOTAL_A + TOTAL_B))" \
  ledgerd-readcheck

echo "==> smoke verification PASSED"
compose down -v
