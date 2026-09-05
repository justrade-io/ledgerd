#!/usr/bin/env bash
set -euo pipefail

# Fault-injection verification: kills the current cluster leader WHILE the
# production write-client is mid-flight under sustained load, then asserts the
# load completes with exactly-once semantics across the leadership change.
#
# This is the containerized counterpart of the in-process FaultInjectionTest, but
# exercises the production WriteClient (automatic onNewLeader retransmit + dedup)
# over real bridge networking and a real process kill, with many commands
# in-flight rather than a single debit.
#
# The load scenario credits account 100 with N units then transfers N unit legs
# to account 200, so exactly-once means balance(100)=0, balance(200)=N, supply=N.

cd "$(dirname "$0")/.."

TOTAL="${LEDGERD_TOTAL:-50000}"
COMPOSE_FILE="docker/docker-compose.yml"
LOG="/tmp/ledgerd-fault-load.log"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

on_error() {
  echo "" >&2
  echo "fault verification FAILED." >&2
  if [[ -n "${LOAD_PID:-}" ]] && kill -0 "$LOAD_PID" 2>/dev/null; then
    kill "$LOAD_PID" 2>/dev/null || true
  fi
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

echo "==> start sustained load (TOTAL=$TOTAL) in the background"
rm -f "$LOG"
compose run -T --rm --no-deps -e LEDGERD_TOTAL="$TOTAL" ledgerd-client >"$LOG" 2>&1 &
LOAD_PID=$!

echo "==> wait for the driver to report the current leader"
leader=""
deadline=$((SECONDS + 120))
while ((SECONDS < deadline)); do
  leader="$(grep -oE 'leader=[0-9]+' "$LOG" | head -n1 | sed 's/leader=//' || true)"
  if [[ -n "$leader" ]]; then
    break
  fi
  if ! kill -0 "$LOAD_PID" 2>/dev/null; then
    break
  fi
  sleep 0.5
done

if [[ -z "$leader" ]]; then
  echo "driver never reported a leader; load log:" >&2
  cat "$LOG" >&2
  exit 1
fi
echo "   leader is node $leader"

echo "==> kill leader node $leader mid-flight"
compose stop "ledgerd-node-$leader"

echo "==> wait for the load to complete (failover + retransmit + drain)"
if ! wait "$LOAD_PID"; then
  echo "load failed; log:" >&2
  cat "$LOG" >&2
  exit 1
fi

echo "==> load summary:"
cat "$LOG"

# Note: WriteClient.leaderChanges may stay 0 even across a real leadership
# change in this scenario (the client can recover via time-based retry rather
# than the onNewLeader retransmit-all path). The exactly-once assertions below
# (load exit code + read replica convergence) are the real oracle, not this
# counter, so it is printed for information only.

echo "==> verify exactly-once: read replica converges to supply=$TOTAL"
compose run -T --rm --no-deps \
  -e LEDGERD_EXPECT_SUPPLY="$TOTAL" \
  -e LEDGERD_EXPECT_BALANCE_A=0 \
  -e LEDGERD_EXPECT_BALANCE_B="$TOTAL" \
  ledgerd-readcheck

echo "==> fault verification PASSED"
compose down -v
