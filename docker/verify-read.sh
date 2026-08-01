#!/usr/bin/env bash
#
# Operational verification for the ADBE write cluster + read replica node,
# driven entirely through docker compose. It builds the images, brings up the
# full topology (3 write members + 1 read replica node), and exercises the
# cases that occur in operation:
#
#   1. Cold start: all write nodes and the read node become healthy.
#   2. Write path: the remote client smoke test commits a credit + transfer.
#   3. Read-after-write over the live log (no snapshot required): the read node
#      reflects both sides of a transfer, the supply, a batch, and a missing
#      account.
#   4. Live-log increment: a later write reaches the read node without any
#      snapshot being taken.
#   5. Malformed HTTP requests are rejected (400/404) without crashing the node.
#   6. Read node restart: it re-syncs from the log and serves correct data,
#      independently of the write cluster.
#   7. Write node restart: the cluster keeps committing (quorum) and the read
#      node keeps serving.
#   8. Read decoupling: with the read node stopped, writes still commit (the
#      read node is not a Raft member and does not affect quorum); on restart it
#      catches up.
#
# Usage:
#   bash docker/verify-read.sh           # run and tear down on completion
#   KEEP=1 bash docker/verify-read.sh    # leave the stack running afterwards
#
# Requires: docker (with the compose plugin) and curl on the host.

set -euo pipefail

# --- Locate the repository root (parent of this script's directory) ---------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT}"

COMPOSE="docker compose"
READ_BASE="http://localhost:8080"
KEEP="${KEEP:-0}"

# --- Logging helpers ---------------------------------------------------------
log() { printf '\n\033[1;34m[verify]\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32m[ PASS ]\033[0m %s\n' "$*"; }
die() {
    printf '\033[1;31m[ FAIL ]\033[0m %s\n' "$*" >&2
    log "Dumping read node logs for diagnosis:"
    ${COMPOSE} logs --tail=80 adbe-read-0 >&2 || true
    exit 1
}

command -v curl >/dev/null 2>&1 || { echo "curl is required on the host" >&2; exit 1; }

# --- HTTP helpers ------------------------------------------------------------
http_body() { curl -s --max-time 5 "$1" || true; }
http_body_post() { curl -s --max-time 5 -H 'Content-Type: application/json' -d "$2" "$1" || true; }
http_code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1" || echo 000; }

# await_contains <url> <substring> [timeout_s]
await_contains() {
    local url="$1" want="$2" timeout="${3:-30}"
    local deadline=$(( $(date +%s) + timeout )) body=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        body="$(http_body "$url")"
        case "$body" in
            *"$want"*) return 0 ;;
        esac
        sleep 0.5
    done
    die "timed out waiting for ${url} to contain '${want}'; last body: ${body}"
}

# await_contains_post <url> <body> <substring> [timeout_s]
await_contains_post() {
    local url="$1" req="$2" want="$3" timeout="${4:-30}"
    local deadline=$(( $(date +%s) + timeout )) body=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        body="$(http_body_post "$url" "$req")"
        case "$body" in
            *"$want"*) return 0 ;;
        esac
        sleep 0.5
    done
    die "timed out waiting for POST ${url} to contain '${want}'; last body: ${body}"
}

# await_health <url> [timeout_s] - wait until GET returns 200
await_health() {
    local url="$1" timeout="${2:-90}"
    local deadline=$(( $(date +%s) + timeout )) code=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        code="$(http_code "$url")"
        [ "$code" = "200" ] && return 0
        sleep 1
    done
    die "timed out waiting for ${url} to return 200 (last code: ${code})"
}

# run_client <client_id> - run the remote client smoke test; returns its output.
# The client exits non-zero once the cluster state is no longer fresh (its
# built-in balance assertions assume a brand-new cluster), but the commands it
# submits are still committed. Callers decide whether a non-zero exit is fatal.
run_client() {
    ${COMPOSE} run --rm -e ADBE_CLIENT_ID="$1" client 2>&1 || true
}

# --- Cleanup -----------------------------------------------------------------
cleanup() {
    if [ "${KEEP}" = "1" ]; then
        log "KEEP=1: leaving the stack running. Tear down with: docker compose down"
        return 0
    fi
    log "Tearing down..."
    ${COMPOSE} down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# =============================================================================
log "Step 0: clean slate + build + up (3 write members + 1 read replica node)"
${COMPOSE} down --remove-orphans >/dev/null 2>&1 || true
${COMPOSE} up -d --build
pass "stack started"

log "Step 1: await health of all write nodes and the read node"
await_health "http://localhost:9100/healthz" 90
await_health "http://localhost:9101/healthz" 90
await_health "http://localhost:9102/healthz" 90
await_health "${READ_BASE}/healthz" 90
pass "all write nodes (9100-9102) and read node (8080) are healthy"

log "Step 2: remote client smoke test (CREDIT 100 +500, TRANSFER 150 100->200)"
out="$(run_client 1)"
echo "${out}" | grep -q "status=SUCCESS" || die "client smoke test did not report SUCCESS:\n${out}"
pass "write cluster committed credit + transfer"

log "Step 3: read-after-write via live log (no snapshot required)"
await_contains "${READ_BASE}/balance/100" '"balance":350' 30
await_contains "${READ_BASE}/balance/200" '"balance":150' 30
await_contains "${READ_BASE}/supply" '"totalSupply":500' 30
await_contains "${READ_BASE}/balance/999" '"exists":false' 30
await_contains_post "${READ_BASE}/balances" '{"ids":[100,200,999]}' '"account":999,"exists":false' 30
pass "read node reflects both sides of the transfer, supply, batch, and missing account"

log "Step 4: live-log increment (a later write reaches the read node, no snapshot)"
# A second client (distinct id => distinct command ids) commits one more credit
# of 500 before its fresh-state assertion fails, so total supply becomes 1000.
run_client 2 >/dev/null
await_contains "${READ_BASE}/supply" '"totalSupply":1000' 30
pass "post-start write delivered to the read node via the live log (supply=1000)"

log "Step 5: malformed HTTP requests are rejected without crashing the node"
[ "$(http_code "${READ_BASE}/nope")" = "404" ] || die "expected 404 for unknown route"
[ "$(http_code "${READ_BASE}/balance/not-a-number")" = "400" ] || die "expected 400 for non-numeric id"
[ "$(http_code "${READ_BASE}/allowance/1")" = "400" ] || die "expected 400 for missing delegate"
[ "$(http_code "${READ_BASE}/allowance/x/y")" = "400" ] || die "expected 400 for non-numeric allowance"
[ "$(http_code "${READ_BASE}/healthz")" = "200" ] || die "read node unhealthy after malformed requests"
pass "malformed requests rejected (404/400); node still healthy"

log "Step 6: restart the read node; it re-syncs from the log independently"
${COMPOSE} restart adbe-read-0
await_health "${READ_BASE}/healthz" 90
await_contains "${READ_BASE}/supply" '"totalSupply":1000' 30
pass "read node restarted, re-synced, and serves correct data (supply=1000)"

log "Step 7: restart a write node; the cluster keeps committing and reads keep serving"
${COMPOSE} restart adbe-node-2
await_health "http://localhost:9102/healthz" 90
# A third client commits one more credit (supply -> 1500) while node-2 is back;
# quorum (nodes 0 and 1) was maintained throughout the restart.
run_client 3 >/dev/null
await_contains "${READ_BASE}/supply" '"totalSupply":1500' 30
pass "write node restarted; writes still commit and reads still serve (supply=1500)"

log "Step 8: stop the read node; writes still commit (read node does not affect quorum)"
${COMPOSE} stop adbe-read-0
out="$(run_client 4)"
echo "${out}" | grep -q "status=SUCCESS" \
    || die "writes failed while read node was down (quorum should be unaffected):\n${out}"
pass "writes commit with the read node down (supply should be 2000)"
${COMPOSE} start adbe-read-0
await_health "${READ_BASE}/healthz" 90
await_contains "${READ_BASE}/supply" '"totalSupply":2000' 30
pass "read node restarted and caught up (supply=2000)"

log "ALL OPERATIONAL CHECKS PASSED"
