#!/usr/bin/env bash
#
# Operational verification for the ADBE write cluster + read replica node,
# driven entirely through docker compose. It builds the images, brings up the
# full topology (3 write members + 1 read replica node + 1 risk service), and
# exercises the cases that occur in operation:
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
#   9. Snapshot load: a cluster snapshot is triggered, then the read node is
#      restarted; starting fresh it discovers and loads the service snapshot from
#      the archive (advance-only, skipping cluster framing) and serves correct
#      data, proving the snapshot load path end to end.
#   10. Archive failover (ADR 0008): kill the write member whose Archive the read
#      node follows (node 0); the cluster keeps committing (quorum 2 of 3) and
#      the read node fails over to a surviving member's Archive and converges.
#   11. Multi-asset + holds (ADR 0009, 0010): a scenario client writes on assets 1
#       and 2 (credit/transfer plus reserve/capture/release), and the read node
#       serves the correct per-asset available balances and conserved supply via
#       the ?asset= query parameter. Held funds are inferred from the drop in
#       available balance together with conserved supply.
#   12. Domain event journal (ADR 0011): with the journal enabled on every write
#       member, a follower (the event-verifier) observes the recorded semantic
#       event stream, failing over across members, proving the journal is
#       recorded and consumable end to end.
#   13. AI risk substrate (ADR 0012): the always-on risk service follows the same
#       event journal (failing over across members) and turns it into live risk
#       signals - it exposes per-account velocity scores, the money-flow graph
#       edges, and the dashboard over HTTP, proving the fan-out consumer works in
#       the deployment.
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
RISK_BASE="http://localhost:8090"
KEEP="${KEEP:-0}"

# --- Logging helpers ---------------------------------------------------------
log() { printf '\n\033[1;34m[verify]\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32m[ PASS ]\033[0m %s\n' "$*"; }
die() {
    printf '\033[1;31m[ FAIL ]\033[0m %s\n' "$*" >&2
    log "Dumping read + risk node logs for diagnosis:"
    ${COMPOSE} logs --tail=80 adbe-read-0 >&2 || true
    ${COMPOSE} logs --tail=80 adbe-risk-0 >&2 || true
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

# run_client_scenario <client_id> <scenario> - run the remote client with a named
# scenario (see RemoteClientExample). Used to drive multi-asset + holds commands.
run_client_scenario() {
    ${COMPOSE} run --rm -e ADBE_CLIENT_ID="$1" -e ADBE_SCENARIO="$2" client 2>&1 || true
}

# run_event_verifier - run the one-shot domain event journal follower verifier;
# returns its output. It prints 'EVENT JOURNAL VERIFIED' and exits 0 once it
# observes recorded events across the members' Archives (ADR 0011).
run_event_verifier() {
    ${COMPOSE} run --rm event-verifier 2>&1 || true
}

# trigger_snapshot - trigger a cluster snapshot via ClusterTool on every member.
# Only the leader can take a snapshot; followers report 'not the leader' and are
# ignored. The --add-opens flags are required by Agrona (matches the launcher).
# Fails if no member applied a snapshot.
trigger_snapshot() {
    local out="" applied=""
    for n in 0 1 2; do
        out="$(${COMPOSE} exec -T "adbe-node-${n}" java \
            --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
            --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
            -cp '/opt/adbe/lib/*' io.aeron.cluster.ClusterTool /var/adbe/cluster snapshot 2>&1 || true)"
        echo "${out}" | grep -q "SNAPSHOT applied successfully" && applied="yes"
    done
    [ -n "${applied}" ] || die "no cluster member applied a snapshot (leader trigger failed)"
}

# await_log_contains <service> <substring> [timeout_s] - wait until a service's
# docker logs contain a substring.
await_log_contains() {
    local svc="$1" want="$2" timeout="${3:-30}"
    local deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if ${COMPOSE} logs --no-color "$svc" 2>/dev/null | grep -q "$want"; then
            return 0
        fi
        sleep 1
    done
    die "timed out waiting for ${svc} logs to contain '${want}'"
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
log "Step 0: clean slate + build + up (3 write members + 1 read replica node + 1 risk service)"
${COMPOSE} down --remove-orphans >/dev/null 2>&1 || true
${COMPOSE} up -d --build
# The client is profile-gated and started via 'compose run', which reuses a
# cached image; build it explicitly so scenario runs pick up the latest code.
# The event-verifier is likewise profile-gated; build it too.
${COMPOSE} build client event-verifier
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

log "Step 9: snapshot load - trigger a cluster snapshot, restart the read node, it loads the snapshot"
# Commit one more credit (supply -> 2500) so the snapshot captures real state.
run_client 5 >/dev/null
await_contains "${READ_BASE}/supply" '"totalSupply":2500' 30
# Trigger a snapshot on the leader (ClusterTool on every member; followers report
# 'not the leader' and are ignored).
trigger_snapshot
pass "cluster snapshot triggered on the leader"
# Restart the read node: starting fresh (applied position 0) it discovers the
# service snapshot on the archive, loads it (advance-only, skipping the cluster
# framing), and serves the snapshotted state. The 'snapshot loaded' log proves
# the snapshot path was exercised, and the supply proves no clobber occurred.
${COMPOSE} restart adbe-read-0
await_health "${READ_BASE}/healthz" 90
await_log_contains adbe-read-0 "snapshot loaded" 30
await_contains "${READ_BASE}/supply" '"totalSupply":2500' 30
pass "read node restarted, loaded the service snapshot, and serves correct data (supply=2500)"

log "Step 10: archive failover (ADR 0008) - kill node 0, the read node's archive source"
# node 0 is first in ADBE_ARCHIVE_CHANNELS, so the read node follows its Archive.
# Killing it (SIGKILL => a crash, not a graceful stop) breaks that source; the
# write cluster keeps quorum (nodes 1 and 2) and the read node must fail over to
# a surviving member's Archive and keep converging.
${COMPOSE} kill adbe-node-0
# Commit one more credit (supply -> 3000) through the surviving quorum, retrying
# across the leader change triggered by node 0's death (same client id => the
# command is idempotent across retries).
committed=""
for _ in 1 2 3 4 5 6; do
    out="$(run_client 6)"
    if echo "${out}" | grep -q "status=SUCCESS"; then
        committed="yes"
        break
    fi
    sleep 2
done
[ -n "${committed}" ] || die "writes failed after killing node 0 (quorum 2 of 3 should survive)"
pass "write cluster still commits after node 0 dies (quorum survives)"
await_contains "${READ_BASE}/supply" '"totalSupply":3000' 60
await_health "${READ_BASE}/healthz" 60
pass "read node failed over to a surviving archive and converged (supply=3000)"

log "Step 11: multi-asset (ADR 0009) + holds (ADR 0010) end to end via ?asset="
# A dedicated scenario client writes on fresh accounts (700/701) and assets (1, 2),
# independent of the default-asset state above. Retry across any leader change left
# over from node 0's death in Step 10 (same client id => idempotent retries).
committed=""
for _ in 1 2 3 4 5 6; do
    out="$(run_client_scenario 7 multiasset)"
    if echo "${out}" | grep -q "multi-asset + holds scenario committed"; then
        committed="yes"
        break
    fi
    sleep 2
done
[ -n "${committed}" ] || die "multi-asset + holds scenario did not commit:\n${out}"
# Asset 1: plain multi-asset credit (1000) + transfer (400 of it, 700 -> 701).
await_contains "${READ_BASE}/balance/700?asset=1" '"balance":600' 30
await_contains "${READ_BASE}/balance/701?asset=1" '"balance":400' 30
await_contains "${READ_BASE}/supply?asset=1" '"totalSupply":1000' 30
# Asset 2: two-phase holds. credit 1000, reserve 300, capture 200 (700 -> 701),
# release 50 leaves account 700 available at 750 and account 701 at 200; supply is
# conserved at 1000 (reserved funds never left the total).
await_contains "${READ_BASE}/balance/700?asset=2" '"balance":750' 30
await_contains "${READ_BASE}/balance/701?asset=2" '"balance":200' 30
await_contains "${READ_BASE}/supply?asset=2" '"totalSupply":1000' 30
# Asset isolation: account 700 was never touched on the default asset 0.
await_contains "${READ_BASE}/balance/700?asset=0" '"exists":false' 30
await_contains "${READ_BASE}/balance/700" '"exists":false' 30
pass "multi-asset balances/supply and two-phase holds verified across assets 1 and 2"

log "Step 12: domain event journal (ADR 0011) - a follower observes recorded events"
# The event-verifier follows all three members' event streams (failing over from
# node 0, which was killed in Step 10) and exits 0 once it observes recorded
# domain events from the credits, transfers, and holds committed above.
out="$(run_event_verifier)"
echo "${out}" | grep -q "EVENT JOURNAL VERIFIED" \
    || die "event journal verifier did not observe recorded events:\n${out}"
pass "event journal follower observed recorded domain events"

log "Step 13: AI risk substrate (ADR 0012) - the risk dashboard scores the event stream"
# The always-on risk service follows the same event journal (failing over from
# node 0, killed in Step 10) and scores accounts. It must become healthy, expose
# the accounts touched above, surface the 100 -> 200 transfer as a graph edge, and
# serve the dashboard HTML.
await_health "${RISK_BASE}/healthz" 90
await_contains "${RISK_BASE}/risk/scores" '"account":100' 60
await_contains "${RISK_BASE}/risk/scores" '"account":200' 60
await_contains "${RISK_BASE}/risk/graph" '"from":100,"to":200' 60
await_contains "${RISK_BASE}/" 'ADBE Risk Substrate' 10
pass "risk service is healthy and exposes scores, the transfer graph edge, and the dashboard"

log "ALL OPERATIONAL CHECKS PASSED"
