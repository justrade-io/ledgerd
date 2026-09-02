#!/usr/bin/env sh
# One-shot verifier for the domain event journal (ADR 0011). Runs an
# EventJournalFollower against the cluster Archives, waits until it has observed
# recorded domain events, and exits 0 on success (printing a result line) or 1 on
# timeout. Profile-gated in docker-compose; invoked by docker/verify-read.sh.
#
# Recognised environment variables (see EventJournalVerifier):
#   LEDGERD_ARCHIVE_CHANNELS  comma-separated Archive control channels (preferred)
#   LEDGERD_ARCHIVE_CHANNEL   single Archive control channel (fallback)
#   LEDGERD_LOCAL_HOST        routable host for Archive call-backs (default: own IP)
#   LEDGERD_AERON_DIR         embedded media driver directory
#   LEDGERD_EVENT_MIN         minimum events to observe (default 1)
#   LEDGERD_EVENT_TIMEOUT_MS  wait budget in milliseconds (default 30000)
set -eu

if [ -z "${LEDGERD_ARCHIVE_CHANNELS:-}" ] && [ -z "${LEDGERD_ARCHIVE_CHANNEL:-}" ]; then
    echo "LEDGERD_ARCHIVE_CHANNELS (comma-separated) or LEDGERD_ARCHIVE_CHANNEL is required" >&2
    exit 1
fi

# Advertise an address routable from the Archive (this container's own IP); the
# Archive connects back to the follower's control-response and replay streams.
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export LEDGERD_LOCAL_HOST="${LEDGERD_LOCAL_HOST:-${CONTAINER_IP}}"
export LEDGERD_AERON_DIR="${LEDGERD_AERON_DIR:-/tmp/aeron-ledgerd-event-verifier}"

exec java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp '/opt/ledgerd/lib/*' \
    io.justrade.ledgerd.read.journal.EventJournalVerifier
