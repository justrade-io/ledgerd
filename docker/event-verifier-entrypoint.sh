#!/usr/bin/env sh
# One-shot verifier for the domain event journal (ADR 0011). Runs an
# EventJournalFollower against the cluster Archives, waits until it has observed
# recorded domain events, and exits 0 on success (printing a result line) or 1 on
# timeout. Profile-gated in docker-compose; invoked by docker/verify-read.sh.
#
# Recognised environment variables (see EventJournalVerifier):
#   ADBE_ARCHIVE_CHANNELS  comma-separated Archive control channels (preferred)
#   ADBE_ARCHIVE_CHANNEL   single Archive control channel (fallback)
#   ADBE_LOCAL_HOST        routable host for Archive call-backs (default: own IP)
#   ADBE_AERON_DIR         embedded media driver directory
#   ADBE_EVENT_MIN         minimum events to observe (default 1)
#   ADBE_EVENT_TIMEOUT_MS  wait budget in milliseconds (default 30000)
set -eu

if [ -z "${ADBE_ARCHIVE_CHANNELS:-}" ] && [ -z "${ADBE_ARCHIVE_CHANNEL:-}" ]; then
    echo "ADBE_ARCHIVE_CHANNELS (comma-separated) or ADBE_ARCHIVE_CHANNEL is required" >&2
    exit 1
fi

# Advertise an address routable from the Archive (this container's own IP); the
# Archive connects back to the follower's control-response and replay streams.
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export ADBE_LOCAL_HOST="${ADBE_LOCAL_HOST:-${CONTAINER_IP}}"
export ADBE_AERON_DIR="${ADBE_AERON_DIR:-/tmp/aeron-adbe-event-verifier}"

exec java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp '/opt/adbe/lib/*' \
    com.adbe.read.journal.EventJournalVerifier
