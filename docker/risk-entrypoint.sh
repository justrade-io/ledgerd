#!/usr/bin/env sh
# Starts the ADBE AI risk service (ADR 0012). It runs standalone (not a Raft
# member): it follows the write cluster's recorded domain event journal (ADR
# 0011, stream 108) via an EventJournalFollower, scores accounts for transaction
# velocity and money-flow graph centrality, and serves the live risk dashboard
# over HTTP.
#
# Recognised environment variables (see RiskServiceLauncher):
#   ADBE_ARCHIVE_CHANNELS  comma-separated Archive control channels, one per
#                          cluster member; the follower fails over across them
#                          (ADR 0008). Falls back to ADBE_ARCHIVE_CHANNEL.
#   ADBE_ARCHIVE_CHANNEL   single Archive control channel (legacy),
#                          e.g. aeron:udp?endpoint=write-node:20104
#   ADBE_LOCAL_HOST        routable host for Archive call-backs (control response
#                          + replays). Defaults to this container's own IP so the
#                          Archive on another container can connect back.
#   ADBE_HTTP_PORT         dashboard HTTP port (default 8090)
#   ADBE_AERON_DIR         embedded media driver directory (default /tmp/aeron-adbe-risk)
set -eu

# At least one Archive endpoint must be configured (multi-endpoint preferred).
if [ -z "${ADBE_ARCHIVE_CHANNELS:-}" ] && [ -z "${ADBE_ARCHIVE_CHANNEL:-}" ]; then
    echo "ADBE_ARCHIVE_CHANNELS (comma-separated) or ADBE_ARCHIVE_CHANNEL is required" >&2
    exit 1
fi

# The Archive connects back to this service's control-response subscription and
# replays, so advertise an address routable from the Archive: this container's
# own IP on the docker network (an ephemeral port is chosen with :0).
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export ADBE_LOCAL_HOST="${ADBE_LOCAL_HOST:-${CONTAINER_IP}}"

# Absolute, container-local media driver directory; override with ADBE_AERON_DIR.
export ADBE_AERON_DIR="${ADBE_AERON_DIR:-/tmp/aeron-adbe-risk}"

echo "Starting ADBE risk service (http=${ADBE_HTTP_PORT:-8090}, archives=${ADBE_ARCHIVE_CHANNELS:-${ADBE_ARCHIVE_CHANNEL:-}}, localHost=${ADBE_LOCAL_HOST})"
exec /opt/adbe/bin/adbe-risk
