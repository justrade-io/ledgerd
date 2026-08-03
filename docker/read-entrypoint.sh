#!/usr/bin/env sh
# Starts one ADBE read replica node. It runs standalone (not a Raft member):
# connects to the write cluster's Aeron Archive, follows the consensus log
# (loading snapshots as they appear), and serves reads over HTTP.
#
# Recognised environment variables (see ReadServiceLauncher):
#   ADBE_ARCHIVE_CHANNELS  comma-separated Archive control channels, one per
#                          cluster member; the replica fails over across them
#                          (ADR 0008). Falls back to ADBE_ARCHIVE_CHANNEL.
#   ADBE_ARCHIVE_CHANNEL   single Archive control channel (legacy),
#                          e.g. aeron:udp?endpoint=write-node:20104
#   ADBE_LOCAL_HOST        routable host for Archive call-backs (control response
#                          + replays). Defaults to this container's own IP so the
#                          Archive on another container can connect back.
#   ADBE_HTTP_PORT         HTTP query port (default 8080)
#   ADBE_SNAPSHOT_POLL_MS  interval between snapshot polls (default 5000)
#   ADBE_LIVE_LOG          follow the consensus log (default true)
#   ADBE_AERON_DIR         embedded media driver directory (default /tmp/aeron-adbe-read)
set -eu

# At least one Archive endpoint must be configured (multi-endpoint preferred).
if [ -z "${ADBE_ARCHIVE_CHANNELS:-}" ] && [ -z "${ADBE_ARCHIVE_CHANNEL:-}" ]; then
    echo "ADBE_ARCHIVE_CHANNELS (comma-separated) or ADBE_ARCHIVE_CHANNEL is required" >&2
    exit 1
fi

# The Archive connects back to this node's control-response subscription and
# replays, so advertise an address routable from the Archive: this container's
# own IP on the docker network (an ephemeral port is chosen with :0).
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export ADBE_LOCAL_HOST="${ADBE_LOCAL_HOST:-${CONTAINER_IP}}"

# Absolute, container-local media driver directory so co-located replicas never
# share a driver dir; override with ADBE_AERON_DIR.
export ADBE_AERON_DIR="${ADBE_AERON_DIR:-/tmp/aeron-adbe-read}"

echo "Starting ADBE read replica node (http=${ADBE_HTTP_PORT:-8080}, archives=${ADBE_ARCHIVE_CHANNELS:-${ADBE_ARCHIVE_CHANNEL:-}}, localHost=${ADBE_LOCAL_HOST})"
exec /opt/adbe/bin/adbe-read
