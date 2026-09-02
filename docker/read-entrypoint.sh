#!/usr/bin/env sh
# Starts one LEDGERD read replica node. It runs standalone (not a Raft member):
# connects to the write cluster's Aeron Archive, follows the consensus log
# (loading snapshots as they appear), and serves reads over HTTP.
#
# Recognised environment variables (see ReadServiceLauncher):
#   LEDGERD_ARCHIVE_CHANNELS  comma-separated Archive control channels, one per
#                          cluster member; the replica fails over across them
#                          (ADR 0008). Falls back to LEDGERD_ARCHIVE_CHANNEL.
#   LEDGERD_ARCHIVE_CHANNEL   single Archive control channel (legacy),
#                          e.g. aeron:udp?endpoint=write-node:20104
#   LEDGERD_LOCAL_HOST        routable host for Archive call-backs (control response
#                          + replays). Defaults to this container's own IP so the
#                          Archive on another container can connect back.
#   LEDGERD_HTTP_PORT         HTTP query port (default 8080)
#   LEDGERD_SNAPSHOT_POLL_MS  interval between snapshot polls (default 5000)
#   LEDGERD_LIVE_LOG          follow the consensus log (default true)
#   LEDGERD_AERON_DIR         embedded media driver directory (default /tmp/aeron-ledgerd-read)
set -eu

# At least one Archive endpoint must be configured (multi-endpoint preferred).
if [ -z "${LEDGERD_ARCHIVE_CHANNELS:-}" ] && [ -z "${LEDGERD_ARCHIVE_CHANNEL:-}" ]; then
    echo "LEDGERD_ARCHIVE_CHANNELS (comma-separated) or LEDGERD_ARCHIVE_CHANNEL is required" >&2
    exit 1
fi

# The Archive connects back to this node's control-response subscription and
# replays, so advertise an address routable from the Archive: this container's
# own IP on the docker network (an ephemeral port is chosen with :0).
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export LEDGERD_LOCAL_HOST="${LEDGERD_LOCAL_HOST:-${CONTAINER_IP}}"

# Absolute, container-local media driver directory so co-located replicas never
# share a driver dir; override with LEDGERD_AERON_DIR.
export LEDGERD_AERON_DIR="${LEDGERD_AERON_DIR:-/tmp/aeron-ledgerd-read}"

echo "Starting LEDGERD read replica node (http=${LEDGERD_HTTP_PORT:-8080}, archives=${LEDGERD_ARCHIVE_CHANNELS:-${LEDGERD_ARCHIVE_CHANNEL:-}}, localHost=${LEDGERD_LOCAL_HOST})"
exec /opt/ledgerd/bin/read
