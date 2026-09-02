#!/usr/bin/env sh
# Starts the LEDGERD AI risk service (ADR 0012). It runs standalone (not a Raft
# member): it follows the write cluster's recorded domain event journal (ADR
# 0011, stream 108) via an EventJournalFollower, scores accounts for transaction
# velocity and money-flow graph centrality, and serves the live risk dashboard
# over HTTP.
#
# Recognised environment variables (see RiskServiceLauncher):
#   LEDGERD_ARCHIVE_CHANNELS  comma-separated Archive control channels, one per
#                          cluster member; the follower fails over across them
#                          (ADR 0008). Falls back to LEDGERD_ARCHIVE_CHANNEL.
#   LEDGERD_ARCHIVE_CHANNEL   single Archive control channel (legacy),
#                          e.g. aeron:udp?endpoint=write-node:20104
#   LEDGERD_LOCAL_HOST        routable host for Archive call-backs (control response
#                          + replays). Defaults to this container's own IP so the
#                          Archive on another container can connect back.
#   LEDGERD_HTTP_PORT         dashboard HTTP port (default 8090)
#   LEDGERD_AERON_DIR         embedded media driver directory (default /tmp/aeron-ledgerd-risk)
set -eu

# At least one Archive endpoint must be configured (multi-endpoint preferred).
if [ -z "${LEDGERD_ARCHIVE_CHANNELS:-}" ] && [ -z "${LEDGERD_ARCHIVE_CHANNEL:-}" ]; then
    echo "LEDGERD_ARCHIVE_CHANNELS (comma-separated) or LEDGERD_ARCHIVE_CHANNEL is required" >&2
    exit 1
fi

# The Archive connects back to this service's control-response subscription and
# replays, so advertise an address routable from the Archive: this container's
# own IP on the docker network (an ephemeral port is chosen with :0).
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export LEDGERD_LOCAL_HOST="${LEDGERD_LOCAL_HOST:-${CONTAINER_IP}}"

# Absolute, container-local media driver directory; override with LEDGERD_AERON_DIR.
export LEDGERD_AERON_DIR="${LEDGERD_AERON_DIR:-/tmp/aeron-ledgerd-risk}"

echo "Starting LEDGERD risk service (http=${LEDGERD_HTTP_PORT:-8090}, archives=${LEDGERD_ARCHIVE_CHANNELS:-${LEDGERD_ARCHIVE_CHANNEL:-}}, localHost=${LEDGERD_LOCAL_HOST})"
exec /opt/ledgerd/bin/risk
