#!/usr/bin/env sh
# Starts one ADBE standby read node. It runs standalone (not a Raft member):
# connects to the write cluster's Aeron Archive, follows the consensus log
# (loading snapshots as they appear), and serves reads over HTTP.
#
# Recognised environment variables (see ReadServiceLauncher):
#   ADBE_ARCHIVE_CHANNEL   Archive control channel (required),
#                          e.g. aeron:udp?endpoint=write-node:20104
#   ADBE_LOCAL_HOST        routable host for Archive call-backs (control response
#                          + replays). Defaults to this container's own IP so the
#                          Archive on another container can connect back.
#   ADBE_HTTP_PORT         HTTP query port (default 8080)
#   ADBE_SNAPSHOT_POLL_MS  interval between snapshot polls (default 5000)
#   ADBE_LIVE_LOG          follow the consensus log (default true)
set -eu

: "${ADBE_ARCHIVE_CHANNEL:?ADBE_ARCHIVE_CHANNEL is required (e.g. aeron:udp?endpoint=write-node:20104)}"

# The Archive connects back to this node's control-response subscription and
# replays, so advertise an address routable from the Archive: this container's
# own IP on the docker network (an ephemeral port is chosen with :0).
CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export ADBE_LOCAL_HOST="${ADBE_LOCAL_HOST:-${CONTAINER_IP}}"

echo "Starting ADBE standby read node (http=${ADBE_HTTP_PORT:-8080}, archive=${ADBE_ARCHIVE_CHANNEL}, localHost=${ADBE_LOCAL_HOST})"
exec /opt/adbe/bin/adbe-read
