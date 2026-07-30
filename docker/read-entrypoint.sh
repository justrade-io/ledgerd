#!/usr/bin/env sh
# Starts one ADBE read node. Supports two modes via ADBE_MODE:
#
#   standby (default): Runs standalone; connects to the cluster's Aeron Archive,
#       downloads the latest snapshot, and serves reads over HTTP. Does not
#       participate in Raft consensus. Requires ADBE_ARCHIVE_CHANNEL.
#
#   cluster: Runs as a full Raft voting member hosting ReadModelService.
#       Requires ADBE_NODE_ID, ADBE_HOST, and ADBE_CLUSTER_MEMBERS.
set -eu

MODE="${ADBE_MODE:-standby}"

if [ "${MODE}" = "standby" ]; then
    : "${ADBE_ARCHIVE_CHANNEL:?ADBE_ARCHIVE_CHANNEL is required in standby mode (e.g. aeron:udp?endpoint=write-node:20104)}"
else
    : "${ADBE_NODE_ID:?ADBE_NODE_ID is required (0-based member id)}"
    : "${ADBE_HOST:?ADBE_HOST is required (this node's advertised hostname)}"
    : "${ADBE_CLUSTER_MEMBERS:?ADBE_CLUSTER_MEMBERS is required (Aeron member string)}"
fi

ADBE_BASE_DIR="${ADBE_BASE_DIR:-/var/adbe}"
export ADBE_BASE_DIR
mkdir -p "${ADBE_BASE_DIR}"

echo "Starting ADBE read node (mode=${MODE}, http=${ADBE_HTTP_PORT:-8080}, baseDir=${ADBE_BASE_DIR})"
exec /opt/adbe/bin/adbe-read
