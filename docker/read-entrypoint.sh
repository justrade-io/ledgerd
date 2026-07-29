#!/usr/bin/env sh
# Starts one read-enabled ADBE node: a cluster follower hosting ReadModelService
# plus the HTTP query API. ReadServiceLauncher reads its configuration directly
# from the environment, so no properties file is generated here. Every member of
# a read cluster runs this same image (homogeneous services), which keeps
# snapshots byte-identical across members.
set -eu

: "${ADBE_NODE_ID:?ADBE_NODE_ID is required (0-based member id)}"
: "${ADBE_HOST:?ADBE_HOST is required (this node's advertised hostname)}"
: "${ADBE_CLUSTER_MEMBERS:?ADBE_CLUSTER_MEMBERS is required (Aeron member string)}"

ADBE_BASE_DIR="${ADBE_BASE_DIR:-/var/adbe}"
export ADBE_BASE_DIR
mkdir -p "${ADBE_BASE_DIR}"

echo "Starting ADBE read node ${ADBE_NODE_ID} as ${ADBE_HOST} (http=${ADBE_HTTP_PORT:-8080}, baseDir=${ADBE_BASE_DIR})"
exec /opt/adbe/bin/adbe-read
