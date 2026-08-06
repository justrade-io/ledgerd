#!/usr/bin/env sh
# Generates a per-node cluster.properties from environment variables and starts
# one ADBE cluster node via the launcher's fromProperties path. Keeping the
# cluster topology in the environment (compose) means the image stays generic.
set -eu

: "${ADBE_NODE_ID:?ADBE_NODE_ID is required (0-based member id)}"
: "${ADBE_HOST:?ADBE_HOST is required (this node's advertised hostname)}"
: "${ADBE_CLUSTER_MEMBERS:?ADBE_CLUSTER_MEMBERS is required (Aeron member string)}"

ADBE_BASE_DIR="${ADBE_BASE_DIR:-/var/adbe}"
ADBE_CLEAN_START="${ADBE_CLEAN_START:-true}"

mkdir -p "${ADBE_BASE_DIR}"
CONFIG_FILE="${ADBE_BASE_DIR}/cluster.properties"
{
    echo "adbe.clusterMembers=${ADBE_CLUSTER_MEMBERS}"
    echo "adbe.host=${ADBE_HOST}"
    echo "adbe.baseDir=${ADBE_BASE_DIR}"
} >"${CONFIG_FILE}"

# nodeId and cleanStart are read as system properties by ClusterLauncher; the
# start script forwards JAVA_OPTS to the JVM. The --add-opens flags for
# Aeron/Agrona are already baked into the launcher start script.
JAVA_OPTS="${JAVA_OPTS:-} -Dadbe.nodeId=${ADBE_NODE_ID} -Dadbe.cleanStart=${ADBE_CLEAN_START}"
if [ -n "${ADBE_METRICS_PORT:-}" ]; then
    JAVA_OPTS="${JAVA_OPTS} -Dadbe.metricsPort=${ADBE_METRICS_PORT}"
fi
# Opt-in domain event journal (ADR 0011): every member records its own event
# stream to its Archive when enabled.
if [ "${ADBE_EVENT_JOURNAL:-false}" = "true" ]; then
    JAVA_OPTS="${JAVA_OPTS} -Dadbe.eventJournal=true"
    if [ -n "${ADBE_EVENT_JOURNAL_CAPACITY:-}" ]; then
        JAVA_OPTS="${JAVA_OPTS} -Dadbe.eventJournalCapacity=${ADBE_EVENT_JOURNAL_CAPACITY}"
    fi
fi
export JAVA_OPTS

echo "Starting ADBE node ${ADBE_NODE_ID} as ${ADBE_HOST} (baseDir=${ADBE_BASE_DIR})"
exec /opt/adbe/bin/adbe-launcher "--config=${CONFIG_FILE}"
