#!/usr/bin/env sh
# Generates a per-node cluster.properties from environment variables and starts
# one LEDGERD cluster node via the launcher's fromProperties path. Keeping the
# cluster topology in the environment (compose) means the image stays generic.
set -eu

: "${LEDGERD_NODE_ID:?LEDGERD_NODE_ID is required (0-based member id)}"
: "${LEDGERD_HOST:?LEDGERD_HOST is required (this node's advertised hostname)}"
: "${LEDGERD_CLUSTER_MEMBERS:?LEDGERD_CLUSTER_MEMBERS is required (Aeron member string)}"

LEDGERD_BASE_DIR="${LEDGERD_BASE_DIR:-/var/ledgerd}"
LEDGERD_CLEAN_START="${LEDGERD_CLEAN_START:-true}"

mkdir -p "${LEDGERD_BASE_DIR}"
CONFIG_FILE="${LEDGERD_BASE_DIR}/cluster.properties"
{
    echo "ledgerd.clusterMembers=${LEDGERD_CLUSTER_MEMBERS}"
    echo "ledgerd.host=${LEDGERD_HOST}"
    echo "ledgerd.baseDir=${LEDGERD_BASE_DIR}"
} >"${CONFIG_FILE}"

# nodeId and cleanStart are read as system properties by ClusterLauncher; the
# start script forwards JAVA_OPTS to the JVM. The --add-opens flags for
# Aeron/Agrona are already baked into the launcher start script.
JAVA_OPTS="${JAVA_OPTS:-} -Dledgerd.nodeId=${LEDGERD_NODE_ID} -Dledgerd.cleanStart=${LEDGERD_CLEAN_START}"
if [ -n "${LEDGERD_METRICS_PORT:-}" ]; then
    JAVA_OPTS="${JAVA_OPTS} -Dledgerd.metricsPort=${LEDGERD_METRICS_PORT}"
fi
# Opt-in domain event journal (ADR 0011): every member records its own event
# stream to its Archive when enabled.
if [ "${LEDGERD_EVENT_JOURNAL:-false}" = "true" ]; then
    JAVA_OPTS="${JAVA_OPTS} -Dledgerd.eventJournal=true"
    if [ -n "${LEDGERD_EVENT_JOURNAL_CAPACITY:-}" ]; then
        JAVA_OPTS="${JAVA_OPTS} -Dledgerd.eventJournalCapacity=${LEDGERD_EVENT_JOURNAL_CAPACITY}"
    fi
fi
export JAVA_OPTS

echo "Starting LEDGERD node ${LEDGERD_NODE_ID} as ${LEDGERD_HOST} (baseDir=${LEDGERD_BASE_DIR})"
exec /opt/ledgerd/bin/launcher "--config=${CONFIG_FILE}"
