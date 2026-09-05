#!/usr/bin/env bash
set -euo pipefail

JAVA_OPTS=(
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
)

case "${LEDGERD_ROLE:-}" in
  node)
    : "${LEDGERD_NODE_ID:?LEDGERD_NODE_ID is required}"
    : "${LEDGERD_HOST:?LEDGERD_HOST is required}"
    : "${LEDGERD_CLUSTER_MEMBERS:?LEDGERD_CLUSTER_MEMBERS is required}"

    {
      printf 'ledgerd.clusterMembers=%s\n' "${LEDGERD_CLUSTER_MEMBERS}"
      printf 'ledgerd.baseDir=%s\n' "${LEDGERD_BASE_DIR:-/data}"
      printf 'ledgerd.host=%s\n' "${LEDGERD_HOST}"
    } > /tmp/ledgerd-node.properties

    args=("${JAVA_OPTS[@]}")
    args+=("-Dledgerd.nodeId=${LEDGERD_NODE_ID}")
    args+=("-Dledgerd.cleanStart=${LEDGERD_CLEAN_START:-true}")
    args+=("-Dledgerd.eventJournal=${LEDGERD_EVENT_JOURNAL:-false}")
    if [[ -n "${LEDGERD_METRICS_PORT:-}" ]]; then
      args+=("-Dledgerd.metricsPort=${LEDGERD_METRICS_PORT}")
    fi
    args+=(-cp "/app/lib/*")
    args+=(io.justrade.ledgerd.launcher.ClusterLauncher --config=/tmp/ledgerd-node.properties)
    exec java "${args[@]}"
    ;;

  read)
    exec java "${JAVA_OPTS[@]}" -cp "/app/lib/*" io.justrade.ledgerd.read.ReadServiceLauncher
    ;;

  client)
    exec java "${JAVA_OPTS[@]}" -cp "/app/lib/*" io.justrade.ledgerd.examples.LoadGenerator
    ;;

  readcheck)
    exec java "${JAVA_OPTS[@]}" -cp "/app/lib/*" io.justrade.ledgerd.examples.ReadCheck
    ;;

  *)
    echo "LEDGERD_ROLE must be one of: node, read, client, readcheck" >&2
    exit 1
    ;;
esac
