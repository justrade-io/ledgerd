#!/usr/bin/env sh
# Runs the remote client smoke test. Aeron cluster egress must advertise an
# address the cluster nodes can route back to, so default the egress endpoint to
# this container's own IP on the docker network (an ephemeral port with :0).
set -eu

CONTAINER_IP="$(hostname -i | awk '{print $1}')"
export ADBE_EGRESS_ENDPOINT="${ADBE_EGRESS_ENDPOINT:-${CONTAINER_IP}:0}"
echo "Client egress endpoint: ${ADBE_EGRESS_ENDPOINT}"

exec java \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp '/opt/adbe/lib/*' \
    com.adbe.examples.RemoteClientExample
