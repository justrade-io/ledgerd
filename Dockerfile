# Multi-stage, multi-target build.
#
#   * stage "build"          : produces the launcher, read, risk, and examples install distributions.
#   * target "node"          : slim JRE that runs one cluster node (default, BalanceService).
#   * target "read"          : slim JRE that runs one read replica node (ReadReplicaNode + HTTP).
#   * target "risk"          : slim JRE that runs the AI risk service (RiskServiceLauncher + dashboard, ADR 0012).
#   * target "client"        : slim JRE that runs the remote client smoke test.
#   * target "event-verifier": slim JRE that follows the domain event journal and verifies it (ADR 0011).
#
# The cluster topology is supplied at runtime via environment variables
# (see docker-compose.yml).

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon :launcher:installDist :read:installDist :risk:installDist :examples:installDist

FROM eclipse-temurin:21-jre AS node
LABEL org.opencontainers.image.title="ledgerd-launcher" \
      org.opencontainers.image.description="LEDGERD - Aeron Distributed Balance Engine cluster node" \
      org.opencontainers.image.licenses="MIT"

# Run as a non-root user.
RUN useradd --system --create-home --home-dir /home/ledgerd ledgerd \
    && mkdir -p /var/ledgerd \
    && chown -R ledgerd:ledgerd /var/ledgerd

COPY --from=build /src/launcher/build/install/launcher /opt/ledgerd
COPY docker/entrypoint.sh /opt/ledgerd/entrypoint.sh
RUN chmod +x /opt/ledgerd/entrypoint.sh /opt/ledgerd/bin/launcher \
    && chown -R ledgerd:ledgerd /opt/ledgerd

USER ledgerd
WORKDIR /var/ledgerd
ENTRYPOINT ["/opt/ledgerd/entrypoint.sh"]

FROM eclipse-temurin:21-jre AS read
LABEL org.opencontainers.image.title="ledgerd-read" \
      org.opencontainers.image.description="LEDGERD read replica node (ReadReplicaNode + HTTP query API)" \
      org.opencontainers.image.licenses="MIT"

# Run as a non-root user.
RUN useradd --system --create-home --home-dir /home/ledgerd ledgerd \
    && mkdir -p /var/ledgerd \
    && chown -R ledgerd:ledgerd /var/ledgerd

COPY --from=build /src/read/build/install/read /opt/ledgerd
COPY docker/read-entrypoint.sh /opt/ledgerd/read-entrypoint.sh
RUN chmod +x /opt/ledgerd/read-entrypoint.sh /opt/ledgerd/bin/read \
    && chown -R ledgerd:ledgerd /opt/ledgerd

USER ledgerd
WORKDIR /var/ledgerd
ENTRYPOINT ["/opt/ledgerd/read-entrypoint.sh"]

FROM eclipse-temurin:21-jre AS risk
LABEL org.opencontainers.image.title="ledgerd-risk" \
      org.opencontainers.image.description="LEDGERD AI risk service (RiskServiceLauncher + dashboard, ADR 0012)" \
      org.opencontainers.image.licenses="MIT"

# Run as a non-root user.
RUN useradd --system --create-home --home-dir /home/ledgerd ledgerd \
    && mkdir -p /var/ledgerd \
    && chown -R ledgerd:ledgerd /var/ledgerd

COPY --from=build /src/risk/build/install/risk /opt/ledgerd
COPY docker/risk-entrypoint.sh /opt/ledgerd/risk-entrypoint.sh
RUN chmod +x /opt/ledgerd/risk-entrypoint.sh /opt/ledgerd/bin/risk \
    && chown -R ledgerd:ledgerd /opt/ledgerd

USER ledgerd
WORKDIR /var/ledgerd
ENTRYPOINT ["/opt/ledgerd/risk-entrypoint.sh"]

FROM eclipse-temurin:21-jre AS client
LABEL org.opencontainers.image.title="ledgerd-client-example" \
      org.opencontainers.image.description="LEDGERD remote client smoke test" \
      org.opencontainers.image.licenses="MIT"

RUN useradd --system --create-home --home-dir /home/ledgerd ledgerd

COPY --from=build /src/examples/build/install/examples/lib /opt/ledgerd/lib
COPY docker/client-entrypoint.sh /opt/ledgerd/client-entrypoint.sh
RUN chmod +x /opt/ledgerd/client-entrypoint.sh
USER ledgerd
# RemoteClientExample reads ingress endpoints from LEDGERD_INGRESS_ENDPOINTS; the
# entrypoint derives a routable egress endpoint from the container's own IP.
ENTRYPOINT ["/opt/ledgerd/client-entrypoint.sh"]

FROM eclipse-temurin:21-jre AS event-verifier
LABEL org.opencontainers.image.title="ledgerd-event-verifier" \
      org.opencontainers.image.description="LEDGERD domain event journal follower verifier (ADR 0011)" \
      org.opencontainers.image.licenses="MIT"

RUN useradd --system --create-home --home-dir /home/ledgerd ledgerd

# Reuse the read distribution: EventJournalVerifier ships in the read jar.
COPY --from=build /src/read/build/install/read/lib /opt/ledgerd/lib
COPY docker/event-verifier-entrypoint.sh /opt/ledgerd/event-verifier-entrypoint.sh
RUN chmod +x /opt/ledgerd/event-verifier-entrypoint.sh
USER ledgerd
WORKDIR /tmp
ENTRYPOINT ["/opt/ledgerd/event-verifier-entrypoint.sh"]
