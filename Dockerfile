# Multi-stage, multi-target build.
#
#   * stage "build"  : produces the launcher and examples install distributions.
#   * target "node"  : slim JRE that runs one cluster node (default).
#   * target "client": slim JRE that runs the remote client smoke test.
#
# The cluster topology is supplied at runtime via environment variables
# (see docker-compose.yml).

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon :adbe-launcher:installDist :adbe-examples:installDist

FROM eclipse-temurin:21-jre AS node
LABEL org.opencontainers.image.title="adbe-launcher" \
      org.opencontainers.image.description="ADBE - Aeron Distributed Balance Engine cluster node" \
      org.opencontainers.image.licenses="MIT"

# Run as a non-root user.
RUN useradd --system --create-home --home-dir /home/adbe adbe \
    && mkdir -p /var/adbe \
    && chown -R adbe:adbe /var/adbe

COPY --from=build /src/adbe-launcher/build/install/adbe-launcher /opt/adbe
COPY docker/entrypoint.sh /opt/adbe/entrypoint.sh
RUN chmod +x /opt/adbe/entrypoint.sh /opt/adbe/bin/adbe-launcher \
    && chown -R adbe:adbe /opt/adbe

USER adbe
WORKDIR /var/adbe
ENTRYPOINT ["/opt/adbe/entrypoint.sh"]

FROM eclipse-temurin:21-jre AS client
LABEL org.opencontainers.image.title="adbe-client-example" \
      org.opencontainers.image.description="ADBE remote client smoke test" \
      org.opencontainers.image.licenses="MIT"

RUN useradd --system --create-home --home-dir /home/adbe adbe

COPY --from=build /src/adbe-examples/build/install/adbe-examples/lib /opt/adbe/lib
COPY docker/client-entrypoint.sh /opt/adbe/client-entrypoint.sh
RUN chmod +x /opt/adbe/client-entrypoint.sh
USER adbe
# RemoteClientExample reads ingress endpoints from ADBE_INGRESS_ENDPOINTS; the
# entrypoint derives a routable egress endpoint from the container's own IP.
ENTRYPOINT ["/opt/adbe/client-entrypoint.sh"]
