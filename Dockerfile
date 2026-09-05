# syntax=docker/dockerfile:1

# Build stage: assemble the examples distribution, which transitively bundles the
# launcher, read, write-client, read-client, core and protocol modules plus the
# Aeron stack, so a single image can host every LEDGERD role (cluster node, read
# replica, load driver, read verification).
FROM gradle:8.10.2-jdk21 AS build
USER root
WORKDIR /workspace
COPY . .
# The base image ships Gradle 8.10.2 (matching the wrapper), so invoke `gradle`
# directly to avoid re-downloading the distribution on every image build. The
# cache mount keeps resolved Maven dependencies across rebuilds.
RUN --mount=type=cache,target=/root/.gradle \
    gradle :examples:installDist --no-daemon

# Runtime stage: a JRE with the assembled classpath and a role-dispatching entrypoint.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/examples/build/install/examples/lib /app/lib
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Aeron and Agrona require these module opens; inherited by every java invocation
# in the container (nodes, read replica, load driver, and ClusterTool).
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

ENTRYPOINT ["/entrypoint.sh"]
