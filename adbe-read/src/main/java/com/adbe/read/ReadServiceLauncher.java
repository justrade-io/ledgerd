package com.adbe.read;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.config.StandbyConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for a read-service process. Supports two modes:
 *
 * <dl>
 *   <dt>{@code ADBE_MODE=standby} (default for read deployments)</dt>
 *   <dd>Runs as a standalone standby node: connects to the cluster's Aeron
 *       Archive, periodically downloads the latest service snapshot, loads it
 *       into the balance engine, and serves reads over HTTP. The standby node
 *       does NOT appear in {@code clusterMembers} and does not vote or affect
 *       quorum. Staleness is bounded by the snapshot poll interval.</dd>
 *
 *   <dt>{@code ADBE_MODE=cluster} (legacy, for homogeneous read clusters)</dt>
 *   <dd>Joins the cluster as a full Raft voting member hosting
 *       {@link com.adbe.read.projection.ReadModelService}. All members in the
 *       cluster must host the identical service for snapshot compatibility.</dd>
 * </dl>
 *
 * <p>Configuration is read from environment variables with sensible localhost
 * defaults:
 *
 * <pre>
 *   ADBE_MODE             standby | cluster              (default standby)
 *   ADBE_NODE_ID          this member's id               (default 0)
 *   ADBE_CLUSTER_MEMBERS  Aeron cluster members string   (default single-node localhost)
 *   ADBE_BASE_DIR         directory root
 *   ADBE_HOST             this node's host               (default localhost)
 *   ADBE_HTTP_PORT        HTTP query port                (default 8080)
 *   ADBE_CLEAN_START      wipe prior state on start      (default true)
 *   ADBE_ARCHIVE_CHANNEL  Archive control channel        (standby mode only)
 * </pre>
 */
public final class ReadServiceLauncher {

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws InterruptedException {
        final String mode = envOrDefault("ADBE_MODE", "standby");
        final int httpPort = Integer.parseInt(envOrDefault("ADBE_HTTP_PORT", "8080"));
        final boolean cleanStart = Boolean.parseBoolean(envOrDefault("ADBE_CLEAN_START", "true"));

        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(httpPort).build();

        if ("standby".equalsIgnoreCase(mode)) {
            launchStandby(readConfig);
        } else {
            launchCluster(readConfig, cleanStart);
        }
    }

    private static void launchStandby(final ReadServiceConfig readConfig) throws InterruptedException {
        final StandbyConfig standbyConfig = resolveStandbyConfig();

        try (StandbyReadNode node = new StandbyReadNode(standbyConfig, CoreConfig.defaults(), readConfig)) {
            System.out.printf(
                    "ADBE standby read service started: httpPort=%d archiveChannel=%s%n",
                    node.httpPort(), standbyConfig.archiveControlChannel());
            parkUntilShutdown("adbe-standby-shutdown");
        }
    }

    private static void launchCluster(final ReadServiceConfig readConfig, final boolean cleanStart)
            throws InterruptedException {
        final int nodeId = Integer.parseInt(envOrDefault("ADBE_NODE_ID", "0"));
        final ClusterConfig clusterConfig = resolveClusterConfig(nodeId);

        try (ReadNode node = new ReadNode(clusterConfig, CoreConfig.defaults(), readConfig, cleanStart)) {
            System.out.printf("ADBE cluster read service started: nodeId=%d httpPort=%d%n", nodeId, node.httpPort());
            parkUntilShutdown("adbe-read-shutdown");
        }
    }

    private static StandbyConfig resolveStandbyConfig() {
        final String archiveChannel = System.getenv("ADBE_ARCHIVE_CHANNEL");
        final StandbyConfig.Builder builder = StandbyConfig.builder();
        if (archiveChannel != null && !archiveChannel.isBlank()) {
            builder.archiveControlChannel(archiveChannel);
        }
        final String pollInterval = System.getenv("ADBE_SNAPSHOT_POLL_MS");
        if (pollInterval != null && !pollInterval.isBlank()) {
            builder.pollIntervalMs(Long.parseLong(pollInterval));
        }
        return builder.build();
    }

    private static ClusterConfig resolveClusterConfig(final int nodeId) {
        final String members = System.getenv("ADBE_CLUSTER_MEMBERS");
        final String baseDir = System.getenv("ADBE_BASE_DIR");
        final String host = envOrDefault("ADBE_HOST", "localhost");

        if (members == null || members.isBlank()) {
            final Path root = Paths.get(baseDir == null || baseDir.isBlank() ? "build/adbe-read-node" : baseDir);
            return ClusterConfig.singleNodeLocalhost(nodeId, root);
        }

        final Path propsRoot =
                Paths.get(baseDir == null || baseDir.isBlank() ? "build/adbe-read-node-" + nodeId : baseDir);
        return ClusterConfig.fromMembers(nodeId, host, members, propsRoot);
    }

    private static void parkUntilShutdown(final String hookName) {
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown, hookName));
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String envOrDefault(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
