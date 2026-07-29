package com.adbe.read;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.read.config.ReadServiceConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for a standalone read-service process. Joins the cluster as a
 * follower and serves eventually-consistent reads over HTTP.
 *
 * <p>Configuration is read from environment variables with sensible localhost
 * defaults so it runs out of the box and in Docker:
 *
 * <pre>
 *   ADBE_NODE_ID          this member's id                (default 0)
 *   ADBE_CLUSTER_MEMBERS  Aeron cluster members string    (default single-node localhost)
 *   ADBE_BASE_DIR         directory root for driver/archive/cluster
 *   ADBE_HOST             this node's host                (default localhost)
 *   ADBE_HTTP_PORT        HTTP query port                 (default 8080)
 *   ADBE_CLEAN_START      wipe prior state on start       (default true)
 * </pre>
 */
public final class ReadServiceLauncher {

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws InterruptedException {
        final int nodeId = Integer.parseInt(envOrDefault("ADBE_NODE_ID", "0"));
        final int httpPort = Integer.parseInt(envOrDefault("ADBE_HTTP_PORT", "8080"));
        final boolean cleanStart = Boolean.parseBoolean(envOrDefault("ADBE_CLEAN_START", "true"));

        final ClusterConfig clusterConfig = resolveClusterConfig(nodeId);
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(httpPort).build();

        try (ReadNode node = new ReadNode(clusterConfig, CoreConfig.defaults(), readConfig, cleanStart)) {
            System.out.printf("ADBE read service started: nodeId=%d httpPort=%d%n", nodeId, node.httpPort());
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown, "adbe-read-shutdown"));
            latch.await();
        }
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

    private static String envOrDefault(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
