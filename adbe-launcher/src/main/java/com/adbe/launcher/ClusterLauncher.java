package com.adbe.launcher;

import com.adbe.config.CoreConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.LockSupport;

/**
 * Entry point that starts a single ADBE cluster node and blocks until the JVM is
 * terminated.
 *
 * <p>Configuration sources, in order of precedence:
 *
 * <ul>
 *   <li>{@code --config=<file>} argument or {@code -Dadbe.config=<file>}: load a
 *       node from a {@code .properties} file (production / multi-node).
 *   <li>otherwise a single-node localhost cluster is started, with node id and
 *       base directory from {@code -Dadbe.nodeId} and {@code -Dadbe.baseDir}.
 * </ul>
 *
 * <p>{@code -Dadbe.cleanStart=false} preserves prior archive and cluster state so
 * a restarted node can recover and catch up.
 */
public final class ClusterLauncher {

    private ClusterLauncher() {}

    public static void main(final String[] args) {
        final int nodeId = Integer.getInteger("adbe.nodeId", 0);
        final String configPath = configPath(args);

        final ClusterConfig clusterConfig;
        if (configPath != null) {
            clusterConfig = ClusterConfig.fromProperties(Paths.get(configPath), nodeId);
        } else {
            final Path baseDir = Paths.get(System.getProperty("adbe.baseDir", "build/adbe-node-" + nodeId));
            clusterConfig = ClusterConfig.singleNodeLocalhost(nodeId, baseDir);
        }

        final boolean cleanStart = Boolean.parseBoolean(System.getProperty("adbe.cleanStart", "true"));
        final CoreConfig coreConfig = CoreConfig.defaults();

        final ClusterNode node = new ClusterNode(clusterConfig, coreConfig, cleanStart);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "adbe-shutdown"));

        // Park the main thread; the service runs on the clustered service agent thread.
        while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(1_000_000_000L);
        }
    }

    private static String configPath(final String[] args) {
        final String prefix = "--config=";
        for (final String arg : args) {
            if (arg.startsWith(prefix)) {
                return arg.substring(prefix.length());
            }
        }
        return System.getProperty("adbe.config");
    }
}
