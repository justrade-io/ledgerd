package com.adbe.launcher;

import com.adbe.config.CoreConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.LockSupport;

/**
 * Entry point that starts a single ADBE cluster node and blocks until the JVM is
 * terminated. Node id and base directory can be overridden via system
 * properties {@code adbe.nodeId} and {@code adbe.baseDir}.
 */
public final class ClusterLauncher {

    private ClusterLauncher() {}

    public static void main(final String[] args) {
        final int nodeId = Integer.getInteger("adbe.nodeId", 0);
        final Path baseDir = Paths.get(System.getProperty("adbe.baseDir", "build/adbe-node-" + nodeId));

        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(nodeId, baseDir);
        final CoreConfig coreConfig = CoreConfig.defaults();

        final ClusterNode node = new ClusterNode(clusterConfig, coreConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "adbe-shutdown"));

        // Park the main thread; the service runs on the clustered service agent thread.
        while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(1_000_000_000L);
        }
    }
}
