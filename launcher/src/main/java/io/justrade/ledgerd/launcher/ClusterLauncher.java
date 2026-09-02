package io.justrade.ledgerd.launcher;

import io.justrade.ledgerd.config.CoreConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.LockSupport;

/**
 * Entry point that starts a single LEDGERD cluster node and blocks until the JVM is
 * terminated.
 *
 * <p>Configuration sources, in order of precedence:
 *
 * <ul>
 *   <li>{@code --config=<file>} argument or {@code -Dledgerd.config=<file>}: load a
 *       node from a {@code .properties} file (production / multi-node).
 *   <li>otherwise a single-node localhost cluster is started, with node id and
 *       base directory from {@code -Dledgerd.nodeId} and {@code -Dledgerd.baseDir}.
 * </ul>
 *
 * <p>{@code -Dledgerd.cleanStart=false} preserves prior archive and cluster state so
 * a restarted node can recover and catch up.
 */
public final class ClusterLauncher {

    private ClusterLauncher() {}

    public static void main(final String[] args) {
        final int nodeId = Integer.getInteger("ledgerd.nodeId", 0);
        final String configPath = configPath(args);

        final ClusterConfig clusterConfig;
        if (configPath != null) {
            clusterConfig = ClusterConfig.fromProperties(Paths.get(configPath), nodeId);
        } else {
            final Path baseDir = Paths.get(System.getProperty("ledgerd.baseDir", "build/ledgerd-node-" + nodeId));
            clusterConfig = ClusterConfig.singleNodeLocalhost(nodeId, baseDir);
        }

        final boolean cleanStart = Boolean.parseBoolean(System.getProperty("ledgerd.cleanStart", "true"));
        CoreConfig coreConfig = CoreConfig.defaults();
        // Opt-in domain event journal (ADR 0011), enabled with -Dledgerd.eventJournal=true.
        if (Boolean.getBoolean("ledgerd.eventJournal")) {
            final int capacity =
                    Integer.getInteger("ledgerd.eventJournalCapacity", CoreConfig.DEFAULT_EVENT_JOURNAL_CAPACITY);
            coreConfig = coreConfig.withEventJournal(capacity);
        }

        final ClusterNode node = new ClusterNode(clusterConfig, coreConfig, cleanStart);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "ledgerd-shutdown"));

        // Optional Prometheus metrics endpoint, enabled with -Dledgerd.metricsPort=<port>.
        final Integer metricsPort = Integer.getInteger("ledgerd.metricsPort");
        if (metricsPort != null) {
            final MetricsHttpServer metricsServer = new MetricsHttpServer(node.countersManager(), metricsPort);
            Runtime.getRuntime().addShutdownHook(new Thread(metricsServer::close, "ledgerd-metrics-shutdown"));
        }

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
        return System.getProperty("ledgerd.config");
    }
}
