package io.justrade.ledgerd.launcher;

import io.aeron.cluster.ClusterControl;
import io.justrade.ledgerd.config.CoreConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

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
            // Overflow policy (ADR 0014): DROP (default, best-effort) or BLOCK (spin-drain).
            final String policy = System.getProperty("ledgerd.journalOverflowPolicy");
            if (policy != null) {
                coreConfig = coreConfig.withJournalOverflowPolicy(
                        CoreConfig.JournalOverflowPolicy.valueOf(policy.trim().toUpperCase(Locale.ROOT)));
            }
        }

        // Transfer-batch capacities (ADR 0012), tunable for high-throughput batches.
        final int maxBatchSize = Integer.getInteger("ledgerd.maxBatchSize", CoreConfig.DEFAULT_MAX_BATCH_SIZE);
        final int batchDedupWindow =
                Integer.getInteger("ledgerd.batchDedupWindow", CoreConfig.DEFAULT_BATCH_DEDUP_WINDOW);
        coreConfig = coreConfig.withBatch(maxBatchSize, batchDedupWindow);

        // Preallocated map capacities, tunable to size the hot-path stores for a
        // target account/asset workload (docs/CAPACITY-PLANNING.md).
        final int accountCapacity = Integer.getInteger("ledgerd.accountCapacity", CoreConfig.DEFAULT_ACCOUNT_CAPACITY);
        final int allowanceOwnerCapacity =
                Integer.getInteger("ledgerd.allowanceOwnerCapacity", CoreConfig.DEFAULT_ALLOWANCE_OWNER_CAPACITY);
        final int dedupClientCapacity =
                Integer.getInteger("ledgerd.dedupClientCapacity", CoreConfig.DEFAULT_DEDUP_CLIENT_CAPACITY);
        coreConfig = coreConfig.withCapacities(accountCapacity, allowanceOwnerCapacity, dedupClientCapacity);

        final String batchDedupWarning = coreConfig.batchDedupSizingWarning();
        if (batchDedupWarning != null) {
            System.err.println("[ledgerd] WARN: " + batchDedupWarning);
        }

        final ClusterNode node = new ClusterNode(clusterConfig, coreConfig, cleanStart);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "ledgerd-shutdown"));

        // Optional Prometheus metrics endpoint, enabled with -Dledgerd.metricsPort=<port>.
        final Integer metricsPort = Integer.getInteger("ledgerd.metricsPort");
        if (metricsPort != null) {
            final MetricsHttpServer metricsServer = new MetricsHttpServer(node.countersManager(), metricsPort);
            Runtime.getRuntime().addShutdownHook(new Thread(metricsServer::close, "ledgerd-metrics-shutdown"));
        }

        // Optional periodic snapshots (H2), enabled with a non-zero interval. Bounds
        // log growth and keeps node restart and read-replica cold-start fast.
        startSnapshotScheduler(clusterConfig, node);

        // Park the main thread; the service runs on the clustered service agent thread.
        while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(1_000_000_000L);
        }
    }

    private static void startSnapshotScheduler(final ClusterConfig clusterConfig, final ClusterNode node) {
        final long intervalSeconds = Long.getLong("ledgerd.snapshotIntervalSeconds", 0L);
        final long intervalCommands = Long.getLong("ledgerd.snapshotIntervalCommands", 0L);
        if (intervalSeconds <= 0 && intervalCommands <= 0) {
            return;
        }
        try {
            final CountersReader clusterCounters = ClusterControl.mapCounters(clusterConfig.clusterDir());
            final AtomicCounter controlToggle = ClusterControl.findControlToggle(clusterCounters, 0);
            if (controlToggle == null) {
                System.err.println("[ledgerd] WARN: snapshot scheduler disabled; cluster control toggle not found");
                return;
            }
            final LongSupplier committedCommands = commandCountSupplier(node.countersManager());
            final SnapshotScheduler scheduler = new SnapshotScheduler(
                    controlToggle, committedCommands, TimeUnit.SECONDS.toNanos(intervalSeconds), intervalCommands);
            scheduler.start();
            Runtime.getRuntime().addShutdownHook(new Thread(scheduler::close, "ledgerd-snapshot-shutdown"));
        } catch (final RuntimeException e) {
            System.err.println("[ledgerd] WARN: snapshot scheduler disabled: " + e.getMessage());
        }
    }

    private static LongSupplier commandCountSupplier(final CountersReader reader) {
        final int id = findCounterId(reader, "ledgerd.commands_processed");
        if (id < 0) {
            return () -> 0L;
        }
        return () -> reader.getCounterValue(id);
    }

    private static int findCounterId(final CountersReader reader, final String label) {
        final int maxId = reader.maxCounterId();
        for (int id = 0; id <= maxId; id++) {
            if (reader.getCounterState(id) == CountersReader.RECORD_ALLOCATED
                    && label.equals(reader.getCounterLabel(id))) {
                return id;
            }
        }
        return -1;
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
