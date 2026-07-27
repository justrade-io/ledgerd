package com.adbe.launcher;

import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceService;
import com.adbe.telemetry.AtomicCounterSink;
import com.adbe.telemetry.CoreMetrics;
import com.adbe.telemetry.CounterSink;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.Locale;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * Launches and owns the Aeron components for one cluster node: the clustered
 * media driver (Media Driver + Archive + Consensus Module) and a single
 * {@link ClusteredServiceContainer} hosting one {@link BalanceService}.
 *
 * <p>A single service agent runs the balance and allowance logic on one thread,
 * satisfying the single-writer / no-locks requirement.
 *
 * <p>Core observability counters are mirrored into a standalone off-heap Agrona
 * {@link CountersManager} buffer, so operators can read counter values from
 * another thread without perturbing the single-writer hot path.
 */
public final class ClusterNode implements AutoCloseable {

    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final CoreMetrics metrics;
    private final CountersManager countersManager;

    /** Launches a node that clears prior state on start (fresh cluster). */
    public ClusterNode(final ClusterConfig config, final CoreConfig coreConfig) {
        this(config, coreConfig, true);
    }

    /**
     * Launches a node.
     *
     * @param cleanStart when {@code true}, deletes any prior archive and cluster
     *     directories on start (fresh cluster). When {@code false}, preserves
     *     them so the node can recover its log and catch up after a restart.
     */
    public ClusterNode(final ClusterConfig config, final CoreConfig coreConfig, final boolean cleanStart) {
        this.countersManager = newCountersManager();
        this.metrics = new CoreMetrics(
                new AtomicCounterSink(allocateCounters(countersManager), allocateGauges(countersManager)));

        final String localControlChannel = "aeron:ipc?term-length=64k";

        final AeronArchive.Context archiveClientContext = new AeronArchive.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .controlRequestChannel(localControlChannel)
                .controlResponseChannel(localControlChannel);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .archiveDir(config.archiveDir())
                .controlChannel(config.archiveControlChannel())
                .localControlChannel(localControlChannel)
                .replicationChannel(config.replicationChannel())
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(cleanStart);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
                .clusterMemberId(config.nodeId())
                .clusterMembers(config.clusterMembers())
                .aeronDirectoryName(config.aeronDirectoryName())
                .clusterDir(config.clusterDir())
                .ingressChannel(config.ingressChannel())
                .replicationChannel(config.replicationChannel())
                .archiveContext(archiveClientContext.clone())
                .deleteDirOnStart(cleanStart);

        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .archiveContext(archiveClientContext.clone())
                .clusterDir(config.clusterDir())
                .clusteredService(new BalanceService(coreConfig, metrics));

        this.clusteredMediaDriver =
                ClusteredMediaDriver.launch(mediaDriverContext, archiveContext, consensusModuleContext);
        this.container = ClusteredServiceContainer.launch(serviceContext);
    }

    private static CountersManager newCountersManager() {
        final int maxCounters = CounterSink.Counter.COUNT + CounterSink.Gauge.COUNT;
        final UnsafeBuffer valuesBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                maxCounters * CountersManager.COUNTER_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final UnsafeBuffer metadataBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                maxCounters * CountersManager.METADATA_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        return new CountersManager(metadataBuffer, valuesBuffer);
    }

    private static AtomicCounter[] allocateCounters(final CountersManager countersManager) {
        final CounterSink.Counter[] all = CounterSink.Counter.values();
        final AtomicCounter[] counters = new AtomicCounter[all.length];
        for (final CounterSink.Counter counter : all) {
            counters[counter.ordinal()] = countersManager.newCounter(
                    "adbe." + counter.name().toLowerCase(Locale.ROOT), CounterSink.TYPE_COUNTER);
        }
        return counters;
    }

    private static AtomicCounter[] allocateGauges(final CountersManager countersManager) {
        final CounterSink.Gauge[] all = CounterSink.Gauge.values();
        final AtomicCounter[] gauges = new AtomicCounter[all.length];
        for (final CounterSink.Gauge gauge : all) {
            gauges[gauge.ordinal()] =
                    countersManager.newCounter("adbe." + gauge.name().toLowerCase(Locale.ROOT), CounterSink.TYPE_GAUGE);
        }
        return gauges;
    }

    public CoreMetrics metrics() {
        return metrics;
    }

    /** Exposes the off-heap counters manager for cross-thread metric reads. */
    public CountersManager countersManager() {
        return countersManager;
    }

    @Override
    public void close() {
        if (container != null) {
            container.close();
        }
        if (clusteredMediaDriver != null) {
            clusteredMediaDriver.close();
        }
    }
}
