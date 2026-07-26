package com.adbe.launcher;

import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceService;
import com.adbe.telemetry.CoreMetrics;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * Launches and owns the Aeron components for one cluster node: the clustered
 * media driver (Media Driver + Archive + Consensus Module) and a single
 * {@link ClusteredServiceContainer} hosting one {@link BalanceService}.
 *
 * <p>A single service agent runs the balance and allowance logic on one thread,
 * satisfying the single-writer / no-locks requirement.
 */
public final class ClusterNode implements AutoCloseable {

    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final CoreMetrics metrics;

    public ClusterNode(final ClusterConfig config, final CoreConfig coreConfig) {
        this.metrics = new CoreMetrics();

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
                .deleteArchiveOnStart(true);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
                .clusterMemberId(config.nodeId())
                .clusterMembers(config.clusterMembers())
                .aeronDirectoryName(config.aeronDirectoryName())
                .clusterDir(config.clusterDir())
                .ingressChannel(config.ingressChannel())
                .replicationChannel(config.replicationChannel())
                .archiveContext(archiveClientContext.clone())
                .deleteDirOnStart(true);

        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .archiveContext(archiveClientContext.clone())
                .clusterDir(config.clusterDir())
                .clusteredService(new BalanceService(coreConfig, metrics));

        this.clusteredMediaDriver =
                ClusteredMediaDriver.launch(mediaDriverContext, archiveContext, consensusModuleContext);
        this.container = ClusteredServiceContainer.launch(serviceContext);
    }

    public CoreMetrics metrics() {
        return metrics;
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
