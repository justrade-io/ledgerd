package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterTool;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import io.justrade.ledgerd.testkit.MultiNodeCluster;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Multi-node regression test for the read replica's snapshot loader, exercising
 * the production (three-node) topology.
 *
 * <p>In a multi-node cluster the LEDGERD service snapshot recording (stream 106) is
 * prefixed with cluster-schema framing records before the LEDGERD
 * {@code SnapshotHeader}. The loader must skip that framing and then load the
 * whole snapshot. The snapshot is made large (100 accounts) so it spans more
 * than one 64-fragment poll batch. With the live log disabled the replica can
 * only serve state from a loaded snapshot.
 *
 * <p>Tagged {@code cluster}: multi-node and timing-sensitive, run via the opt-in
 * {@code clusterTest} task, never wired into {@code check}.
 */
@Tag("cluster")
class ReadReplicaSnapshotLoadClusterTest {

    private static final int NODE_COUNT = 3;
    private static final int ACCOUNTS = 100;
    private static final long RESULT_TIMEOUT_MS = 30_000L;
    private static final long READ_TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(180)
    void readReplicaLoadsLargeServiceSnapshotFromClusterArchive(@TempDir final Path baseDir) throws Exception {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODE_COUNT, baseDir);

            for (int i = 0; i < ACCOUNTS; i++) {
                client.send(1L, i, 0L, i + 1L, CommandType.CREDIT, i + 1L, 0L, 0L, 1L);
            }
            assertTrue(client.awaitResult(ACCOUNTS, RESULT_TIMEOUT_MS), "last credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known before triggering a snapshot");
            assertTrue(ClusterTool.snapshot(configs[leader].clusterDir(), System.out), "snapshot trigger accepted");

            // Live log DISABLED: the replica must build state purely from a service
            // snapshot loaded from a member's archive.
            final int queryPort = ReadClientTestSupport.freeUdpPort();
            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                    .archiveControlChannel(configs[leader].archiveControlChannel())
                    .pollIntervalMs(250L)
                    .liveLogEnabled(false)
                    .queryRequestChannel(ReadClientTestSupport.queryChannel(queryPort))
                    .build();

            try (ReadReplicaNode replica = new ReadReplicaNode(replicaConfig, CoreConfig.defaults());
                    ReadClient readClient = new ReadClient(ReadClientTestSupport.clientConfig(queryPort))) {
                ReadClientTestSupport.awaitSupply(readClient, 0L, 100L, READ_TIMEOUT_MS);
                ReadClientTestSupport.awaitBalance(readClient, 0L, 100L, 1L, READ_TIMEOUT_MS);
                assertTrue(replica.isHealthy(), "replica must be healthy after loading the snapshot");
            }
        }
    }
}
