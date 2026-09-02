package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterTool;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end replication test for the read replica node. Unlike the smoke
 * tests, this drives real commands into the write cluster, forces a snapshot via
 * {@link ClusterTool}, and asserts the read replica downloads and serves that
 * state through the read-client SDK - then keeps writing and asserts the live
 * log converges without another snapshot.
 */
@Tag("integration")
class ReadReplicaReplicationIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long READ_TIMEOUT_MS = 30_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    private ClusterNode clusterNode;
    private ClusterConfig clusterConfig;
    private ReadReplicaNode replicaNode;
    private ReadClient client;

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        if (replicaNode != null) {
            replicaNode.close();
        }
        if (clusterNode != null) {
            clusterNode.close();
        }
    }

    private void startCluster(final Path tempDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);
    }

    private void startReplica(final boolean liveLogEnabled) {
        final int queryPort = ReadClientTestSupport.freeUdpPort();
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .pollIntervalMs(250L)
                .liveLogEnabled(liveLogEnabled)
                .queryRequestChannel(ReadClientTestSupport.queryChannel(queryPort))
                .build();
        replicaNode = new ReadReplicaNode(replicaConfig, CoreConfig.defaults());
        client = new ReadClient(ReadClientTestSupport.clientConfig(queryPort));
    }

    @Test
    @Timeout(120)
    void readReplicaLoadsSnapshotAndServesReplicatedState(@TempDir final Path tempDir) {
        startCluster(tempDir);

        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(writeClient.awaitResult(1L, RESULT_TIMEOUT_MS), "credit 100 result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());

            writeClient.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 200L, 0L, 0L, 300L);
            assertTrue(writeClient.awaitResult(2L, RESULT_TIMEOUT_MS), "credit 200 result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());
        }

        // Force a snapshot so the read replica has a recording to download.
        assertTrue(ClusterTool.snapshot(clusterConfig.clusterDir(), System.out), "snapshot trigger accepted");

        startReplica(true);

        ReadClientTestSupport.awaitSupply(client, 0L, 800L, READ_TIMEOUT_MS);
        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 500L, READ_TIMEOUT_MS);
        ReadClientTestSupport.awaitBalance(client, 0L, 200L, 300L, READ_TIMEOUT_MS);
        ReadClientTestSupport.awaitMissing(client, 0L, 999L, READ_TIMEOUT_MS);
    }

    @Test
    @Timeout(120)
    void readReplicaFollowsLiveLogBetweenSnapshots(@TempDir final Path tempDir) {
        startCluster(tempDir);

        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(writeClient.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());
        }

        assertTrue(ClusterTool.snapshot(clusterConfig.clusterDir(), System.out), "snapshot trigger accepted");

        startReplica(true);
        ReadClientTestSupport.awaitSupply(client, 0L, 500L, READ_TIMEOUT_MS);

        // A new command committed AFTER the snapshot must reach the read replica via
        // the live log, with no further snapshot taken.
        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 100L, 0L, 0L, 250L);
            assertTrue(writeClient.awaitResult(2L, RESULT_TIMEOUT_MS), "post-snapshot credit result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());
        }

        ReadClientTestSupport.awaitSupply(client, 0L, 750L, READ_TIMEOUT_MS);
        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 750L, READ_TIMEOUT_MS);
    }

    @Test
    @Timeout(120)
    void readReplicaLoadsLargeSnapshotWithLiveLogDisabled(@TempDir final Path tempDir) {
        startCluster(tempDir);

        // Credit 100 accounts so the service snapshot (header + 100 balances + 100
        // dedup entries + footer, plus cluster-schema framing) exceeds the read
        // node's 64-fragment poll batch. Live log is DISABLED, so the replica can
        // only serve state loaded from the snapshot.
        final int accounts = 100;
        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            for (int i = 0; i < accounts; i++) {
                writeClient.send(1L, i, 0L, i + 1L, CommandType.CREDIT, i + 1L, 0L, 0L, 1L);
            }
            assertTrue(writeClient.awaitResult(accounts, RESULT_TIMEOUT_MS), "last credit result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());
        }

        assertTrue(ClusterTool.snapshot(clusterConfig.clusterDir(), System.out), "snapshot trigger accepted");

        startReplica(false);

        ReadClientTestSupport.awaitSupply(client, 0L, 100L, READ_TIMEOUT_MS);
        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 1L, READ_TIMEOUT_MS);
    }
}
