package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for the live-log follower's fragment reassembly. A transfer
 * batch larger than one MTU fragment (256 legs, ~8 KB) must be reassembled
 * before it is applied to the read replica's engine; otherwise the replica
 * never converges to the transferred balances. This drives a real batch through
 * the write cluster and asserts the read replica follows it via the live log.
 */
@Tag("integration")
class ReadReplicaLiveLogBatchIntegrationTest {

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

    @Test
    @Timeout(120)
    void readReplicaFollowsLargeTransferBatchViaLiveLog(@TempDir final Path tempDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

        final int legs = 256;

        // Seed account 100 with enough units to cover the batch (single command,
        // single fragment).
        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, legs);
            assertTrue(writeClient.awaitResult(1L, RESULT_TIMEOUT_MS), "seed credit result");
            assertTrue(writeClient.lastStatus() == StatusCode.SUCCESS, "seed credit success");
        }

        startReplica();

        // The replica follows the live log from position 0 (no snapshot taken), so
        // this also confirms it has consumed the seed credit before the batch.
        ReadClientTestSupport.awaitSupply(client, 0L, legs, READ_TIMEOUT_MS);

        final long[] fromIds = new long[legs];
        final long[] toIds = new long[legs];
        final long[] amounts = new long[legs];
        final long[] assetIds = new long[legs];
        final boolean[] linked = new boolean[legs];
        Arrays.fill(fromIds, 100L);
        Arrays.fill(toIds, 200L);
        Arrays.fill(amounts, 1L);

        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.sendBatch(1L, 0L, 0L, 1L, fromIds, toIds, amounts, assetIds, linked);
            assertTrue(writeClient.awaitBatchResult(1L, RESULT_TIMEOUT_MS), "batch result");
        }

        // A transfer batch conserves supply and moves funds 100 -> 200. Reaching
        // these balances proves the multi-fragment batch was reassembled and
        // applied, not dropped or partially decoded.
        ReadClientTestSupport.awaitBalance(client, 0L, 200L, legs, READ_TIMEOUT_MS);
        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 0L, READ_TIMEOUT_MS);
    }

    private void startReplica() {
        final int queryPort = ReadClientTestSupport.freeUdpPort();
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .pollIntervalMs(60_000L)
                .liveLogEnabled(true)
                .queryRequestChannel(ReadClientTestSupport.queryChannel(queryPort))
                .build();
        replicaNode = new ReadReplicaNode(replicaConfig, CoreConfig.defaults());
        client = new ReadClient(ReadClientTestSupport.clientConfig(queryPort));
    }
}
