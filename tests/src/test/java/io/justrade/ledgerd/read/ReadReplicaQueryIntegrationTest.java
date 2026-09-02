package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.client.BalanceResult;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end read-side test over the read replica path: writes via a
 * {@link ClusterTestClient} against a single-node write cluster, then reads back
 * through the read-client SDK from a {@link ReadReplicaNode}. The read replica
 * follows the consensus log from position 0 (no snapshot required), so the read
 * model is complete - it reflects both sides of a transfer, unlike the egress
 * stream which only carries the sender's balance.
 */
@Tag("integration")
class ReadReplicaQueryIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long READ_TIMEOUT_MS = 30_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    private ClusterNode clusterNode;
    private ClusterConfig clusterConfig;
    private ReadReplicaNode replicaNode;
    private ReadClient client;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

        // Live log following from position 0 means reads converge without any
        // snapshot; a long poll interval keeps snapshot loading out of the test.
        final int queryPort = ReadClientTestSupport.freeUdpPort();
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .liveLogEnabled(true)
                .pollIntervalMs(60_000L)
                .queryRequestChannel(ReadClientTestSupport.queryChannel(queryPort))
                .build();
        replicaNode = new ReadReplicaNode(replicaConfig, CoreConfig.defaults());
        client = new ReadClient(ReadClientTestSupport.clientConfig(queryPort));
    }

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
    @Timeout(90)
    void readsReflectCommittedWrites() {
        try (ClusterTestClient clusterClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            clusterClient.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(clusterClient.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, clusterClient.lastStatus());

            clusterClient.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            assertTrue(clusterClient.awaitResult(2L, RESULT_TIMEOUT_MS), "transfer result");
            assertEquals(StatusCode.SUCCESS, clusterClient.lastStatus());

            clusterClient.send(1L, 2L, 0L, 3L, CommandType.APPROVE, 1L, 9L, 0L, 200L);
            assertTrue(clusterClient.awaitResult(3L, RESULT_TIMEOUT_MS), "approve result");
            assertEquals(StatusCode.SUCCESS, clusterClient.lastStatus());
        }

        // Sender lost 150 (500 -> 350); this side is visible on egress.
        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 350L, READ_TIMEOUT_MS);
        // Recipient gained 150 (0 -> 150); only the read model has this side.
        ReadClientTestSupport.awaitBalance(client, 0L, 200L, 150L, READ_TIMEOUT_MS);
        // Total supply is conserved by the transfer.
        ReadClientTestSupport.awaitSupply(client, 0L, 500L, READ_TIMEOUT_MS);
        // Allowance set by APPROVE.
        ReadClientTestSupport.awaitAllowance(client, 0L, 1L, 9L, 200L, READ_TIMEOUT_MS);

        // Batch: two known accounts plus one that does not exist.
        final List<BalanceResult> batch = client.batchBalances(0L, 100L, 200L, 999L);
        assertEquals(3, batch.size(), "batch must echo every requested account");
        assertEquals(new BalanceResult(100L, 350L, true, batch.get(0).appliedPosition()), batch.get(0));
        assertEquals(new BalanceResult(200L, 150L, true, batch.get(1).appliedPosition()), batch.get(1));
        final BalanceResult missing = batch.get(2);
        assertEquals(999L, missing.accountId());
        assertFalse(missing.found(), "account 999 should not exist");
    }
}
