package io.justrade.ledgerd.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for a {@code TransferBatch} over a real single-node cluster:
 * a linked chain flows through consensus and ACKs per-leg, a resend with the
 * same {@code (clientId, clientSeq)} is idempotent, and a failing chain rolls
 * back every leg.
 */
@Tag("integration")
class TransferBatchClusterIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    @Test
    @Timeout(60)
    void batchFlowsThroughConsensusAndIsIdempotent(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());
                ClusterTestClient client =
                        new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {

            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, TIMEOUT_MS), "credit 100");
            client.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 300L, 0L, 0L, 300L);
            assertTrue(client.awaitResult(2L, TIMEOUT_MS), "credit 300");

            // Atomic linked chain [100->200 30 (linked), 300->400 20].
            final long[] from = {100L, 300L};
            final long[] to = {200L, 400L};
            final long[] amount = {30L, 20L};
            final long[] asset = {0L, 0L};
            final boolean[] linked = {true, false};

            client.sendBatch(1L, 2L, 0L, 3L, from, to, amount, asset, linked);
            assertTrue(client.awaitBatchResult(3L, TIMEOUT_MS), "batch result");
            assertEquals(2, client.lastBatchLegCount());
            assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(0));
            assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(1));
            assertEquals(470L, client.lastBatchBalance(0), "leg 0 resultBalance is 100's new balance");
            assertEquals(280L, client.lastBatchBalance(1), "leg 1 resultBalance is 300's new balance");

            // Idempotent resend: same (clientId, clientSeq) must not re-apply.
            client.sendBatch(1L, 2L, 0L, 3L, from, to, amount, asset, linked);
            assertTrue(client.awaitBatchResult(3L, TIMEOUT_MS), "duplicate batch result");
            assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(0));
            assertEquals(470L, client.lastBatchBalance(0), "batch must not double-apply");

            // Exactly-once: account 100 is 500 - 30 = 470.
            client.send(1L, 3L, 0L, 4L, CommandType.DEBIT, 100L, 0L, 0L, 470L);
            assertTrue(client.awaitResult(4L, TIMEOUT_MS), "debit probe");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(0L, client.lastBalance());
        }
    }

    @Test
    @Timeout(60)
    void failingLinkedChainRollsBackEveryLeg(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());
                ClusterTestClient client =
                        new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {

            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, TIMEOUT_MS), "credit 100");

            // Chain [100->200 60 (linked), 200->300 9999] fails on the second leg;
            // the whole chain rolls back and every leg returns the failure status.
            client.sendBatch(
                    1L,
                    1L,
                    0L,
                    2L,
                    new long[] {100L, 200L},
                    new long[] {200L, 300L},
                    new long[] {60L, 9_999L},
                    new long[] {0L, 0L},
                    new boolean[] {true, false});
            assertTrue(client.awaitBatchResult(2L, TIMEOUT_MS), "batch result");
            assertEquals(2, client.lastBatchLegCount());
            assertEquals(StatusCode.INSUFFICIENT_BALANCE, client.lastBatchStatus(0));
            assertEquals(StatusCode.INSUFFICIENT_BALANCE, client.lastBatchStatus(1));

            // Rollback restores account 100 to its full 500 (auto-created 200 removed).
            client.send(1L, 2L, 0L, 3L, CommandType.DEBIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(3L, TIMEOUT_MS), "debit probe");
            assertEquals(StatusCode.SUCCESS, client.lastStatus(), "rollback must restore account 100 to 500");
            assertEquals(0L, client.lastBalance());
        }
    }
}
