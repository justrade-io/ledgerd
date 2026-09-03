package io.justrade.ledgerd.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import io.justrade.ledgerd.testkit.MultiNodeCluster;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Multi-node test for a {@code TransferBatch}: a linked chain commits on a
 * three-node Raft cluster, and resending the same batch after a leader kill is
 * idempotent (applies exactly once across failover).
 *
 * <p>Tagged {@code cluster}: run via the opt-in {@code clusterTest} task, never
 * wired into {@code check}.
 */
@Tag("cluster")
class TransferBatchClusterTest {

    private static final int NODE_COUNT = 3;
    private static final long TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(240)
    void batchSurvivesLeaderFailoverExactlyOnce(@TempDir final Path baseDir) {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            client.send(7L, 0L, 0L, 100L, CommandType.CREDIT, 42L, 0L, 0L, 1_000L);
            assertTrue(client.awaitResult(100L, TIMEOUT_MS), "initial credit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known before failover");

            // Linked chain [42->43 100 (linked), 42->44 50], then kill the leader.
            final long[] from = {42L, 42L};
            final long[] to = {43L, 44L};
            final long[] amount = {100L, 50L};
            final long[] asset = {0L, 0L};
            final boolean[] linked = {true, false};

            client.sendBatch(7L, 1L, 0L, 101L, from, to, amount, asset, linked);
            cluster.stopNode(leader);

            // Resend the same batch until the new leader ACKs it.
            boolean acked = false;
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (!acked && System.currentTimeMillis() < deadline) {
                client.sendBatch(7L, 1L, 0L, 101L, from, to, amount, asset, linked);
                acked = client.awaitBatchResult(101L, 5_000L);
            }
            assertTrue(acked, "batch must be acknowledged after failover");
            assertEquals(2, client.lastBatchLegCount());
            assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(0));
            assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(1));

            // Exactly-once: 1000 - 100 - 50 = 850. A +1 credit must land on 851.
            client.send(7L, 2L, 0L, 102L, CommandType.CREDIT, 42L, 0L, 0L, 1L);
            assertTrue(client.awaitResult(102L, TIMEOUT_MS), "probe credit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(851L, client.lastBalance(), "batch must apply exactly once across failover");
        }
    }
}
