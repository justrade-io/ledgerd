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
 * Fault-injection test: kills the current leader while a client is mid-flight,
 * then retries the in-flight command with the SAME command id after the cluster
 * re-elects a leader. Verifies that failover neither loses nor double-applies the
 * command (idempotency holds across the leadership change).
 *
 * <p>Tagged {@code fault}: timing-sensitive, run via the opt-in {@code faultTest}
 * task and never wired into {@code check}.
 */
@Tag("fault")
class FaultInjectionTest {

    private static final int NODE_COUNT = 3;
    private static final long TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(240)
    void killingLeaderMidFlightPreservesIdempotency(@TempDir final Path baseDir) {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            // Commit an initial credit and learn who the leader is.
            client.send(7L, 0L, 0L, 100L, CommandType.CREDIT, 42L, 0L, 0L, 1_000L);
            assertTrue(client.awaitResult(100L, TIMEOUT_MS), "initial credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(1_000L, client.lastBalance());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known before failover");

            // Send a debit, then kill the leader. The retry below reuses the same
            // (clientSeq, commandId) so the dedup mechanism guarantees exactly-once.
            client.send(7L, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 400L);

            cluster.stopNode(leader);

            // Retry the same command until the new leader ACKs it.
            boolean acked = false;
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (!acked && System.currentTimeMillis() < deadline) {
                client.send(7L, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 400L);
                acked = client.awaitResult(101L, 5_000L);
            }
            assertTrue(acked, "debit must be acknowledged after failover");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            // The debit applied exactly once: 1000 - 400 = 600, regardless of retries.
            // Probe with a fresh credit of 1: balance must be exactly 601.
            client.send(7L, 2L, 0L, 102L, CommandType.CREDIT, 42L, 0L, 0L, 1L);
            assertTrue(client.awaitResult(102L, TIMEOUT_MS), "probe result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(601L, client.lastBalance(), "debit must apply exactly once across failover");
        }
    }
}
