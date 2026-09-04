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
 * Fault-injection retry storm: kills the current leader while a command is
 * mid-flight, resends the same {@code (clientId, clientSeq)} until a new leader
 * acknowledges it, then storms further duplicate resends. Verifies the command
 * applies exactly once across the leadership change, no matter how many times it
 * is retried.
 *
 * <p>Tagged {@code fault}: timing-sensitive, run via the opt-in {@code faultTest}
 * task and never wired into {@code check}.
 */
@Tag("fault")
class RetryStormFailoverTest {

    private static final int NODE_COUNT = 3;
    private static final long CLIENT_ID = 7L;
    private static final int STORM = 100;
    private static final long TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(240)
    void retryStormAcrossFailoverAppliesExactlyOnce(@TempDir final Path baseDir) {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            client.send(CLIENT_ID, 0L, 0L, 100L, CommandType.CREDIT, 42L, 0L, 0L, 1_000L);
            assertTrue(client.awaitResult(100L, TIMEOUT_MS), "initial credit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(1_000L, client.lastBalance());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known before failover");

            // Send a debit, then kill the leader before (or just after) it applies.
            client.send(CLIENT_ID, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
            cluster.stopNode(leader);

            // Resend until the newly elected leader ACKs the single apply.
            boolean acked = false;
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (!acked && System.currentTimeMillis() < deadline) {
                client.send(CLIENT_ID, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
                acked = client.awaitResult(101L, 5_000L);
            }
            assertTrue(acked, "debit must be acknowledged after failover");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            // Storm further resends of the same sequence: every reply must replay the
            // cached result (999) rather than re-applying.
            for (int i = 0; i < STORM; i++) {
                client.send(CLIENT_ID, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
                assertTrue(client.awaitResult(101L, TIMEOUT_MS), "storm resend " + i);
                assertEquals(
                        StatusCode.SUCCESS, client.lastStatus(), "storm resend " + i + " must replay cached success");
                assertEquals(999L, client.lastBalance(), "storm resend " + i + " must not double-apply");
            }

            // Exactly-once: 1000 - 1 - 1 = 998.
            client.send(CLIENT_ID, 2L, 0L, 102L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
            assertTrue(client.awaitResult(102L, TIMEOUT_MS), "probe debit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(998L, client.lastBalance(), "debit must apply exactly once across failover");
        }
    }
}
