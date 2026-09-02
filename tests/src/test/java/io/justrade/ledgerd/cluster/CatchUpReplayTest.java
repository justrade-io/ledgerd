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
 * Verifies that a node stopped while commits continue can be restarted with its
 * prior state preserved ({@code cleanStart == false}), recover its log, and
 * catch up so it can again participate in consensus.
 *
 * <p>The catch-up is proven indirectly: after the recovered node rejoins, a
 * second node is stopped so that availability depends on the recovered node
 * forming the two-of-three majority; a subsequent command still commits with the
 * correct balance.
 *
 * <p>Tagged {@code cluster}: run via the opt-in {@code clusterTest} task.
 */
@Tag("cluster")
class CatchUpReplayTest {

    private static final int NODE_COUNT = 3;
    private static final long TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(240)
    void restartedFollowerCatchesUpAndRejoinsConsensus(@TempDir final Path baseDir) {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            client.send(9L, 0L, 0L, 1L, CommandType.CREDIT, 42L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(500L, client.lastBalance());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known");
            final int followerA = (leader + 1) % NODE_COUNT;
            final int followerB = (leader + 2) % NODE_COUNT;

            // Stop follower A, then keep committing with leader + follower B.
            cluster.stopNode(followerA);

            client.send(9L, 1L, 0L, 2L, CommandType.TRANSFER, 42L, 99L, 0L, 200L);
            assertTrue(client.awaitResult(2L, TIMEOUT_MS), "transfer result while A down");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(300L, client.lastBalance());

            // Restart follower A, preserving its state so it recovers and catches up.
            cluster.restartNode(followerA);

            // Now stop follower B: availability requires the recovered node A to
            // form the majority with the leader. A must have caught up.
            cluster.stopNode(followerB);

            client.send(9L, 2L, 0L, 3L, CommandType.CREDIT, 42L, 0L, 0L, 100L);
            assertTrue(client.awaitResult(3L, TIMEOUT_MS), "credit result after A rejoins");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(400L, client.lastBalance(), "recovered node must serve the correct state");
        }
    }
}
