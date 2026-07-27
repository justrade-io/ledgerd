package com.adbe.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import com.adbe.testkit.ClusterTestClient;
import com.adbe.testkit.MultiNodeCluster;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test over a real three-node Aeron cluster: verifies leader election
 * and that commands are committed through consensus and produce deterministic
 * results, including idempotent retries across the real ingress/egress path.
 *
 * <p>Tagged {@code cluster}: heavier than single-node integration and run via the
 * opt-in {@code clusterTest} task, not the default {@code check} gate.
 */
@Tag("cluster")
class MultiNodeClusterTest {

    private static final int NODE_COUNT = 3;
    private static final long TIMEOUT_MS = 20_000L;

    @Test
    @Timeout(180)
    void threeNodeClusterElectsLeaderAndCommits(@TempDir final Path baseDir) {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(500L, client.lastBalance());
            assertTrue(client.leaderMemberId() >= 0, "a leader should have been elected");

            client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            assertTrue(client.awaitResult(2L, TIMEOUT_MS), "transfer result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(350L, client.lastBalance());

            // Idempotent retry across the real path: same (clientSeq, commandId) is a no-op.
            client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            assertTrue(client.awaitResult(2L, TIMEOUT_MS), "duplicate transfer result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(350L, client.lastBalance());
        }
    }
}
