package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Acceptance test for the read-node HA design (ADR 0008): when the cluster
 * member whose Archive the read replica follows (node 0) dies, the read replica
 * must fail over to a surviving member's Archive and keep converging on the
 * committed log.
 *
 * <p>The read replica is configured with ALL THREE member Archive endpoints. It
 * follows the first reachable one (node 0) and, when node 0 dies, fails over to
 * a surviving member's Archive (round-robin with backoff), keeping its engine
 * state; the engine's command-id dedup makes re-applying the already-seen log
 * prefix idempotent, so reads converge on the new state instead of freezing.
 *
 * <p>The write cluster keeps quorum (2 of 3 survive), so writes still commit
 * after node 0 dies; only the read replica's data source is gone.
 *
 * <p>Tagged {@code fault}: timing-sensitive, run via the opt-in {@code faultTest}
 * task, never wired into {@code check}.
 */
@Tag("fault")
class ReadReplicaArchiveFailoverFaultTest {

    private static final int NODE_COUNT = 3;
    private static final long RESULT_TIMEOUT_MS = 30_000L;
    private static final long RETRY_WINDOW_MS = 5_000L;
    private static final long READ_TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(240)
    void readReplicaFollowsClusterAfterArchiveSourceNodeDies(@TempDir final Path baseDir) throws Exception {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            // ClusterConfig.multiNodeLocalhost is a pure function of baseDir, so these
            // channels match the nodes MultiNodeCluster launched above.
            final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODE_COUNT, baseDir);

            // All three member Archive endpoints (ADR 0008): the replica follows
            // the first reachable one and fails over to a survivor when node 0 dies.
            final int queryPort = ReadClientTestSupport.freeUdpPort();
            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                    .archiveControlChannels(
                            configs[0].archiveControlChannel(),
                            configs[1].archiveControlChannel(),
                            configs[2].archiveControlChannel())
                    .pollIntervalMs(250L)
                    .liveLogEnabled(true)
                    .queryRequestChannel(ReadClientTestSupport.queryChannel(queryPort))
                    .build();

            try (ReadReplicaNode replica = new ReadReplicaNode(replicaConfig, CoreConfig.defaults());
                    ReadClient readClient = new ReadClient(ReadClientTestSupport.clientConfig(queryPort))) {

                // Initial credit; the replica converges via node 0's Archive live log.
                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "initial credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());
                ReadClientTestSupport.awaitSupply(readClient, 0L, 500L, READ_TIMEOUT_MS);

                // Kill the Archive source node. Quorum survives (2 of 3).
                cluster.stopNode(0);

                // Commit more through the surviving members, retrying across any leader
                // change with the same command id (idempotent).
                boolean acked = false;
                final long deadline = System.currentTimeMillis() + RESULT_TIMEOUT_MS;
                while (!acked && System.currentTimeMillis() < deadline) {
                    client.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 100L, 0L, 0L, 250L);
                    acked = client.awaitResult(2L, RETRY_WINDOW_MS);
                }
                assertTrue(acked, "post-kill credit must be acknowledged by the surviving quorum");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                // ACCEPTANCE: the read replica must fail over to a surviving member's
                // Archive and converge on the new state (ADR 0008).
                ReadClientTestSupport.awaitSupply(readClient, 0L, 750L, READ_TIMEOUT_MS);
                assertTrue(replica.isHealthy(), "replica must be healthy after failing over to a survivor");
            }
        }
    }
}
