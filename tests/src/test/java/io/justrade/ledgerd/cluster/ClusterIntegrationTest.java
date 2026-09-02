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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test over a real single-node Aeron cluster: commands flow through
 * consensus into {@code BalanceService} and results return via egress. Also
 * verifies idempotency across the real ingress/egress path.
 */
@Tag("integration")
class ClusterIntegrationTest {

    private static final long TIMEOUT_MS = 15_000L;

    private ClusterNode node;
    private ClusterConfig clusterConfig;

    @BeforeEach
    void startNode(@TempDir final Path baseDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        node = new ClusterNode(clusterConfig, CoreConfig.defaults());
    }

    @AfterEach
    void stopNode() {
        if (node != null) {
            node.close();
        }
    }

    private String ingressEndpoints() {
        return "0=localhost:20100";
    }

    @Test
    @Timeout(60)
    void commandsFlowThroughConsensusAndIdempotencyHolds() {
        try (ClusterTestClient client = new ClusterTestClient(clusterConfig.aeronDirectoryName(), ingressEndpoints())) {
            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(500L, client.lastBalance());

            client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            assertTrue(client.awaitResult(2L, TIMEOUT_MS), "transfer result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(350L, client.lastBalance());

            // Idempotent retry: same (clientId, clientSeq, commandId) must not re-apply.
            client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            assertTrue(client.awaitResult(2L, TIMEOUT_MS), "duplicate transfer result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(350L, client.lastBalance());

            // A fresh debit confirms the transfer applied exactly once (balance still 350).
            client.send(1L, 2L, 0L, 3L, CommandType.DEBIT, 100L, 0L, 0L, 350L);
            assertTrue(client.awaitResult(3L, TIMEOUT_MS), "debit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(0L, client.lastBalance());
        }
    }
}
