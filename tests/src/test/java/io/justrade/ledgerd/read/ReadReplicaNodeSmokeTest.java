package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.client.TotalSupplyResult;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test: starts a single-node write cluster (BalanceService), then a
 * read replica node that connects to the cluster's Archive. Verifies the read
 * replica's Aeron query responder answers correctly even when no snapshot has
 * been taken yet (empty engine).
 */
@Tag("integration")
class ReadReplicaNodeSmokeTest {

    private ClusterNode clusterNode;
    private ReadReplicaNode replicaNode;
    private ReadClient client;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        // Start a single-node write cluster with BalanceService. The
        // ClusterNode brings up an embedded Media Driver + Archive that the read
        // replica node connects to for snapshot polling.
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

        final int queryPort = ReadClientTestSupport.freeUdpPort();
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
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
    @Timeout(30)
    void replicaReportsHealthy() {
        assertTrue(replicaNode.isHealthy(), "replica should be following the cluster archive");
    }

    @Test
    @Timeout(30)
    void supplyReturnsZeroWhenNoSnapshotYet() {
        final TotalSupplyResult result = client.totalSupply(0L);
        assertEquals(0L, result.totalSupply(), "expected totalSupply=0 when no snapshot loaded");
    }

    @Test
    @Timeout(30)
    void balanceReturnsMissingAccount() {
        final var result = client.balance(0L, 999L);
        assertFalse(result.found(), "expected account 999 to be missing");
        assertEquals(0L, result.balance());
    }
}
