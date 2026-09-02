package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test: starts a write cluster, then a read replica node with live
 * log following enabled. Verifies the read replica node starts and serves
 * queries, even when the live log subscriber cannot find a consensus recording
 * (graceful degradation).
 */
@Tag("integration")
class ReadReplicaLiveLogIntegrationTest {

    private ClusterNode clusterNode;
    private ReadReplicaNode replicaNode;
    private ReadClient client;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

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
    @Timeout(30)
    void startsWithLiveLogEnabled() {
        assertTrue(replicaNode.isHealthy(), "replica should be healthy with live log following enabled");
    }

    @Test
    @Timeout(30)
    void queryResponderServesQueries() {
        assertEquals(0L, client.totalSupply(0L).totalSupply(), "empty engine must report zero supply");
    }
}
