package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.client.BalanceResult;
import io.justrade.ledgerd.read.client.QueryListener;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the Edge-side {@link ReadClient} against a real read replica:
 * synchronous typed queries and asynchronous request-id correlation via
 * {@link QueryListener}.
 */
@Tag("integration")
class ReadClientIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    private ClusterNode clusterNode;
    private ClusterConfig clusterConfig;
    private ReadReplicaNode replicaNode;
    private ReadClient client;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
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
    @Timeout(60)
    void syncQueriesReturnTypedResults() {
        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(writeClient.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());
        }

        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 500L, RESULT_TIMEOUT_MS);
        ReadClientTestSupport.awaitSupply(client, 0L, 500L, RESULT_TIMEOUT_MS);
        assertTrue(client.completed() >= 1, "sync queries must have completed");
        assertEquals(0L, client.pendingCount(), "sync queries must not leave pending slots");
    }

    @Test
    @Timeout(60)
    void asyncSubmitCorrelatesResultByRequestId() {
        try (ClusterTestClient writeClient =
                new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            writeClient.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(writeClient.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, writeClient.lastStatus());
        }

        ReadClientTestSupport.awaitBalance(client, 0L, 100L, 500L, RESULT_TIMEOUT_MS);

        final AtomicLong deliveredRequestId = new AtomicLong(-1L);
        final AtomicReference<BalanceResult> delivered = new AtomicReference<>();
        client.setListener(new QueryListener() {
            @Override
            public void onBalance(final long requestId, final BalanceResult result) {
                deliveredRequestId.set(requestId);
                delivered.set(result);
            }
        });

        final long requestId = client.submitBalance(0L, 100L);
        final long deadline = System.currentTimeMillis() + RESULT_TIMEOUT_MS;
        while (delivered.get() == null && System.currentTimeMillis() < deadline) {
            client.poll();
            Thread.onSpinWait();
        }

        final BalanceResult result = delivered.get();
        assertTrue(result != null, "async query must be delivered through the listener");
        assertEquals(requestId, deliveredRequestId.get(), "listener must receive the matching request id");
        assertEquals(500L, result.balance());
        assertTrue(result.found());
        assertEquals(0L, client.pendingCount(), "delivered query must leave the pending set");
    }
}
