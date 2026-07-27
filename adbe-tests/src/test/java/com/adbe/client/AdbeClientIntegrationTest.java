package com.adbe.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.client.config.ClientConfig;
import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the Edge-side {@link AdbeClient} against a real single-node cluster:
 * async submit, command-id correlation via {@link ResultHandler}, and
 * end-to-end latency recording.
 */
@Tag("integration")
class AdbeClientIntegrationTest {

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

    @Test
    @Timeout(60)
    void submitsCommandsAndCorrelatesResults() {
        final long[] lastBalance = {Long.MIN_VALUE};
        final StatusCode[] lastStatus = {StatusCode.NULL_VAL};
        final long[] lastCommandIdLo = {-1L};
        final AtomicInteger results = new AtomicInteger();

        final ResultHandler handler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            lastCommandIdLo[0] = idLo;
            lastStatus[0] = status;
            if (hasBalance) {
                lastBalance[0] = balance;
            }
            results.incrementAndGet();
        };

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (AdbeClient client = new AdbeClient(config, handler)) {
            final long creditId = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
            awaitResult(client, creditId, lastCommandIdLo);
            assertEquals(StatusCode.SUCCESS, lastStatus[0]);
            assertEquals(500L, lastBalance[0]);

            final long transferId = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            awaitResult(client, transferId, lastCommandIdLo);
            assertEquals(StatusCode.SUCCESS, lastStatus[0]);
            assertEquals(350L, lastBalance[0]);

            assertEquals(2L, client.completed());
            assertEquals(0, client.pendingCount());
            assertTrue(client.latencyHistogram().getTotalCount() >= 2, "latency samples recorded");
        }
    }

    private static void awaitResult(final AdbeClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no result for commandIdLo=" + commandIdLo);
    }
}
