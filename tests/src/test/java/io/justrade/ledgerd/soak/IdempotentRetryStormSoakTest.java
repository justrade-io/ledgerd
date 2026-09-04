package io.justrade.ledgerd.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Single-node idempotent retry storm: resends the same {@code (clientId,
 * clientSeq)} many times and verifies the core replays the cached result without
 * re-applying, so a client-side storm of duplicate submits cannot corrupt the
 * balance.
 *
 * <p>Tagged {@code soak}: run via the opt-in {@code soakTest} task, never wired
 * into {@code check}.
 */
@Tag("soak")
class IdempotentRetryStormSoakTest {

    private static final long CLIENT_ID = 7L;
    private static final int STORM = 1_000;
    private static final long TIMEOUT_MS = 30_000L;

    @Test
    @Timeout(120)
    void retryStormNeverDoubleApplies(@TempDir final Path baseDir) {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        try (ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(ClusterConfig.ingressEndpoints(1))) {

            client.send(CLIENT_ID, 0L, 0L, 100L, CommandType.CREDIT, 42L, 0L, 0L, 1_000L);
            assertTrue(client.awaitResult(100L, TIMEOUT_MS), "initial credit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(1_000L, client.lastBalance());

            client.send(CLIENT_ID, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
            assertTrue(client.awaitResult(101L, TIMEOUT_MS), "initial debit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(999L, client.lastBalance());

            for (int i = 0; i < STORM; i++) {
                client.send(CLIENT_ID, 1L, 0L, 101L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
                assertTrue(client.awaitResult(101L, TIMEOUT_MS), "storm resend " + i);
                assertEquals(
                        StatusCode.SUCCESS, client.lastStatus(), "storm resend " + i + " must replay cached success");
                assertEquals(999L, client.lastBalance(), "storm resend " + i + " must not double-apply");
            }

            // A fresh debit must land on 998, proving the storm applied exactly once.
            client.send(CLIENT_ID, 2L, 0L, 102L, CommandType.DEBIT, 42L, 0L, 0L, 1L);
            assertTrue(client.awaitResult(102L, TIMEOUT_MS), "probe debit");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            assertEquals(998L, client.lastBalance(), "debit must apply exactly once despite the retry storm");
        }
    }
}
