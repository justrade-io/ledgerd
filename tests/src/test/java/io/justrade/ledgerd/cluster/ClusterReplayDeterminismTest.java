package io.justrade.ledgerd.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives the same command stream against two independent single-node clusters
 * and asserts the observable result balances are identical, verifying that the
 * replicated state machine executes deterministically over the real
 * ingress/consensus/egress path (complementing the engine-level replay test).
 *
 * <p>Tagged {@code cluster}: run via the opt-in {@code clusterTest} task.
 */
@Tag("cluster")
class ClusterReplayDeterminismTest {

    private static final long TIMEOUT_MS = 20_000L;

    // Fixed, deterministic command stream: {type, accountA, accountB, amount}.
    private static final long[][] COMMANDS = {
        {CommandType.CREDIT.value(), 1, 0, 500},
        {CommandType.CREDIT.value(), 2, 0, 300},
        {CommandType.TRANSFER.value(), 1, 2, 100},
        {CommandType.DEBIT.value(), 2, 0, 50},
        {CommandType.CREDIT.value(), 1, 0, 25},
        {CommandType.TRANSFER.value(), 2, 1, 200},
        {CommandType.DEBIT.value(), 1, 0, 75},
    };

    @Test
    @Timeout(120)
    void identicalCommandStreamsProduceIdenticalBalances(@TempDir final Path baseDir) {
        final long[] balancesA = runAgainstFreshCluster(0, baseDir.resolve("a"));
        final long[] balancesB = runAgainstFreshCluster(1, baseDir.resolve("b"));
        assertArrayEquals(balancesA, balancesB, "deterministic execution across independent clusters");
    }

    private static long[] runAgainstFreshCluster(final int nodeId, final Path dir) {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(nodeId, dir);
        final String ingress = nodeId + "=localhost:" + (ClusterConfig.PORT_BASE + nodeId * ClusterConfig.PORT_STRIDE);
        try (ClusterNode node = new ClusterNode(config, CoreConfig.defaults());
                ClusterTestClient client = new ClusterTestClient(config.aeronDirectoryName(), ingress)) {

            final long[] balances = new long[COMMANDS.length];
            long commandId = 0L;
            for (int i = 0; i < COMMANDS.length; i++) {
                final long[] c = COMMANDS[i];
                commandId++;
                client.send(1L, i, 0L, commandId, CommandType.get((short) c[0]), c[1], c[2], 0L, c[3]);
                assertTrue(client.awaitResult(commandId, TIMEOUT_MS), "result for command " + commandId);
                balances[i] = client.lastBalance();
            }
            return balances;
        }
    }
}
