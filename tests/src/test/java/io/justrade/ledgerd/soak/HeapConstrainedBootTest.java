package io.justrade.ledgerd.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.core.BalanceEngine;
import io.justrade.ledgerd.core.BatchOutcome;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import io.justrade.ledgerd.testkit.TransferBatchFixtures;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jol.info.GraphLayout;

/**
 * End-to-end check of the {@code heap = heapLive / 0.65} sizing formula from the
 * capacity-planning model, run under a real fixed {@code -Xmx} (the opt-in
 * {@code capacityTest} Gradle task pins {@code minHeapSize = maxHeapSize = 2g}).
 *
 * <p>Targets the smallest deploy row (4 vCPU / 8 GB, 2g heap, accountCapacity
 * 2^20): a single-node cluster boots alongside an engine filled to the Pareto
 * live set (32 assets, the hottest at the 0.65 safe-fill ceiling, 16 batch
 * clients). The engine is driven directly rather than through Raft because 680K
 * consensus round-trips would dominate the runtime; the heap-relevant state is
 * the on-heap stores, while Aeron term buffers are off-heap and small.
 *
 * <p>Tagged {@code capacity}: run via the opt-in {@code capacityTest} task, never
 * wired into {@code check}.
 */
@Tag("capacity")
class HeapConstrainedBootTest {

    private static final int TARGET_ASSETS = 32;
    private static final int HOT_ACCOUNTS = (int) (CoreConfig.DEFAULT_ACCOUNT_CAPACITY * 0.65f);
    private static final int SPARSE_ACCOUNTS_PER_ASSET = 8;
    private static final int BATCH_CLIENTS = 16;

    @Test
    @Timeout(300)
    void bootsAndFillsTargetLiveSetWithoutOom(@TempDir final Path baseDir) {
        final CoreConfig config = CoreConfig.defaults();

        try (ClusterNode node = new ClusterNode(ClusterConfig.singleNodeLocalhost(0, baseDir), config)) {
            assertNotNull(node.metrics(), "node must boot and wire its metrics");
            final BalanceEngine engine = new BalanceEngine(config, new CoreMetrics());
            fillPareto(engine, config);

            final long expectedAccounts = (long) HOT_ACCOUNTS + (TARGET_ASSETS - 1) * SPARSE_ACCOUNTS_PER_ASSET;
            assertEquals(expectedAccounts, engine.balances().size(), "every account must be created");
            assertEquals(TARGET_ASSETS, engine.balances().assetCount(), "every asset bucket must be touched");

            final long live = GraphLayout.parseInstance(engine).totalSize();
            final long headroom = (long) (Runtime.getRuntime().maxMemory() * 0.65);
            assertTrue(live < headroom, "live set " + live + " bytes must fit within 65% of heap (" + headroom + ")");
        }
    }

    private static void fillPareto(final BalanceEngine engine, final CoreConfig config) {
        final CommandFixtures commands = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();
        long seq = 1L;

        // The hottest asset approaches the 0.65 safe-fill ceiling without resizing.
        for (long account = 1; account <= HOT_ACCOUNTS; account++) {
            engine.process(commands.encode(1L, seq, 0L, seq, CommandType.CREDIT, 0L, account, 0L, 0L, 1_000L), outcome);
            seq++;
        }

        // Remaining assets stay sparse but still pay the full bucket allocation.
        for (long asset = 1; asset < TARGET_ASSETS; asset++) {
            for (long i = 1; i <= SPARSE_ACCOUNTS_PER_ASSET; i++) {
                engine.process(
                        commands.encode(1L, seq, 0L, seq, CommandType.CREDIT, asset, seq, 0L, 0L, 1_000L), outcome);
                seq++;
            }
        }

        // One batch per client allocates that client's full batch dedup ring.
        final TransferBatchFixtures batches = new TransferBatchFixtures();
        final BatchOutcome batchOutcome = new BatchOutcome(config.maxBatchSize());
        for (long client = 1; client <= BATCH_CLIENTS; client++) {
            engine.processBatch(
                    batches.encode(
                            client,
                            0L,
                            0L,
                            0L,
                            new long[] {1L},
                            new long[] {2L},
                            new long[] {1L},
                            new long[] {0L},
                            new boolean[] {false}),
                    batchOutcome);
        }
    }
}
