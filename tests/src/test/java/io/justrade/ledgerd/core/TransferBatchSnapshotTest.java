package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import io.justrade.ledgerd.testkit.InMemorySnapshot;
import io.justrade.ledgerd.testkit.TransferBatchFixtures;
import org.junit.jupiter.api.Test;

/**
 * Verifies that transfer-batch dedup records are included in the snapshot, so
 * batch idempotency survives a node restart exactly as single-command
 * idempotency does.
 */
class TransferBatchSnapshotTest {

    @Test
    void batchDedupSurvivesSnapshotRoundTrip() {
        final CoreConfig config = CoreConfig.of(1024, 64, 8, 64, 16);
        final BalanceEngine source = new BalanceEngine(config, new CoreMetrics());
        final CommandOutcome commandOutcome = new CommandOutcome();
        final BatchOutcome batchOutcome = new BatchOutcome(8);

        source.process(
                new CommandFixtures().encode(1L, 0L, 0L, 0L, CommandType.CREDIT, 0L, 1L, 0L, 0L, 100L), commandOutcome);
        assertEquals(StatusCode.SUCCESS, commandOutcome.status());

        final long[] from = {1L, 1L};
        final long[] to = {2L, 3L};
        final long[] amount = {30L, 20L};
        final long[] asset = {0L, 0L};
        final boolean[] linked = {false, false};

        assertFalse(source.processBatch(
                new TransferBatchFixtures().encode(1L, 1L, 0L, 1L, from, to, amount, asset, linked), batchOutcome));
        assertEquals(50L, source.balances().rawGet(0L, 1L));

        final SnapshotManager writeManager = new SnapshotManager();
        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(writeManager, source, 1L);

        final BalanceEngine restored = new BalanceEngine(config, new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);
        assertTrue(readManager.verifyInvariant());

        // Resend the same batch: duplicate, cached result, no re-apply.
        final BatchOutcome restoredOutcome = new BatchOutcome(8);
        assertTrue(restored.processBatch(
                new TransferBatchFixtures().encode(1L, 1L, 0L, 1L, from, to, amount, asset, linked), restoredOutcome));
        assertEquals(StatusCode.SUCCESS, restoredOutcome.legStatus(0));
        assertEquals(50L, restored.balances().rawGet(0L, 1L));
        assertEquals(30L, restored.balances().rawGet(0L, 2L));
        assertEquals(20L, restored.balances().rawGet(0L, 3L));

        // Re-serialising the restored engine produces identical bytes.
        final InMemorySnapshot reSerialized = new InMemorySnapshot();
        reSerialized.writeFrom(new SnapshotManager(), restored, 1L);
        assertArrayEquals(snapshot.toByteArray(), reSerialized.toByteArray());
    }
}
