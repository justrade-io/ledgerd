package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import io.justrade.ledgerd.testkit.InMemorySnapshot;
import io.justrade.ledgerd.testkit.TransferBatchFixtures;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/**
 * Property test for transfer batches: a batch applied to two identically-seeded
 * engines must produce byte-identical state (determinism), regardless of the
 * leg amounts and linked-chain shape.
 */
class TransferBatchPropertyTest {

    @Property
    void batchIsDeterministic(
            @ForAll final long amt0,
            @ForAll final long amt1,
            @ForAll final long amt2,
            @ForAll final boolean link0,
            @ForAll final boolean link1) {

        final long[] amounts = {Math.floorMod(amt0, 40L), Math.floorMod(amt1, 40L), Math.floorMod(amt2, 40L)};
        final long[] from = {1L, 2L, 3L};
        final long[] to = {2L, 3L, 1L};
        final long[] asset = {0L, 0L, 0L};
        final boolean[] linked = {link0, link1, false}; // trailing leg never linked

        final BalanceEngine first = engine();
        final BalanceEngine second = engine();
        for (final long account : new long[] {1L, 2L, 3L}) {
            credit(first, account, 100L);
            credit(second, account, 100L);
        }

        apply(first, from, to, amounts, asset, linked);
        apply(second, from, to, amounts, asset, linked);

        assertArrayEquals(snapshot(first), snapshot(second));
    }

    private static BalanceEngine engine() {
        return new BalanceEngine(CoreConfig.of(1024, 64, 8, 64, 16), new CoreMetrics());
    }

    private static void credit(final BalanceEngine engine, final long account, final long amount) {
        final CommandOutcome outcome = new CommandOutcome();
        engine.process(
                new CommandFixtures().encode(1L, account, 0L, account, CommandType.CREDIT, 0L, account, 0L, 0L, amount),
                outcome);
    }

    private static void apply(
            final BalanceEngine engine,
            final long[] from,
            final long[] to,
            final long[] amount,
            final long[] asset,
            final boolean[] linked) {
        final BatchOutcome outcome = new BatchOutcome(8);
        engine.processBatch(
                new TransferBatchFixtures().encode(1L, 1000L, 0L, 1000L, from, to, amount, asset, linked), outcome);
    }

    private static byte[] snapshot(final BalanceEngine engine) {
        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), engine, 1L);
        return snapshot.toByteArray();
    }
}
