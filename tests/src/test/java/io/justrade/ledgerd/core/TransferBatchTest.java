package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import io.justrade.ledgerd.testkit.TransferBatchFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TransferBatchTest {

    private static final long ASSET = 0L;

    private BalanceEngine engine;
    private CommandFixtures commands;
    private TransferBatchFixtures batches;
    private CommandOutcome commandOutcome;
    private BatchOutcome batchOutcome;
    private long seq;

    @BeforeEach
    void setUp() {
        engine = new BalanceEngine(CoreConfig.of(1024, 64, 8, 64, 16).withBatch(8, 16), new CoreMetrics());
        commands = new CommandFixtures();
        batches = new TransferBatchFixtures();
        commandOutcome = new CommandOutcome();
        batchOutcome = new BatchOutcome(8);
        seq = 0L;
    }

    private void credit(final long account, final long amount) {
        final long s = seq++;
        engine.process(
                commands.encode(1L, s, 0L, s, CommandType.CREDIT, ASSET, account, 0L, 0L, amount), commandOutcome);
        assertEquals(StatusCode.SUCCESS, commandOutcome.status());
    }

    private void runBatch(
            final long[] from, final long[] to, final long[] amount, final long[] asset, final boolean[] linked) {
        final long s = seq++;
        engine.processBatch(batches.encode(1L, s, 0L, s, from, to, amount, asset, linked), batchOutcome);
    }

    private void runBatch(final long[] from, final long[] to, final long[] amount, final boolean[] linked) {
        final long[] asset = new long[from.length];
        runBatch(from, to, amount, asset, linked);
    }

    private long balance(final long account) {
        return engine.balances().rawGet(ASSET, account);
    }

    @Test
    void independentLegsAllApply() {
        credit(1L, 100L);
        runBatch(new long[] {1L, 1L}, new long[] {2L, 3L}, new long[] {30L, 20L}, new boolean[] {false, false});

        assertEquals(2, batchOutcome.legCount());
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(0));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(1));
        assertEquals(50L, balance(1L));
        assertEquals(30L, balance(2L));
        assertEquals(20L, balance(3L));
        assertEquals(100L, engine.balances().totalSupply(ASSET));
    }

    @Test
    void linkedChainAppliesSequentially() {
        credit(1L, 100L);
        // A -> B 60, then B -> C 40; the second leg spends B's intermediate balance.
        runBatch(new long[] {1L, 2L}, new long[] {2L, 3L}, new long[] {60L, 40L}, new boolean[] {true, false});

        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(0));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(1));
        assertEquals(40L, balance(1L));
        assertEquals(20L, balance(2L));
        assertEquals(40L, balance(3L));
    }

    @Test
    void linkedChainRollsBackOnFailure() {
        credit(1L, 100L);
        // A -> B 60 succeeds, then B -> C 80 fails; the whole chain rolls back.
        runBatch(new long[] {1L, 2L}, new long[] {2L, 3L}, new long[] {60L, 80L}, new boolean[] {true, false});

        assertEquals(StatusCode.INSUFFICIENT_BALANCE, batchOutcome.legStatus(0));
        assertEquals(StatusCode.INSUFFICIENT_BALANCE, batchOutcome.legStatus(1));
        // A restored, auto-created B removed, C never created.
        assertEquals(100L, balance(1L));
        assertEquals(BalanceStore.MISSING, balance(2L));
        assertEquals(BalanceStore.MISSING, balance(3L));
        assertEquals(100L, engine.balances().totalSupply(ASSET));
    }

    @Test
    void failedChainDoesNotAffectOtherChains() {
        credit(10L, 100L);
        credit(20L, 100L);
        // Chain 1 (leg 0) fails; chain 2 (leg 1) succeeds independently.
        runBatch(new long[] {10L, 20L}, new long[] {11L, 21L}, new long[] {200L, 50L}, new boolean[] {false, false});

        assertEquals(StatusCode.INSUFFICIENT_BALANCE, batchOutcome.legStatus(0));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(1));
        assertEquals(100L, balance(10L));
        assertEquals(50L, balance(20L));
        assertEquals(50L, balance(21L));
    }

    @Test
    void trailingLinkedFlagIsInvalidChain() {
        credit(1L, 100L);
        runBatch(new long[] {1L}, new long[] {2L}, new long[] {30L}, new boolean[] {true});

        assertEquals(StatusCode.INVALID_CHAIN, batchOutcome.legStatus(0));
        assertEquals(100L, balance(1L));
        assertEquals(BalanceStore.MISSING, balance(2L));
    }

    @Test
    void emptyBatchIsNoop() {
        runBatch(new long[0], new long[0], new long[0], new boolean[0]);
        assertEquals(0, batchOutcome.legCount());
        assertEquals(0L, engine.balances().totalSupply(ASSET));
    }

    @Test
    void batchExceedingMaxSizeIsRejected() {
        final int legs = 9; // engine maxBatchSize is 8
        final long[] from = new long[legs];
        final long[] to = new long[legs];
        final long[] amount = new long[legs];
        final long[] asset = new long[legs];
        final boolean[] linked = new boolean[legs];
        for (int i = 0; i < legs; i++) {
            from[i] = 1000L + i;
            to[i] = 2000L + i;
            amount[i] = 1L;
        }
        runBatch(from, to, amount, asset, linked);

        assertEquals(8, batchOutcome.legCount());
        for (int i = 0; i < 8; i++) {
            assertEquals(StatusCode.INVALID_CHAIN, batchOutcome.legStatus(i));
        }
    }

    @Test
    void duplicateBatchReplaysCachedResult() {
        credit(1L, 100L);
        final long[] from = {1L};
        final long[] to = {2L};
        final long[] amount = {30L};
        final long[] asset = {ASSET};
        final boolean[] linked = {false};

        final long s = seq++;
        assertFalse(engine.processBatch(batches.encode(1L, s, 0L, s, from, to, amount, asset, linked), batchOutcome));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(0));

        assertTrue(engine.processBatch(batches.encode(1L, s, 0L, s, from, to, amount, asset, linked), batchOutcome));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(0));
        // Not double-applied.
        assertEquals(70L, balance(1L));
        assertEquals(30L, balance(2L));
    }

    @Test
    void batchConservesSupply() {
        credit(1L, 100L);
        credit(2L, 50L);
        runBatch(new long[] {1L, 2L, 1L}, new long[] {2L, 3L, 4L}, new long[] {10L, 20L, 5L}, new boolean[] {
            true, true, false
        });

        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(0));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(1));
        assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(2));
        assertEquals(150L, engine.balances().totalSupply(ASSET));
    }
}
