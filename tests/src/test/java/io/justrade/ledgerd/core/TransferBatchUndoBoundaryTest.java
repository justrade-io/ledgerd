package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import io.justrade.ledgerd.testkit.TransferBatchFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the transfer-batch undo stack at its boundary. A single linked chain
 * of {@code maxBatchSize} legs, each moving funds between two distinct accounts,
 * pushes up to {@code maxBatchSize * 2} undo frames - the worst case the stack is
 * sized for. The tests drive both the commit path (all legs succeed) and the
 * full-rollback path (the last leg fails) to prove the stack neither overflows
 * nor leaves residue.
 */
class TransferBatchUndoBoundaryTest {

    private static final long ASSET = 0L;
    private static final int MAX_BATCH = 8;

    private BalanceEngine engine;
    private CommandFixtures commands;
    private TransferBatchFixtures batches;
    private CommandOutcome commandOutcome;
    private BatchOutcome batchOutcome;
    private long seq;

    @BeforeEach
    void setUp() {
        engine = new BalanceEngine(CoreConfig.of(1024, 64, 8, 64, 16).withBatch(MAX_BATCH, 16), new CoreMetrics());
        commands = new CommandFixtures();
        batches = new TransferBatchFixtures();
        commandOutcome = new CommandOutcome();
        batchOutcome = new BatchOutcome(MAX_BATCH);
        seq = 0L;
    }

    private void credit(final long account, final long amount) {
        final long s = seq++;
        engine.process(
                commands.encode(1L, s, 0L, s, CommandType.CREDIT, ASSET, account, 0L, 0L, amount), commandOutcome);
        assertEquals(StatusCode.SUCCESS, commandOutcome.status());
    }

    private long balance(final long account) {
        return engine.balances().rawGet(ASSET, account);
    }

    // Builds a single linked chain 1->2->...->(MAX_BATCH+1): legs[0..n-2] linked,
    // the final leg terminates the chain.
    private void runChain(final long lastLegAmount) {
        final long[] from = new long[MAX_BATCH];
        final long[] to = new long[MAX_BATCH];
        final long[] amount = new long[MAX_BATCH];
        final long[] asset = new long[MAX_BATCH];
        final boolean[] linked = new boolean[MAX_BATCH];
        for (int i = 0; i < MAX_BATCH; i++) {
            from[i] = i + 1;
            to[i] = i + 2;
            amount[i] = (i == MAX_BATCH - 1) ? lastLegAmount : 10L;
            asset[i] = ASSET;
            linked[i] = i < MAX_BATCH - 1;
        }
        final long s = seq++;
        engine.processBatch(batches.encode(1L, s, 0L, s, from, to, amount, asset, linked), batchOutcome);
    }

    @Test
    void fullChainAtMaxBatchSizeCommits() {
        credit(1L, 80L);
        runChain(10L);

        assertEquals(MAX_BATCH, batchOutcome.legCount());
        for (int i = 0; i < MAX_BATCH; i++) {
            assertEquals(StatusCode.SUCCESS, batchOutcome.legStatus(i));
        }
        assertEquals(70L, balance(1L));
        for (long account = 2; account < MAX_BATCH + 1; account++) {
            assertEquals(0L, balance(account));
        }
        assertEquals(10L, balance(MAX_BATCH + 1));
        assertEquals(80L, engine.balances().totalSupply(ASSET));
    }

    @Test
    void fullChainAtMaxBatchSizeRollsBackWhenLastLegFails() {
        credit(1L, 70L);
        // The last leg moves more than its source holds; the entire chain unwinds.
        runChain(1000L);

        assertEquals(MAX_BATCH, batchOutcome.legCount());
        for (int i = 0; i < MAX_BATCH; i++) {
            assertEquals(StatusCode.INSUFFICIENT_BALANCE, batchOutcome.legStatus(i));
        }
        assertEquals(70L, balance(1L));
        for (long account = 2; account < MAX_BATCH + 2; account++) {
            assertEquals(BalanceStore.MISSING, balance(account));
        }
        assertEquals(70L, engine.balances().totalSupply(ASSET));
    }
}
