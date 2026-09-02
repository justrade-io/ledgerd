package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiAssetTest {

    private BalanceEngine engine;
    private CommandFixtures fixtures;
    private CommandOutcome outcome;
    private long seq;

    @BeforeEach
    void setUp() {
        engine = new BalanceEngine(CoreConfig.of(1024, 64, 8, 64, 16), new CoreMetrics());
        fixtures = new CommandFixtures();
        outcome = new CommandOutcome();
        seq = 0L;
    }

    private StatusCode run(
            final CommandType type, final long asset, final long a, final long b, final long c, final long amount) {
        engine.process(fixtures.encode(1L, seq, 0L, seq, type, asset, a, b, c, amount), outcome);
        seq++;
        return outcome.status();
    }

    @Test
    void balancesAndSupplyAreIsolatedPerAsset() {
        assertEquals(StatusCode.SUCCESS, run(CommandType.CREDIT, 1L, 1L, 0L, 0L, 100L));
        assertEquals(StatusCode.SUCCESS, run(CommandType.CREDIT, 2L, 1L, 0L, 0L, 50L));

        assertEquals(100L, engine.balances().rawGet(1L, 1L));
        assertEquals(50L, engine.balances().rawGet(2L, 1L));
        assertEquals(BalanceStore.MISSING, engine.balances().rawGet(0L, 1L));

        assertEquals(100L, engine.balances().totalSupply(1L));
        assertEquals(50L, engine.balances().totalSupply(2L));
        assertEquals(0L, engine.balances().totalSupply(0L));
    }

    @Test
    void transferOnOneAssetDoesNotTouchAnother() {
        run(CommandType.CREDIT, 1L, 1L, 0L, 0L, 100L);
        run(CommandType.CREDIT, 2L, 1L, 0L, 0L, 100L);

        assertEquals(StatusCode.SUCCESS, run(CommandType.TRANSFER, 1L, 1L, 2L, 0L, 40L));
        assertEquals(60L, engine.balances().rawGet(1L, 1L));
        assertEquals(40L, engine.balances().rawGet(1L, 2L));
        // Asset 2 is untouched.
        assertEquals(100L, engine.balances().rawGet(2L, 1L));
        assertEquals(BalanceStore.MISSING, engine.balances().rawGet(2L, 2L));
    }

    @Test
    void allowanceOnOneAssetDoesNotAuthorizeAnother() {
        run(CommandType.CREDIT, 1L, 1L, 0L, 0L, 100L);
        run(CommandType.CREDIT, 2L, 1L, 0L, 0L, 100L);
        // Approve delegate 9 to spend owner 1 only on asset 1.
        run(CommandType.APPROVE, 1L, 1L, 9L, 0L, 50L);

        assertEquals(50L, engine.allowances().get(1L, 1L, 9L));
        assertEquals(0L, engine.allowances().get(2L, 1L, 9L));

        // Delegated transfer on asset 2 has no allowance and must be refused.
        assertEquals(StatusCode.INSUFFICIENT_ALLOWANCE, run(CommandType.DELEGATED_TRANSFER, 2L, 9L, 1L, 2L, 10L));
        // The same delegate on asset 1 succeeds.
        assertEquals(StatusCode.SUCCESS, run(CommandType.DELEGATED_TRANSFER, 1L, 9L, 1L, 2L, 10L));
        assertEquals(10L, engine.balances().rawGet(1L, 2L));
    }
}
