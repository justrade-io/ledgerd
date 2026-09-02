package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HoldsTest {

    private static final long ASSET = 7L;

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

    private StatusCode run(final CommandType type, final long a, final long b, final long amount) {
        engine.process(fixtures.encode(1L, seq, 0L, seq, type, ASSET, a, b, 0L, amount), outcome);
        seq++;
        return outcome.status();
    }

    @Test
    void reserveReleaseCaptureMoveFundsAndPreserveSupply() {
        assertEquals(StatusCode.SUCCESS, run(CommandType.CREDIT, 1L, 0L, 100L));

        // Reserve 40: available 60, reserved 40, supply unchanged.
        assertEquals(StatusCode.SUCCESS, run(CommandType.RESERVE, 1L, 0L, 40L));
        assertEquals(60L, engine.balances().rawGet(ASSET, 1L));
        assertEquals(40L, engine.balances().reserved(ASSET, 1L));
        assertEquals(40L, outcome.resultReserved());
        assertEquals(100L, engine.balances().totalSupply(ASSET));

        // Release 10: available 70, reserved 30.
        assertEquals(StatusCode.SUCCESS, run(CommandType.RELEASE, 1L, 0L, 10L));
        assertEquals(70L, engine.balances().rawGet(ASSET, 1L));
        assertEquals(30L, engine.balances().reserved(ASSET, 1L));

        // Capture 20 from account 1 to account 2: reserved 10, account 2 gets 20.
        assertEquals(StatusCode.SUCCESS, run(CommandType.CAPTURE, 1L, 2L, 20L));
        assertEquals(10L, engine.balances().reserved(ASSET, 1L));
        assertEquals(70L, engine.balances().rawGet(ASSET, 1L));
        assertEquals(20L, engine.balances().rawGet(ASSET, 2L));
        // Total supply is conserved across the whole lifecycle.
        assertEquals(100L, engine.balances().totalSupply(ASSET));
    }

    @Test
    void reserveRejectsWhenAvailableInsufficient() {
        run(CommandType.CREDIT, 1L, 0L, 30L);
        assertEquals(StatusCode.INSUFFICIENT_BALANCE, run(CommandType.RESERVE, 1L, 0L, 40L));
        assertEquals(30L, engine.balances().rawGet(ASSET, 1L));
        assertEquals(0L, engine.balances().reserved(ASSET, 1L));
    }

    @Test
    void releaseAndCaptureRejectWhenReservedInsufficient() {
        run(CommandType.CREDIT, 1L, 0L, 50L);
        run(CommandType.RESERVE, 1L, 0L, 20L);
        assertEquals(StatusCode.INSUFFICIENT_RESERVED, run(CommandType.RELEASE, 1L, 0L, 25L));
        assertEquals(StatusCode.INSUFFICIENT_RESERVED, run(CommandType.CAPTURE, 1L, 2L, 25L));
        assertEquals(20L, engine.balances().reserved(ASSET, 1L));
    }

    @Test
    void reserveOnMissingAccountIsInvalid() {
        assertEquals(StatusCode.INVALID_ACCOUNT, run(CommandType.RESERVE, 99L, 0L, 10L));
    }
}
