package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.CommandFixtures;
import org.junit.jupiter.api.Test;

class OverflowTest {

    private static CoreConfig smallConfig() {
        return CoreConfig.of(1024, 64, 8, 64, 16);
    }

    @Test
    void creditNearMaxReturnsOverflowNotWrapAround() {
        final BalanceEngine engine = new BalanceEngine(smallConfig(), new CoreMetrics());
        final CommandFixtures fixtures = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();

        engine.process(fixtures.encode(1L, 0L, 0L, 1L, CommandType.CREDIT, 1L, 0L, 0L, Long.MAX_VALUE - 10L), outcome);
        assertEquals(StatusCode.SUCCESS, outcome.status());

        engine.process(fixtures.encode(1L, 1L, 0L, 2L, CommandType.CREDIT, 1L, 0L, 0L, 100L), outcome);
        assertEquals(StatusCode.OVERFLOW, outcome.status());

        // Balance must be unchanged by the rejected command.
        assertEquals(Long.MAX_VALUE - 10L, engine.balances().rawGet(0L, 1L));
    }

    @Test
    void negativeAmountIsRejected() {
        final BalanceEngine engine = new BalanceEngine(smallConfig(), new CoreMetrics());
        final CommandFixtures fixtures = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();

        engine.process(fixtures.encode(1L, 0L, 0L, 1L, CommandType.CREDIT, 1L, 0L, 0L, -5L), outcome);
        assertEquals(StatusCode.INVALID_AMOUNT, outcome.status());
    }
}
