package com.adbe.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import com.adbe.telemetry.CoreMetrics;
import com.adbe.testkit.CommandFixtures;
import org.junit.jupiter.api.Test;

class DedupIdempotencyTest {

    private static CoreConfig smallConfig() {
        return CoreConfig.of(1024, 64, 8, 64, 16);
    }

    @Test
    void duplicateCommandIsAppliedExactlyOnce() {
        final CoreMetrics metrics = new CoreMetrics();
        final BalanceEngine engine = new BalanceEngine(smallConfig(), metrics);
        final CommandFixtures fixtures = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();

        final boolean firstDuplicate =
                engine.process(fixtures.encode(1L, 0L, 0L, 42L, CommandType.CREDIT, 100L, 0L, 0L, 50L), outcome);
        assertFalse(firstDuplicate);
        assertEquals(StatusCode.SUCCESS, outcome.status());
        assertEquals(50L, outcome.resultBalance());

        // Same (clientId, clientSeq) resubmitted: must return the cached result, not re-apply.
        final boolean secondDuplicate =
                engine.process(fixtures.encode(1L, 0L, 0L, 42L, CommandType.CREDIT, 100L, 0L, 0L, 50L), outcome);
        assertTrue(secondDuplicate);
        assertEquals(50L, outcome.resultBalance());
        assertEquals(42L, outcome.commandIdLo());

        assertEquals(50L, engine.balances().rawGet(100L));
        assertEquals(1L, metrics.duplicatesDetected());
        assertEquals(1L, metrics.commandsProcessed());
    }

    @Test
    void distinctSequencesAreAllApplied() {
        final BalanceEngine engine = new BalanceEngine(smallConfig(), new CoreMetrics());
        final CommandFixtures fixtures = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();

        for (long seq = 0; seq < 5; seq++) {
            engine.process(fixtures.encode(7L, seq, 0L, seq, CommandType.CREDIT, 200L, 0L, 0L, 10L), outcome);
        }
        assertEquals(50L, engine.balances().rawGet(200L));
    }
}
