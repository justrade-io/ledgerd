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

        assertEquals(50L, engine.balances().rawGet(0L, 100L));
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
        assertEquals(50L, engine.balances().rawGet(0L, 200L));
    }

    @Test
    void sequenceEvictedFromBoundedWindowIsCountedAndReapplied() {
        // window=16, so clientSeq 0 and 16 map to the same ring slot (seq & 15).
        final CoreMetrics metrics = new CoreMetrics();
        final BalanceEngine engine = new BalanceEngine(CoreConfig.of(1024, 64, 8, 64, 16), metrics);
        final CommandFixtures fixtures = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();

        // Fill all 16 slots (seq 0..15): no eviction, each slot was empty.
        for (long seq = 0; seq < 16; seq++) {
            engine.process(fixtures.encode(1L, seq, 0L, seq, CommandType.CREDIT, 100L, 0L, 0L, 1L), outcome);
        }
        assertEquals(0L, metrics.dedupEvicted());

        // seq 16 lands on slot 0, evicting seq 0's dedup record.
        engine.process(fixtures.encode(1L, 16L, 0L, 16L, CommandType.CREDIT, 100L, 0L, 0L, 1L), outcome);
        assertEquals(1L, metrics.dedupEvicted());
        assertEquals(17L, engine.balances().rawGet(0L, 100L));

        // A late retry of the evicted seq 0 is no longer deduplicated: it re-applies.
        final boolean duplicate =
                engine.process(fixtures.encode(1L, 0L, 0L, 0L, CommandType.CREDIT, 100L, 0L, 0L, 1L), outcome);
        assertFalse(duplicate, "evicted sequence must not be detected as duplicate");
        assertEquals(18L, engine.balances().rawGet(0L, 100L));
    }
}
