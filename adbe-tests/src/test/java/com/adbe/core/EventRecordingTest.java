package com.adbe.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.adbe.config.CoreConfig;
import com.adbe.core.CommandOutcome.EventKind;
import com.adbe.core.CommandOutcome.EventRecord;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.EventCause;
import com.adbe.protocol.StatusCode;
import com.adbe.telemetry.CoreMetrics;
import com.adbe.testkit.CommandFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the semantic domain events each command records into the reused
 * {@link CommandOutcome} (ADR 0011). This is the deterministic core of the
 * journal: encoding and durable recording are exercised elsewhere.
 */
class EventRecordingTest {

    private static final long ASSET = 3L;

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

    private boolean run(final CommandType type, final long a, final long b, final long amount) {
        final boolean duplicate =
                engine.process(fixtures.encode(1L, seq, 0L, seq, type, ASSET, a, b, 0L, amount), outcome);
        seq++;
        return duplicate;
    }

    @Test
    void creditRecordsOneBalanceChange() {
        run(CommandType.CREDIT, 1L, 0L, 500L);
        assertEquals(1, outcome.eventCount());
        final EventRecord e = outcome.event(0);
        assertEquals(EventKind.BALANCE_CHANGED, e.kind());
        assertEquals(EventCause.CREDIT, e.cause());
        assertEquals(ASSET, e.assetId());
        assertEquals(1L, e.accountA());
        assertEquals(500L, e.valueA());
        assertEquals(500L, e.valueB());
    }

    @Test
    void debitRecordsNegativeDelta() {
        run(CommandType.CREDIT, 1L, 0L, 500L);
        run(CommandType.DEBIT, 1L, 0L, 200L);
        assertEquals(1, outcome.eventCount());
        final EventRecord e = outcome.event(0);
        assertEquals(EventKind.BALANCE_CHANGED, e.kind());
        assertEquals(EventCause.DEBIT, e.cause());
        assertEquals(300L, e.valueA());
        assertEquals(-200L, e.valueB());
    }

    @Test
    void transferRecordsThreeEventsWhoseDeltasNetToZero() {
        run(CommandType.CREDIT, 1L, 0L, 500L);
        run(CommandType.TRANSFER, 1L, 2L, 150L);

        assertEquals(3, outcome.eventCount());

        final EventRecord debit = outcome.event(0);
        assertEquals(EventKind.BALANCE_CHANGED, debit.kind());
        assertEquals(EventCause.TRANSFER_DEBIT, debit.cause());
        assertEquals(1L, debit.accountA());
        assertEquals(350L, debit.valueA());
        assertEquals(-150L, debit.valueB());

        final EventRecord credit = outcome.event(1);
        assertEquals(EventKind.BALANCE_CHANGED, credit.kind());
        assertEquals(EventCause.TRANSFER_CREDIT, credit.cause());
        assertEquals(2L, credit.accountA());
        assertEquals(150L, credit.valueA());
        assertEquals(150L, credit.valueB());

        final EventRecord edge = outcome.event(2);
        assertEquals(EventKind.TRANSFER, edge.kind());
        assertEquals(1L, edge.accountA());
        assertEquals(2L, edge.accountB());
        assertEquals(150L, edge.valueA());
        assertNull(edge.cause());

        // Supply conservation at the event layer: the paired deltas cancel.
        assertEquals(0L, debit.valueB() + credit.valueB());
    }

    @Test
    void approveRecordsAllowanceChange() {
        run(CommandType.APPROVE, 1L, 9L, 200L);
        assertEquals(1, outcome.eventCount());
        final EventRecord e = outcome.event(0);
        assertEquals(EventKind.ALLOWANCE_CHANGED, e.kind());
        assertEquals(1L, e.accountA());
        assertEquals(9L, e.accountB());
        assertEquals(200L, e.valueA());
    }

    @Test
    void reserveAndReleaseRecordHoldEvents() {
        run(CommandType.CREDIT, 1L, 0L, 500L);

        run(CommandType.RESERVE, 1L, 0L, 200L);
        assertEquals(1, outcome.eventCount());
        assertEquals(EventKind.RESERVED, outcome.event(0).kind());
        assertEquals(300L, outcome.event(0).valueA());
        assertEquals(200L, outcome.event(0).valueB());

        run(CommandType.RELEASE, 1L, 0L, 50L);
        assertEquals(1, outcome.eventCount());
        assertEquals(EventKind.RELEASED, outcome.event(0).kind());
        assertEquals(350L, outcome.event(0).valueA());
        assertEquals(150L, outcome.event(0).valueB());
    }

    @Test
    void captureToOtherAccountRecordsCaptureCreditAndEdge() {
        run(CommandType.CREDIT, 1L, 0L, 500L);
        run(CommandType.RESERVE, 1L, 0L, 300L);
        run(CommandType.CAPTURE, 1L, 2L, 120L);

        assertEquals(3, outcome.eventCount());
        assertEquals(EventKind.CAPTURED, outcome.event(0).kind());
        assertEquals(1L, outcome.event(0).accountA());
        assertEquals(180L, outcome.event(0).valueB());

        assertEquals(EventKind.BALANCE_CHANGED, outcome.event(1).kind());
        assertEquals(EventCause.TRANSFER_CREDIT, outcome.event(1).cause());
        assertEquals(2L, outcome.event(1).accountA());
        assertEquals(120L, outcome.event(1).valueB());

        assertEquals(EventKind.TRANSFER, outcome.event(2).kind());
        assertEquals(1L, outcome.event(2).accountA());
        assertEquals(2L, outcome.event(2).accountB());
    }

    @Test
    void delegatedTransferRecordsFourEvents() {
        run(CommandType.CREDIT, 1L, 0L, 500L);
        run(CommandType.APPROVE, 1L, 9L, 300L);
        // delegate=9 spends owner=1's funds to account 2.
        engine.process(
                fixtures.encode(1L, seq, 0L, seq, CommandType.DELEGATED_TRANSFER, ASSET, 9L, 1L, 2L, 120L), outcome);
        seq++;

        assertEquals(4, outcome.eventCount());
        assertEquals(EventKind.BALANCE_CHANGED, outcome.event(0).kind());
        assertEquals(EventCause.DELEGATED_DEBIT, outcome.event(0).cause());
        assertEquals(EventKind.BALANCE_CHANGED, outcome.event(1).kind());
        assertEquals(EventCause.DELEGATED_CREDIT, outcome.event(1).cause());
        assertEquals(EventKind.TRANSFER, outcome.event(2).kind());
        assertEquals(EventKind.ALLOWANCE_CHANGED, outcome.event(3).kind());
        assertEquals(180L, outcome.event(3).valueA());
    }

    @Test
    void rejectedCommandRecordsNoEvents() {
        // Debit an account that does not exist: rejected, no state change, no events.
        run(CommandType.DEBIT, 42L, 0L, 10L);
        assertEquals(StatusCode.INVALID_ACCOUNT, outcome.status());
        assertEquals(0, outcome.eventCount());
    }

    @Test
    void duplicateCommandReportsNoEvents() {
        run(CommandType.CREDIT, 1L, 0L, 500L);
        assertEquals(1, outcome.eventCount());
        // Re-submit the same clientSeq: a dedup hit must report zero events so the
        // journaler never re-emits a fact that already happened.
        final boolean duplicate =
                engine.process(fixtures.encode(1L, 0L, 0L, 0L, CommandType.CREDIT, ASSET, 1L, 0L, 0L, 500L), outcome);
        assertEquals(true, duplicate);
        assertEquals(0, outcome.eventCount());
    }
}
