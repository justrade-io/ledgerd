package com.adbe.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.adbe.config.CoreConfig;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import com.adbe.telemetry.CoreMetrics;
import com.adbe.testkit.CommandFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandlerBehaviourTest {

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

    private StatusCode run(final CommandType type, final long a, final long b, final long c, final long amount) {
        engine.process(fixtures.encode(1L, seq, 0L, seq, type, a, b, c, amount), outcome);
        seq++;
        return outcome.status();
    }

    @Test
    void debitRequiresExistingAccountAndSufficientFunds() {
        assertEquals(StatusCode.INVALID_ACCOUNT, run(CommandType.DEBIT, 1L, 0L, 0L, 5L));
        assertEquals(StatusCode.SUCCESS, run(CommandType.CREDIT, 1L, 0L, 0L, 10L));
        assertEquals(StatusCode.INSUFFICIENT_BALANCE, run(CommandType.DEBIT, 1L, 0L, 0L, 20L));
        assertEquals(StatusCode.SUCCESS, run(CommandType.DEBIT, 1L, 0L, 0L, 4L));
        assertEquals(6L, engine.balances().rawGet(0L, 1L));
    }

    @Test
    void transferMovesFundsAndPreservesTotalSupply() {
        run(CommandType.CREDIT, 1L, 0L, 0L, 100L);
        assertEquals(StatusCode.SUCCESS, run(CommandType.TRANSFER, 1L, 2L, 0L, 40L));
        assertEquals(60L, engine.balances().rawGet(0L, 1L));
        assertEquals(40L, engine.balances().rawGet(0L, 2L));
        assertEquals(100L, engine.balances().totalSupply(0L));
    }

    @Test
    void allowanceIncreaseDecreaseAndDelegatedTransfer() {
        run(CommandType.CREDIT, 1L, 0L, 0L, 100L);
        assertEquals(StatusCode.SUCCESS, run(CommandType.APPROVE, 1L, 9L, 0L, 30L));
        assertEquals(StatusCode.SUCCESS, run(CommandType.INCREASE_ALLOWANCE, 1L, 9L, 0L, 20L));
        assertEquals(50L, engine.allowances().get(0L, 1L, 9L));
        assertEquals(StatusCode.SUCCESS, run(CommandType.DECREASE_ALLOWANCE, 1L, 9L, 0L, 10L));
        assertEquals(40L, engine.allowances().get(0L, 1L, 9L));

        // delegate=9 spends owner=1 -> to=2, amount 25
        assertEquals(StatusCode.SUCCESS, run(CommandType.DELEGATED_TRANSFER, 9L, 1L, 2L, 25L));
        assertEquals(75L, engine.balances().rawGet(0L, 1L));
        assertEquals(25L, engine.balances().rawGet(0L, 2L));
        assertEquals(15L, engine.allowances().get(0L, 1L, 9L));
    }

    @Test
    void delegatedTransferDistinguishesAllowanceFromBalance() {
        run(CommandType.CREDIT, 1L, 0L, 0L, 10L);
        run(CommandType.APPROVE, 1L, 9L, 0L, 5L);
        // Allowance (5) is the binding constraint.
        assertEquals(StatusCode.INSUFFICIENT_ALLOWANCE, run(CommandType.DELEGATED_TRANSFER, 9L, 1L, 2L, 8L));
        // Raise allowance above balance so balance becomes the binding constraint.
        run(CommandType.APPROVE, 1L, 9L, 0L, 100L);
        assertEquals(StatusCode.INSUFFICIENT_BALANCE, run(CommandType.DELEGATED_TRANSFER, 9L, 1L, 2L, 50L));
    }
}
