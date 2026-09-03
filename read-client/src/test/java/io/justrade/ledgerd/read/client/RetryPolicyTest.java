package io.justrade.ledgerd.read.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.read.client.RetryPolicy.Action;
import org.junit.jupiter.api.Test;

/**
 * Deterministic tests for the read-client retransmission decision. Time is a
 * plain {@code now} argument, so no media driver or wall clock is involved.
 */
final class RetryPolicyTest {

    private static final long TIMEOUT_NS = 1_000L;

    @Test
    void waitsWhenSlotIsNotReady() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 5);
        // Past its deadline, but mid-delivery (ready == false): must not be touched.
        assertEquals(Action.WAIT, policy.evaluate(500L, 100L, 0L, 0, false));
    }

    @Test
    void waitsBeforeDeadline() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 5);
        assertEquals(Action.WAIT, policy.evaluate(99L, 100L, 0L, 0, true));
    }

    @Test
    void retriesExactlyAtDeadline() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 5);
        assertEquals(Action.RETRY, policy.evaluate(100L, 100L, 0L, 0, true));
    }

    @Test
    void retriesWhenDueWithinBudget() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 5);
        assertEquals(Action.RETRY, policy.evaluate(200L, 100L, 0L, 1, true));
    }

    @Test
    void expiresOnOverallTimeoutBeforeCountingRetries() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 5);
        // now - submitted > timeout wins even with retries still available.
        assertEquals(Action.EXPIRE, policy.evaluate(1_500L, 100L, 0L, 0, true));
    }

    @Test
    void expiresWhenRetriesExhausted() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 3);
        assertEquals(Action.EXPIRE, policy.evaluate(200L, 100L, 0L, 3, true));
    }

    @Test
    void unboundedRetriesNeverExpireOnCount() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 0);
        assertEquals(Action.RETRY, policy.evaluate(200L, 100L, 0L, 999, true));
    }

    @Test
    void unboundedRetriesStillExpireOnTimeout() {
        final RetryPolicy policy = new RetryPolicy(TIMEOUT_NS, 0);
        assertEquals(Action.EXPIRE, policy.evaluate(2_000L, 100L, 0L, 999, true));
    }
}
