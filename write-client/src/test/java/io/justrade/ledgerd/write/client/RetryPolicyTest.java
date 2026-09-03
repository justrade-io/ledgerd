package io.justrade.ledgerd.write.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.write.client.RetryPolicy.Action;
import org.junit.jupiter.api.Test;

/**
 * Deterministic tests for the write-client retransmission decision. Time is a
 * plain {@code now} argument, so no cluster client or wall clock is involved.
 */
final class RetryPolicyTest {

    @Test
    void waitsWhenSlotIsFree() {
        final RetryPolicy policy = new RetryPolicy(5);
        assertEquals(Action.WAIT, policy.evaluate(500L, 100L, 0, false, false));
    }

    @Test
    void waitsBeforeDeadlineWithoutForcedResend() {
        final RetryPolicy policy = new RetryPolicy(5);
        assertEquals(Action.WAIT, policy.evaluate(99L, 100L, 0, false, true));
    }

    @Test
    void retriesExactlyAtDeadline() {
        final RetryPolicy policy = new RetryPolicy(5);
        assertEquals(Action.RETRY, policy.evaluate(100L, 100L, 0, false, true));
    }

    @Test
    void forcedResendRetriesBeforeDeadline() {
        final RetryPolicy policy = new RetryPolicy(5);
        assertEquals(Action.RETRY, policy.evaluate(50L, 100L, 0, true, true));
    }

    @Test
    void expiresWhenRetriesExhausted() {
        final RetryPolicy policy = new RetryPolicy(3);
        assertEquals(Action.EXPIRE, policy.evaluate(200L, 100L, 3, false, true));
    }

    @Test
    void forcedResendStillExpiresWhenRetriesExhausted() {
        final RetryPolicy policy = new RetryPolicy(3);
        assertEquals(Action.EXPIRE, policy.evaluate(50L, 100L, 3, true, true));
    }

    @Test
    void unboundedRetriesNeverExpire() {
        final RetryPolicy policy = new RetryPolicy(0);
        assertEquals(Action.RETRY, policy.evaluate(200L, 100L, 999, false, true));
    }
}
