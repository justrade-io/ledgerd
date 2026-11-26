package io.justrade.ledgerd.write.client;

/**
 * Pure, Aeron-free retransmission decision for a single in-flight command.
 * Extracted from {@link WriteClient} so the retry budget and leader-change
 * resend can be unit-tested with a controlled clock, without standing up a
 * cluster client.
 */
final class RetryPolicy {

    /** What {@link WriteClient#poll()} should do with a pending command this cycle. */
    enum Action {
        WAIT,
        RETRY,
        EXPIRE
    }

    private final int maxRetries;
    private final boolean rawSingleShot;

    RetryPolicy(final int maxRetries) {
        this(maxRetries, false);
    }

    RetryPolicy(final int maxRetries, final boolean rawSingleShot) {
        this.maxRetries = maxRetries;
        this.rawSingleShot = rawSingleShot;
    }

    /**
     * @param forceResend true after a leader change, forcing an immediate resend
     */
    Action evaluate(
            final long now,
            final long deadlineNanos,
            final int retries,
            final boolean forceResend,
            final boolean inUse) {
        if (!inUse) {
            return Action.WAIT;
        }
        final boolean due = forceResend || (now - deadlineNanos) >= 0;
        if (!due) {
            return Action.WAIT;
        }
        // Raw single-shot never resends: a timeout or leader change surfaces as an
        // indeterminate outcome instead of being masked by an idempotent retry.
        if (rawSingleShot) {
            return Action.EXPIRE;
        }
        if (maxRetries > 0 && retries >= maxRetries) {
            return Action.EXPIRE;
        }
        return Action.RETRY;
    }
}
