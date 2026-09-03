package io.justrade.ledgerd.read.client;

/**
 * Pure, Aeron-free retransmission decision for a single in-flight query.
 * Extracted from {@link ReadClient} so the retry and overall-timeout budget can
 * be unit-tested with a controlled clock, without standing up a media driver.
 */
final class RetryPolicy {

    /** What {@link ReadClient#poll()} should do with a pending query this cycle. */
    enum Action {
        WAIT,
        RETRY,
        EXPIRE
    }

    private final long messageTimeoutNs;
    private final int maxRetries;

    RetryPolicy(final long messageTimeoutNs, final int maxRetries) {
        this.messageTimeoutNs = messageTimeoutNs;
        this.maxRetries = maxRetries;
    }

    /**
     * @param ready whether the slot may be retransmitted now (in use, not mid-delivery)
     */
    Action evaluate(
            final long now,
            final long deadlineNanos,
            final long submittedNanos,
            final int retries,
            final boolean ready) {
        if (!ready || (now - deadlineNanos) < 0) {
            return Action.WAIT;
        }
        // Overall budget bounds every query even under unbounded per-message retries.
        if (now - submittedNanos > messageTimeoutNs) {
            return Action.EXPIRE;
        }
        if (maxRetries > 0 && retries >= maxRetries) {
            return Action.EXPIRE;
        }
        return Action.RETRY;
    }
}
