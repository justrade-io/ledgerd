package io.justrade.ledgerd.write.client;

/**
 * Thrown by {@link WriteClient#submit} when the in-flight command window is full.
 *
 * <p>Signals backpressure explicitly to the caller rather than silently dropping
 * the command, per the reliable request/response contract. The caller should
 * {@link WriteClient#poll} to drain acknowledgements and retry.
 */
public final class BackpressureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BackpressureException(final String message) {
        super(message);
    }
}
