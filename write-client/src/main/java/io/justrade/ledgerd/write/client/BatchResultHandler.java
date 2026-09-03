package io.justrade.ledgerd.write.client;

/**
 * Callback invoked when a {@code TransferBatchResult} arrives for a previously
 * submitted batch. Matched to the original request by batch id.
 *
 * <p>Invoked on the client's polling thread; implementations must not block.
 */
@FunctionalInterface
public interface BatchResultHandler {

    /** No-op handler used when the caller does not register one. */
    BatchResultHandler NOOP = (batchIdHi, batchIdLo, results) -> {};

    /**
     * @param batchIdHi high 64 bits of the batch id echoed by the core
     * @param batchIdLo low 64 bits of the batch id echoed by the core
     * @param results one result per leg, in request order
     */
    void onBatchResult(long batchIdHi, long batchIdLo, TransferLegResult[] results);

    /**
     * Invoked when a batch is abandoned because it exhausted
     * {@code ClientConfig.maxRetries()} without a result. The default
     * implementation does nothing.
     *
     * @param batchIdHi high 64 bits of the abandoned batch id
     * @param batchIdLo low 64 bits of the abandoned batch id
     */
    default void onBatchExpired(final long batchIdHi, final long batchIdLo) {}
}
