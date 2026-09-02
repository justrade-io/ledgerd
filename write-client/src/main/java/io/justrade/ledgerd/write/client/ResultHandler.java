package io.justrade.ledgerd.write.client;

import io.justrade.ledgerd.protocol.StatusCode;

/**
 * Callback invoked when a {@code CommandResult} arrives for a previously
 * submitted command. Matched to the original request by command id.
 *
 * <p>Invoked on the client's polling thread; implementations must not block.
 */
@FunctionalInterface
public interface ResultHandler {

    /**
     * @param commandIdHi high 64 bits of the command id echoed by the core
     * @param commandIdLo low 64 bits of the command id echoed by the core
     * @param status deterministic result status
     * @param resultBalance resulting balance, valid only when {@code hasBalance}
     * @param hasBalance whether {@code resultBalance} is present
     * @param resultAllowance resulting allowance, valid only when {@code hasAllowance}
     * @param hasAllowance whether {@code resultAllowance} is present
     */
    void onResult(
            long commandIdHi,
            long commandIdLo,
            StatusCode status,
            long resultBalance,
            boolean hasBalance,
            long resultAllowance,
            boolean hasAllowance);

    /**
     * Invoked when a command is abandoned because it exhausted
     * {@code ClientConfig.maxRetries()} without a result. Guarantees the caller
     * is notified of every submitted command (never a silent drop). The default
     * implementation does nothing.
     *
     * @param commandIdHi high 64 bits of the abandoned command id
     * @param commandIdLo low 64 bits of the abandoned command id
     */
    default void onExpired(final long commandIdHi, final long commandIdLo) {}
}
