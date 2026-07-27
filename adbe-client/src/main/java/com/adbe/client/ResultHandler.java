package com.adbe.client;

import com.adbe.protocol.StatusCode;

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
}
