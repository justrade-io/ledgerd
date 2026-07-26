package com.adbe.core.handlers;

import com.adbe.collections.BalanceStore;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.StatusCode;
import com.adbe.util.Amounts;

/**
 * Decreases an account balance and the total supply. Fails with
 * {@link StatusCode#INSUFFICIENT_BALANCE} when funds are inadequate and
 * {@link StatusCode#INVALID_ACCOUNT} when the account does not exist.
 */
public final class DebitHandler {

    private final BalanceStore balances;

    public DebitHandler(final BalanceStore balances) {
        this.balances = balances;
    }

    public void handle(final long accountId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long current = balances.rawGet(accountId);
        if (current == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        if (current < amount) {
            out.status(StatusCode.INSUFFICIENT_BALANCE);
            return;
        }
        final long updated = current - amount;
        balances.set(accountId, updated);
        balances.adjustTotalSupply(-amount);
        out.balance(updated);
        out.status(StatusCode.SUCCESS);
    }
}
