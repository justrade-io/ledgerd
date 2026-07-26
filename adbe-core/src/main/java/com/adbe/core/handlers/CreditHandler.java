package com.adbe.core.handlers;

import com.adbe.collections.BalanceStore;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.StatusCode;
import com.adbe.util.Amounts;

/**
 * Increases an account balance and the total supply. Creates the account on
 * first credit. Overflow is reported via {@link StatusCode#OVERFLOW}.
 */
public final class CreditHandler {

    private final BalanceStore balances;

    public CreditHandler(final BalanceStore balances) {
        this.balances = balances;
    }

    public void handle(final long accountId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long raw = balances.rawGet(accountId);
        final long base = raw == BalanceStore.MISSING ? 0L : raw;
        if (Amounts.addOverflows(base, amount)) {
            out.status(StatusCode.OVERFLOW);
            return;
        }
        final long updated = base + amount;
        balances.set(accountId, updated);
        balances.adjustTotalSupply(amount);
        out.balance(updated);
        out.status(StatusCode.SUCCESS);
    }
}
