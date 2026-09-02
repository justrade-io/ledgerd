package io.justrade.ledgerd.core.handlers;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.util.Amounts;

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

    public void handle(final long assetId, final long accountId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long current = balances.rawGet(assetId, accountId);
        if (current == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        if (current < amount) {
            out.status(StatusCode.INSUFFICIENT_BALANCE);
            return;
        }
        final long updated = current - amount;
        balances.set(assetId, accountId, updated);
        balances.adjustTotalSupply(assetId, -amount);
        out.balance(updated);
        out.addBalanceChanged(assetId, accountId, updated, -amount, EventCause.DEBIT);
        out.status(StatusCode.SUCCESS);
    }
}
