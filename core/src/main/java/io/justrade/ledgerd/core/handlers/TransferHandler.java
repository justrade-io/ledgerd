package io.justrade.ledgerd.core.handlers;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.util.Amounts;

/**
 * Atomically moves funds between two accounts. Total supply is unchanged. The
 * destination is created on first receipt. The source-account balance is
 * returned on success.
 */
public final class TransferHandler {

    private final BalanceStore balances;

    public TransferHandler(final BalanceStore balances) {
        this.balances = balances;
    }

    public void handle(
            final long assetId, final long fromId, final long toId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long fromBalance = balances.rawGet(assetId, fromId);
        if (fromBalance == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        if (fromBalance < amount) {
            out.status(StatusCode.INSUFFICIENT_BALANCE);
            return;
        }
        if (fromId == toId) {
            // Moving funds to the same account is a validated no-op.
            out.balance(fromBalance);
            out.status(StatusCode.SUCCESS);
            return;
        }
        final long toRaw = balances.rawGet(assetId, toId);
        final long toBase = toRaw == BalanceStore.MISSING ? 0L : toRaw;
        if (Amounts.addOverflows(toBase, amount)) {
            out.status(StatusCode.OVERFLOW);
            return;
        }
        balances.set(assetId, fromId, fromBalance - amount);
        balances.set(assetId, toId, toBase + amount);
        out.balance(fromBalance - amount);
        out.addBalanceChanged(assetId, fromId, fromBalance - amount, -amount, EventCause.TRANSFER_DEBIT);
        out.addBalanceChanged(assetId, toId, toBase + amount, amount, EventCause.TRANSFER_CREDIT);
        out.addTransfer(assetId, fromId, toId, amount);
        out.status(StatusCode.SUCCESS);
    }
}
