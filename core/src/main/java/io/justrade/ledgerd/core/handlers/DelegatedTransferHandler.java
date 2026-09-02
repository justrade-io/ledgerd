package io.justrade.ledgerd.core.handlers;

import io.justrade.ledgerd.collections.AllowanceStore;
import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.util.Amounts;

/**
 * A delegate spends the owner's balance, sending funds to a third account and
 * consuming allowance. Distinguishes {@link StatusCode#INSUFFICIENT_ALLOWANCE}
 * from {@link StatusCode#INSUFFICIENT_BALANCE} so the Edge can report the cause.
 * Total supply is unchanged.
 */
public final class DelegatedTransferHandler {

    private final BalanceStore balances;
    private final AllowanceStore allowances;

    public DelegatedTransferHandler(final BalanceStore balances, final AllowanceStore allowances) {
        this.balances = balances;
        this.allowances = allowances;
    }

    public void handle(
            final long assetId,
            final long delegateId,
            final long ownerId,
            final long toId,
            final long amount,
            final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long allowance = allowances.get(assetId, ownerId, delegateId);
        if (allowance < amount) {
            out.status(StatusCode.INSUFFICIENT_ALLOWANCE);
            return;
        }
        final long ownerBalance = balances.rawGet(assetId, ownerId);
        if (ownerBalance == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        if (ownerBalance < amount) {
            out.status(StatusCode.INSUFFICIENT_BALANCE);
            return;
        }
        if (ownerId == toId) {
            // Funds remain with the owner; only allowance is consumed.
            final long remaining = allowance - amount;
            allowances.set(assetId, ownerId, delegateId, remaining);
            out.balance(ownerBalance);
            out.allowance(remaining);
            out.addAllowanceChanged(assetId, ownerId, delegateId, remaining);
            out.status(StatusCode.SUCCESS);
            return;
        }
        final long toRaw = balances.rawGet(assetId, toId);
        final long toBase = toRaw == BalanceStore.MISSING ? 0L : toRaw;
        if (Amounts.addOverflows(toBase, amount)) {
            out.status(StatusCode.OVERFLOW);
            return;
        }
        balances.set(assetId, ownerId, ownerBalance - amount);
        balances.set(assetId, toId, toBase + amount);
        final long remainingAllowance = allowance - amount;
        allowances.set(assetId, ownerId, delegateId, remainingAllowance);
        out.balance(ownerBalance - amount);
        out.allowance(remainingAllowance);
        out.addBalanceChanged(assetId, ownerId, ownerBalance - amount, -amount, EventCause.DELEGATED_DEBIT);
        out.addBalanceChanged(assetId, toId, toBase + amount, amount, EventCause.DELEGATED_CREDIT);
        out.addTransfer(assetId, ownerId, toId, amount);
        out.addAllowanceChanged(assetId, ownerId, delegateId, remainingAllowance);
        out.status(StatusCode.SUCCESS);
    }
}
