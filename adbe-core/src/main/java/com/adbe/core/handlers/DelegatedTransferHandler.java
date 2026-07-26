package com.adbe.core.handlers;

import com.adbe.collections.AllowanceStore;
import com.adbe.collections.BalanceStore;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.StatusCode;
import com.adbe.util.Amounts;

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
            final long delegateId, final long ownerId, final long toId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long allowance = allowances.get(ownerId, delegateId);
        if (allowance < amount) {
            out.status(StatusCode.INSUFFICIENT_ALLOWANCE);
            return;
        }
        final long ownerBalance = balances.rawGet(ownerId);
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
            allowances.set(ownerId, delegateId, remaining);
            out.balance(ownerBalance);
            out.allowance(remaining);
            out.status(StatusCode.SUCCESS);
            return;
        }
        final long toRaw = balances.rawGet(toId);
        final long toBase = toRaw == BalanceStore.MISSING ? 0L : toRaw;
        if (Amounts.addOverflows(toBase, amount)) {
            out.status(StatusCode.OVERFLOW);
            return;
        }
        balances.set(ownerId, ownerBalance - amount);
        balances.set(toId, toBase + amount);
        final long remainingAllowance = allowance - amount;
        allowances.set(ownerId, delegateId, remainingAllowance);
        out.balance(ownerBalance - amount);
        out.allowance(remainingAllowance);
        out.status(StatusCode.SUCCESS);
    }
}
