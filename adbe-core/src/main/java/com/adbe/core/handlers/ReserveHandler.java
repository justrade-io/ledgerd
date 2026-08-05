package com.adbe.core.handlers;

import com.adbe.collections.BalanceStore;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.StatusCode;
import com.adbe.util.Amounts;

/**
 * Two-phase spending over an account's balance, split into {@code available}
 * and {@code reserved} buckets. {@code reserve} earmarks funds, {@code release}
 * un-earmarks them, and {@code capture} settles held funds to a destination
 * account. Total supply is preserved by all three: funds only move between an
 * account's own buckets ({@code reserve}, {@code release}) or from one account's
 * reserved bucket to another's available bucket ({@code capture}).
 */
public final class ReserveHandler {

    private final BalanceStore balances;

    public ReserveHandler(final BalanceStore balances) {
        this.balances = balances;
    }

    /** Moves {@code amount} from available to reserved on one account. */
    public void reserve(final long assetId, final long accountId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long available = balances.rawGet(assetId, accountId);
        if (available == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        if (available < amount) {
            out.status(StatusCode.INSUFFICIENT_BALANCE);
            return;
        }
        final long newAvailable = available - amount;
        final long newReserved = balances.reserved(assetId, accountId) + amount;
        balances.set(assetId, accountId, newAvailable);
        balances.setReserved(assetId, accountId, newReserved);
        out.balance(newAvailable);
        out.reserved(newReserved);
        out.status(StatusCode.SUCCESS);
    }

    /** Moves {@code amount} from reserved back to available on one account. */
    public void release(final long assetId, final long accountId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long available = balances.rawGet(assetId, accountId);
        if (available == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        final long reserved = balances.reserved(assetId, accountId);
        if (reserved < amount) {
            out.status(StatusCode.INSUFFICIENT_RESERVED);
            return;
        }
        final long newAvailable = available + amount;
        final long newReserved = reserved - amount;
        balances.set(assetId, accountId, newAvailable);
        balances.setReserved(assetId, accountId, newReserved);
        out.balance(newAvailable);
        out.reserved(newReserved);
        out.status(StatusCode.SUCCESS);
    }

    /**
     * Settles {@code amount} of {@code fromId}'s reserved funds into {@code toId}'s
     * available balance. Total supply is unchanged.
     */
    public void capture(
            final long assetId, final long fromId, final long toId, final long amount, final CommandOutcome out) {
        if (Amounts.isNegative(amount)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long fromAvailable = balances.rawGet(assetId, fromId);
        if (fromAvailable == BalanceStore.MISSING) {
            out.status(StatusCode.INVALID_ACCOUNT);
            return;
        }
        final long fromReserved = balances.reserved(assetId, fromId);
        if (fromReserved < amount) {
            out.status(StatusCode.INSUFFICIENT_RESERVED);
            return;
        }
        if (fromId == toId) {
            // Settling to self returns the held funds to available.
            final long newReserved = fromReserved - amount;
            final long newAvailable = fromAvailable + amount;
            balances.set(assetId, fromId, newAvailable);
            balances.setReserved(assetId, fromId, newReserved);
            out.balance(newAvailable);
            out.reserved(newReserved);
            out.status(StatusCode.SUCCESS);
            return;
        }
        final long toRaw = balances.rawGet(assetId, toId);
        final long toBase = toRaw == BalanceStore.MISSING ? 0L : toRaw;
        if (Amounts.addOverflows(toBase, amount)) {
            out.status(StatusCode.OVERFLOW);
            return;
        }
        final long newReserved = fromReserved - amount;
        balances.setReserved(assetId, fromId, newReserved);
        balances.set(assetId, toId, toBase + amount);
        out.balance(fromAvailable);
        out.reserved(newReserved);
        out.status(StatusCode.SUCCESS);
    }
}
