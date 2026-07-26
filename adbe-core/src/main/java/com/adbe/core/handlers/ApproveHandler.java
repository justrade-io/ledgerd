package com.adbe.core.handlers;

import com.adbe.collections.AllowanceStore;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.StatusCode;
import com.adbe.util.Amounts;

/**
 * Allowance management: overwrite (approve), relative increase, and relative
 * decrease. The relative operations avoid the read-modify-write race that a
 * bare overwrite would introduce when multiple approvals arrive close together.
 */
public final class ApproveHandler {

    private final AllowanceStore allowances;

    public ApproveHandler(final AllowanceStore allowances) {
        this.allowances = allowances;
    }

    /** Overwrite semantics: sets the allowance to an absolute limit. */
    public void approve(final long ownerId, final long delegateId, final long limit, final CommandOutcome out) {
        if (Amounts.isNegative(limit)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        allowances.set(ownerId, delegateId, limit);
        out.allowance(limit);
        out.status(StatusCode.SUCCESS);
    }

    /** Relative increase; overflow-checked. */
    public void increase(final long ownerId, final long delegateId, final long delta, final CommandOutcome out) {
        if (Amounts.isNegative(delta)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long current = allowances.get(ownerId, delegateId);
        if (Amounts.addOverflows(current, delta)) {
            out.status(StatusCode.OVERFLOW);
            return;
        }
        final long updated = current + delta;
        allowances.set(ownerId, delegateId, updated);
        out.allowance(updated);
        out.status(StatusCode.SUCCESS);
    }

    /** Relative decrease; fails if it would go below zero. */
    public void decrease(final long ownerId, final long delegateId, final long delta, final CommandOutcome out) {
        if (Amounts.isNegative(delta)) {
            out.status(StatusCode.INVALID_AMOUNT);
            return;
        }
        final long current = allowances.get(ownerId, delegateId);
        if (current < delta) {
            out.status(StatusCode.INSUFFICIENT_ALLOWANCE);
            return;
        }
        final long updated = current - delta;
        allowances.set(ownerId, delegateId, updated);
        out.allowance(updated);
        out.status(StatusCode.SUCCESS);
    }
}
