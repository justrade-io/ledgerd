package com.adbe.core;

import com.adbe.protocol.StatusCode;

/**
 * Mutable, reusable holder for the outcome of a single command.
 *
 * <p>A single instance is owned by {@link BalanceService} and reset before each
 * command dispatch, so handlers never allocate a result object on the hot path.
 * It carries the command identity so the same value can be cached for dedup and
 * re-sent verbatim on a duplicate submission.
 */
public final class CommandOutcome {

    private long commandIdHi;
    private long commandIdLo;
    private StatusCode status = StatusCode.SUCCESS;
    private long resultBalance;
    private boolean hasBalance;
    private long resultAllowance;
    private boolean hasAllowance;
    private long resultReserved;
    private boolean hasReserved;

    /** Clears all fields and records the command identity for the next dispatch. */
    public void reset(final long idHi, final long idLo) {
        this.commandIdHi = idHi;
        this.commandIdLo = idLo;
        this.status = StatusCode.SUCCESS;
        this.resultBalance = 0L;
        this.hasBalance = false;
        this.resultAllowance = 0L;
        this.hasAllowance = false;
        this.resultReserved = 0L;
        this.hasReserved = false;
    }

    public void status(final StatusCode newStatus) {
        this.status = newStatus;
    }

    public void balance(final long balance) {
        this.resultBalance = balance;
        this.hasBalance = true;
    }

    public void allowance(final long allowance) {
        this.resultAllowance = allowance;
        this.hasAllowance = true;
    }

    public void reserved(final long reserved) {
        this.resultReserved = reserved;
        this.hasReserved = true;
    }

    /** Copies identity and result fields from a cached dedup record. */
    public void set(
            final long idHi,
            final long idLo,
            final StatusCode cachedStatus,
            final long balance,
            final boolean balancePresent,
            final long allowance,
            final boolean allowancePresent,
            final long reserved,
            final boolean reservedPresent) {
        this.commandIdHi = idHi;
        this.commandIdLo = idLo;
        this.status = cachedStatus;
        this.resultBalance = balance;
        this.hasBalance = balancePresent;
        this.resultAllowance = allowance;
        this.hasAllowance = allowancePresent;
        this.resultReserved = reserved;
        this.hasReserved = reservedPresent;
    }

    public long commandIdHi() {
        return commandIdHi;
    }

    public long commandIdLo() {
        return commandIdLo;
    }

    public StatusCode status() {
        return status;
    }

    public long resultBalance() {
        return resultBalance;
    }

    public boolean hasBalance() {
        return hasBalance;
    }

    public long resultAllowance() {
        return resultAllowance;
    }

    public boolean hasAllowance() {
        return hasAllowance;
    }

    public long resultReserved() {
        return resultReserved;
    }

    public boolean hasReserved() {
        return hasReserved;
    }
}
