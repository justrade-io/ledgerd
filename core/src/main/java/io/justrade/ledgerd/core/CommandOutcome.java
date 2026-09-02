package io.justrade.ledgerd.core;

import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;

/**
 * Mutable, reusable holder for the outcome of a single command.
 *
 * <p>A single instance is owned by {@link BalanceService} and reset before each
 * command dispatch, so handlers never allocate a result object on the hot path.
 * It carries the command identity so the same value can be cached for dedup and
 * re-sent verbatim on a duplicate submission.
 *
 * <p>It also accumulates the semantic domain events a command produced (ADR
 * 0011) into a fixed-capacity, preallocated buffer, so {@code BalanceService}
 * can encode the event journal without any per-command allocation. The buffer
 * is sized for the worst case ({@code DELEGATED_TRANSFER}: two balance changes,
 * one transfer edge, one allowance change).
 */
public final class CommandOutcome {

    /** Worst-case number of events a single command emits (DELEGATED_TRANSFER). */
    public static final int MAX_EVENTS = 4;

    /** The kind of a recorded domain event; determines how its fields are read. */
    public enum EventKind {
        BALANCE_CHANGED,
        RESERVED,
        CAPTURED,
        RELEASED,
        TRANSFER,
        ALLOWANCE_CHANGED
    }

    /**
     * One recorded domain event, reused across commands. Field meaning depends on
     * {@link #kind()}:
     *
     * <ul>
     *   <li>{@code BALANCE_CHANGED}: {@code accountA}=account, {@code valueA}=new
     *       balance, {@code valueB}=signed delta, {@code cause} set.
     *   <li>{@code RESERVED/CAPTURED/RELEASED}: {@code accountA}=account,
     *       {@code valueA}=new available, {@code valueB}=new reserved.
     *   <li>{@code TRANSFER}: {@code accountA}=from, {@code accountB}=to,
     *       {@code valueA}=amount.
     *   <li>{@code ALLOWANCE_CHANGED}: {@code accountA}=owner,
     *       {@code accountB}=delegate, {@code valueA}=new allowance.
     * </ul>
     */
    public static final class EventRecord {
        private EventKind kind;
        private long assetId;
        private long accountA;
        private long accountB;
        private long valueA;
        private long valueB;
        private EventCause cause;

        public EventKind kind() {
            return kind;
        }

        public long assetId() {
            return assetId;
        }

        public long accountA() {
            return accountA;
        }

        public long accountB() {
            return accountB;
        }

        public long valueA() {
            return valueA;
        }

        public long valueB() {
            return valueB;
        }

        public EventCause cause() {
            return cause;
        }
    }

    private long commandIdHi;
    private long commandIdLo;
    private StatusCode status = StatusCode.SUCCESS;
    private long resultBalance;
    private boolean hasBalance;
    private long resultAllowance;
    private boolean hasAllowance;
    private long resultReserved;
    private boolean hasReserved;

    private final EventRecord[] events = new EventRecord[MAX_EVENTS];
    private int eventCount;

    public CommandOutcome() {
        for (int i = 0; i < events.length; i++) {
            events[i] = new EventRecord();
        }
    }

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
        this.eventCount = 0;
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
        this.eventCount = 0;
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

    /** Number of domain events recorded for the command just processed. */
    public int eventCount() {
        return eventCount;
    }

    /** The recorded event at {@code index}; valid only for {@code index < eventCount()}. */
    public EventRecord event(final int index) {
        return events[index];
    }

    /** Records a balance change (credit, debit, or one side of a transfer). */
    public void addBalanceChanged(
            final long assetId, final long account, final long newBalance, final long delta, final EventCause cause) {
        final EventRecord e = events[eventCount++];
        e.kind = EventKind.BALANCE_CHANGED;
        e.assetId = assetId;
        e.accountA = account;
        e.valueA = newBalance;
        e.valueB = delta;
        e.cause = cause;
    }

    /** Records a hold event ({@code RESERVE}, {@code CAPTURE}, or {@code RELEASE}). */
    public void addHold(
            final EventKind kind,
            final long assetId,
            final long account,
            final long newAvailable,
            final long newReserved) {
        final EventRecord e = events[eventCount++];
        e.kind = kind;
        e.assetId = assetId;
        e.accountA = account;
        e.valueA = newAvailable;
        e.valueB = newReserved;
        e.cause = null;
    }

    /** Records a transfer graph edge (balances carried by paired balance-change events). */
    public void addTransfer(final long assetId, final long from, final long to, final long amount) {
        final EventRecord e = events[eventCount++];
        e.kind = EventKind.TRANSFER;
        e.assetId = assetId;
        e.accountA = from;
        e.accountB = to;
        e.valueA = amount;
        e.cause = null;
    }

    /** Records an allowance change (approve, increase, decrease, or delegated spend). */
    public void addAllowanceChanged(
            final long assetId, final long owner, final long delegate, final long newAllowance) {
        final EventRecord e = events[eventCount++];
        e.kind = EventKind.ALLOWANCE_CHANGED;
        e.assetId = assetId;
        e.accountA = owner;
        e.accountB = delegate;
        e.valueA = newAllowance;
        e.cause = null;
    }
}
