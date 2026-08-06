package com.adbe.config;

import org.agrona.BitUtil;

/**
 * Immutable, preallocated capacity and tuning configuration for the core engine.
 *
 * <p>All capacities are validated to be powers of two so that hot-path index
 * arithmetic can use {@code & (capacity - 1)} instead of modulo.
 */
public final class CoreConfig {

    /** Default number of accounts preallocated in the balance map. */
    public static final int DEFAULT_ACCOUNT_CAPACITY = 1 << 20;

    /** Default number of allowance owners preallocated. */
    public static final int DEFAULT_ALLOWANCE_OWNER_CAPACITY = 1 << 16;

    /** Default per-owner delegate capacity. */
    public static final int DEFAULT_DELEGATE_CAPACITY = 1 << 4;

    /** Default number of distinct clients preallocated in the dedup table. */
    public static final int DEFAULT_DEDUP_CLIENT_CAPACITY = 1 << 16;

    /** Default dedup window size (most recent commands retained per client). */
    public static final int DEFAULT_DEDUP_WINDOW = 1 << 10;

    /** Default event-journal ring capacity in bytes (power of two, ADR 0011). */
    public static final int DEFAULT_EVENT_JOURNAL_CAPACITY = 1 << 20;

    private final int accountCapacity;
    private final int allowanceOwnerCapacity;
    private final int delegateCapacity;
    private final int dedupClientCapacity;
    private final int dedupWindow;
    private final boolean eventJournalEnabled;
    private final int eventJournalCapacity;

    private CoreConfig(
            final int accountCapacity,
            final int allowanceOwnerCapacity,
            final int delegateCapacity,
            final int dedupClientCapacity,
            final int dedupWindow,
            final boolean eventJournalEnabled,
            final int eventJournalCapacity) {
        this.accountCapacity = requirePowerOfTwo(accountCapacity, "accountCapacity");
        this.allowanceOwnerCapacity = requirePowerOfTwo(allowanceOwnerCapacity, "allowanceOwnerCapacity");
        this.delegateCapacity = requirePowerOfTwo(delegateCapacity, "delegateCapacity");
        this.dedupClientCapacity = requirePowerOfTwo(dedupClientCapacity, "dedupClientCapacity");
        this.dedupWindow = requirePowerOfTwo(dedupWindow, "dedupWindow");
        this.eventJournalEnabled = eventJournalEnabled;
        this.eventJournalCapacity = requirePowerOfTwo(eventJournalCapacity, "eventJournalCapacity");
    }

    /** Returns a configuration populated with the documented defaults. */
    public static CoreConfig defaults() {
        return new CoreConfig(
                DEFAULT_ACCOUNT_CAPACITY,
                DEFAULT_ALLOWANCE_OWNER_CAPACITY,
                DEFAULT_DELEGATE_CAPACITY,
                DEFAULT_DEDUP_CLIENT_CAPACITY,
                DEFAULT_DEDUP_WINDOW,
                false,
                DEFAULT_EVENT_JOURNAL_CAPACITY);
    }

    /** Returns a configuration with explicit capacities; each must be a power of two. */
    public static CoreConfig of(
            final int accountCapacity,
            final int allowanceOwnerCapacity,
            final int delegateCapacity,
            final int dedupClientCapacity,
            final int dedupWindow) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                false,
                DEFAULT_EVENT_JOURNAL_CAPACITY);
    }

    /**
     * Returns a copy of this configuration with the domain event journal enabled
     * (ADR 0011), using a ring of {@code capacity} bytes (a power of two).
     */
    public CoreConfig withEventJournal(final int capacity) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                true,
                capacity);
    }

    public boolean eventJournalEnabled() {
        return eventJournalEnabled;
    }

    public int eventJournalCapacity() {
        return eventJournalCapacity;
    }

    public int accountCapacity() {
        return accountCapacity;
    }

    public int allowanceOwnerCapacity() {
        return allowanceOwnerCapacity;
    }

    public int delegateCapacity() {
        return delegateCapacity;
    }

    public int dedupClientCapacity() {
        return dedupClientCapacity;
    }

    public int dedupWindow() {
        return dedupWindow;
    }

    private static int requirePowerOfTwo(final int value, final String name) {
        if (value <= 0 || !BitUtil.isPowerOfTwo(value)) {
            throw new IllegalArgumentException(name + " must be a positive power of two, was: " + value);
        }
        return value;
    }
}
