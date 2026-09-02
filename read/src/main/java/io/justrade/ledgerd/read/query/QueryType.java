package io.justrade.ledgerd.read.query;

/**
 * The kinds of read a client may request. The ordinal is used as a compact wire
 * code on the in-process query ring buffers; keep the order stable.
 */
public enum QueryType {
    /** Single account balance. */
    BALANCE,
    /** Several account balances in one request. */
    BATCH_BALANCE,
    /** Allowance for an (owner, delegate) pair. */
    ALLOWANCE,
    /** Engine-wide total supply. */
    TOTAL_SUPPLY;

    private static final QueryType[] VALUES = values();

    /** Returns the type for a wire code, or throws if it is out of range. */
    public static QueryType fromCode(final int code) {
        if (code < 0 || code >= VALUES.length) {
            throw new IllegalArgumentException("Unknown query type code: " + code);
        }
        return VALUES[code];
    }

    /** Compact wire code for this type. */
    public int code() {
        return ordinal();
    }
}
