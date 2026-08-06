package com.adbe.bench;

/**
 * A single logical wallet operation in the benchmark workload. Flat primitives so
 * the generated stream is compact and replayed identically against every backend.
 *
 * <p>For {@link OpType#TRANSFER}, {@code accountA} is the source and
 * {@code accountB} the destination; for {@code CREDIT}/{@code DEBIT} only
 * {@code accountA} is used.
 */
public record Op(OpType type, long accountA, long accountB, long amount) {

    /** Signed effect of this op on the total balance across all accounts. */
    public long supplyDelta() {
        return switch (type) {
            case CREDIT -> amount;
            case DEBIT -> -amount;
            case TRANSFER -> 0L;
        };
    }
}
