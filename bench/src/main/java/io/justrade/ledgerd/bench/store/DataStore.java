package io.justrade.ledgerd.bench.store;

import io.justrade.ledgerd.bench.Op;
import org.HdrHistogram.Histogram;

/**
 * A backend under benchmark. Implementations run the same logical wallet workload
 * (credit / debit / transfer) with their own natural concurrency model and record
 * end-to-end per-op latency into a {@link Histogram}, so the harness can compare
 * throughput and tail latency across LEDGERD, PostgreSQL, and Redis on equal footing.
 *
 * <p>Not thread-safe from the harness's point of view: {@link #setup}, {@link #run},
 * {@link #resetLatency}, and {@link #verify} are called sequentially on the harness
 * thread. Any internal concurrency (worker threads, an async pipeline) is owned by
 * the implementation and must be quiesced before {@link #run} returns.
 */
public interface DataStore extends AutoCloseable {

    /** Short backend label used in the report (for example {@code "ledgerd"}). */
    String name();

    /** Creates {@code accounts} accounts (ids {@code 1..accounts}) each holding {@code initialBalance}. */
    void setup(int accounts, long initialBalance) throws Exception;

    /** Executes every op, recording per-op latency, and blocks until all have completed. */
    void run(Op[] ops) throws Exception;

    /** End-to-end per-op latency in nanoseconds, accumulated across {@link #run} calls. */
    Histogram latencyHistogram();

    /** Discards recorded latency samples so a following {@link #run} measures a clean window. */
    void resetLatency();

    /**
     * Sanity check after all ops have run. Implementations that can cheaply read
     * their total supply assert it equals {@code expectedSupply}; others assert an
     * equivalent health invariant (for example that no op was dropped).
     */
    void verify(long expectedSupply) throws Exception;

    @Override
    void close();
}
