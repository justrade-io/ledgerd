package com.adbe.bench.store;

import com.adbe.bench.Op;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Base for backends whose natural concurrency is a fixed pool of worker threads,
 * each with its own blocking connection (JDBC, Redis). The op stream is split into
 * contiguous slices, one per worker; each worker times every op into a private
 * histogram (avoiding cross-thread contention on a shared recorder), and the
 * slices are merged after all workers finish.
 *
 * <p>The worker count is the concurrency-parity knob: it is set to the same value
 * ADBE uses for {@code maxInFlight}, so the comparison holds the number of
 * concurrent in-flight operations roughly equal across backends.
 */
abstract class ThreadedDataStore implements DataStore {

    private static final long HIGHEST_TRACKABLE_NS = TimeUnit.HOURS.toNanos(1);

    /** A per-thread connection plus the logic to execute one op against it. */
    interface Worker extends AutoCloseable {
        void execute(Op op) throws Exception;

        @Override
        void close() throws Exception;
    }

    private final String name;
    private final int workers;
    private final Histogram latency = new Histogram(HIGHEST_TRACKABLE_NS, 3);

    ThreadedDataStore(final String name, final int workers) {
        this.name = name;
        this.workers = Math.max(1, workers);
    }

    /** Creates a fresh worker bound to its own connection. Called once per thread per {@link #run}. */
    protected abstract Worker createWorker() throws Exception;

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final void run(final Op[] ops) throws Exception {
        final int threadCount = Math.min(workers, Math.max(1, ops.length));
        final Thread[] threads = new Thread[threadCount];
        final Histogram[] histograms = new Histogram[threadCount];
        final Throwable[] failures = new Throwable[threadCount];
        final int chunk = (ops.length + threadCount - 1) / threadCount;

        for (int t = 0; t < threadCount; t++) {
            final int index = t;
            final int start = t * chunk;
            final int end = Math.min(ops.length, start + chunk);
            final Histogram local = new Histogram(HIGHEST_TRACKABLE_NS, 3);
            histograms[t] = local;
            threads[t] = new Thread(
                    () -> {
                        try (Worker worker = createWorker()) {
                            for (int i = start; i < end; i++) {
                                final long began = System.nanoTime();
                                worker.execute(ops[i]);
                                local.recordValue(Math.min(System.nanoTime() - began, HIGHEST_TRACKABLE_NS));
                            }
                        } catch (final Throwable e) {
                            failures[index] = e;
                        }
                    },
                    name + "-worker-" + t);
            threads[t].start();
        }

        for (final Thread thread : threads) {
            thread.join();
        }
        for (final Throwable failure : failures) {
            if (failure != null) {
                throw new IllegalStateException(name + " worker failed", failure);
            }
        }
        for (final Histogram local : histograms) {
            latency.add(local);
        }
    }

    @Override
    public final Histogram latencyHistogram() {
        return latency;
    }

    @Override
    public final void resetLatency() {
        latency.reset();
    }
}
