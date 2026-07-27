package com.adbe.telemetry;

import org.agrona.concurrent.status.AtomicCounter;

/**
 * A {@link CounterSink} backed by off-heap Agrona {@link AtomicCounter}s so that
 * external readers can observe counter values from another thread.
 *
 * <p>Writes use release ordering ({@link AtomicCounter#incrementOrdered()} /
 * {@link AtomicCounter#setOrdered(long)}), which is correct and allocation-free
 * for the single-writer clustered-service thread and cheaper than a full
 * sequentially-consistent store.
 *
 * <p>The backing counters are allocated by the launcher from a standalone
 * {@code CountersManager} buffer; this class holds references only and performs
 * no allocation on the hot path.
 */
public final class AtomicCounterSink implements CounterSink {

    private final AtomicCounter[] counters;

    /**
     * @param counters one counter per {@link Counter} ordinal; length must equal
     *     {@link Counter#COUNT}.
     */
    public AtomicCounterSink(final AtomicCounter[] counters) {
        if (counters.length != Counter.COUNT) {
            throw new IllegalArgumentException("expected " + Counter.COUNT + " counters, got " + counters.length);
        }
        this.counters = counters;
    }

    @Override
    public void increment(final Counter counter) {
        counters[counter.ordinal()].incrementOrdered();
    }

    @Override
    public void set(final Counter counter, final long value) {
        counters[counter.ordinal()].setOrdered(value);
    }
}
