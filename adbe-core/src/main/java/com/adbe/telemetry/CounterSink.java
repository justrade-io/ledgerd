package com.adbe.telemetry;

/**
 * Allocation-free, single-writer sink for the core's observability counters.
 *
 * <p>The default {@link #NOOP} implementation discards updates and is used by the
 * engine in unit and replay tests, where no external counter surface is required.
 * The cluster wires an off-heap implementation so operators can read counter
 * values from another thread without disturbing the single-writer hot path.
 *
 * <p>Implementations MUST be allocation-free and MUST NOT block; the only caller
 * is the clustered-service thread.
 */
public interface CounterSink {

    /** Prometheus-style type id for a monotonic counter. */
    int TYPE_COUNTER = 0;

    /** Prometheus-style type id for a gauge (value that can go up or down). */
    int TYPE_GAUGE = 1;

    /** Ordinal indices of the counters exposed by the core. */
    enum Counter {
        COMMANDS_PROCESSED,
        DUPLICATES_DETECTED,
        INSUFFICIENT_BALANCE,
        INSUFFICIENT_ALLOWANCE,
        INVALID_ACCOUNT,
        OVERFLOW,
        INVALID_AMOUNT,
        BACKPRESSURE_EVENTS,
        LEADER_ELECTIONS,
        DEDUP_EVICTED,
        EVENT_JOURNAL_OVERFLOW;

        /** Number of distinct counters; useful for sizing off-heap buffers. */
        public static final int COUNT = values().length;
    }

    /** Ordinal indices of the gauges exposed by the core. */
    enum Gauge {
        SNAPSHOT_WRITE_NANOS,
        SNAPSHOT_READ_NANOS,
        BALANCE_COUNT,
        ALLOWANCE_OWNER_COUNT,
        DEDUP_CLIENT_COUNT;

        /** Number of distinct gauges; useful for sizing off-heap buffers. */
        public static final int COUNT = values().length;
    }

    /** Increments the given counter by one using release ordering. */
    void increment(Counter counter);

    /** Sets the given counter to an absolute value using release ordering. */
    void set(Counter counter, long value);

    /** Sets the given gauge to an absolute value using release ordering. */
    void set(Gauge gauge, long value);

    /** A sink that discards all updates; the default for tests and the raw engine. */
    CounterSink NOOP = new CounterSink() {
        @Override
        public void increment(final Counter counter) {
            // Intentionally empty: no external counter surface.
        }

        @Override
        public void set(final Counter counter, final long value) {
            // Intentionally empty: no external counter surface.
        }

        @Override
        public void set(final Gauge gauge, final long value) {
            // Intentionally empty: no external counter surface.
        }
    };
}
