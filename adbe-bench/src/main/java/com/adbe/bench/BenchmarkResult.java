package com.adbe.bench;

/**
 * One backend's measured window: throughput plus latency percentiles (all latency
 * fields in nanoseconds), or an error if the backend could not run (for example
 * because Docker was unavailable).
 */
public record BenchmarkResult(
        String backend,
        long ops,
        double throughputPerSec,
        long p50Nanos,
        long p99Nanos,
        long p999Nanos,
        long maxNanos,
        String error) {

    public static BenchmarkResult ok(
            final String backend,
            final long ops,
            final double throughputPerSec,
            final long p50Nanos,
            final long p99Nanos,
            final long p999Nanos,
            final long maxNanos) {
        return new BenchmarkResult(backend, ops, throughputPerSec, p50Nanos, p99Nanos, p999Nanos, maxNanos, null);
    }

    public static BenchmarkResult failed(final String backend, final String error) {
        return new BenchmarkResult(backend, 0L, 0.0, 0L, 0L, 0L, 0L, error);
    }

    public boolean isError() {
        return error != null;
    }
}
