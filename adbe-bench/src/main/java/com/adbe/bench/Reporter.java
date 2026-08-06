package com.adbe.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Renders benchmark results as a console table and a CSV file. */
public final class Reporter {

    private static final double NANOS_PER_MICRO = 1_000.0;

    private Reporter() {}

    public static void printTable(final List<BenchmarkResult> results) {
        System.out.printf(
                "%n%-10s %14s %12s %12s %12s %12s%n",
                "backend", "throughput/s", "p50 (us)", "p99 (us)", "p99.9 (us)", "max (us)");
        System.out.println("-".repeat(76));
        for (final BenchmarkResult r : results) {
            if (r.isError()) {
                System.out.printf("%-10s %14s  (%s)%n", r.backend(), "FAILED", r.error());
                continue;
            }
            System.out.printf(
                    "%-10s %14.0f %12.1f %12.1f %12.1f %12.1f%n",
                    r.backend(),
                    r.throughputPerSec(),
                    micros(r.p50Nanos()),
                    micros(r.p99Nanos()),
                    micros(r.p999Nanos()),
                    micros(r.maxNanos()));
        }
        System.out.println();
    }

    public static void writeCsv(final String csvPath, final List<BenchmarkResult> results) {
        final Path path = Path.of(csvPath);
        final StringBuilder sb = new StringBuilder();
        sb.append("backend,ops,throughput_ops_per_sec,p50_us,p99_us,p999_us,max_us,status\n");
        for (final BenchmarkResult r : results) {
            sb.append(r.backend())
                    .append(',')
                    .append(r.ops())
                    .append(',')
                    .append(String.format("%.2f", r.throughputPerSec()))
                    .append(',')
                    .append(String.format("%.2f", micros(r.p50Nanos())))
                    .append(',')
                    .append(String.format("%.2f", micros(r.p99Nanos())))
                    .append(',')
                    .append(String.format("%.2f", micros(r.p999Nanos())))
                    .append(',')
                    .append(String.format("%.2f", micros(r.maxNanos())))
                    .append(',')
                    .append(r.isError() ? "FAILED" : "OK")
                    .append('\n');
        }
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Files.writeString(path, sb.toString());
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to write CSV: " + csvPath, e);
        }
        System.out.println("Wrote " + path.toAbsolutePath());
    }

    private static double micros(final long nanos) {
        return nanos / NANOS_PER_MICRO;
    }
}
