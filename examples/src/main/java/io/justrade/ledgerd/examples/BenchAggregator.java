package io.justrade.ledgerd.examples;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Merges the per-client latency histograms written by {@link LoadGenerator} and
 * {@link BatchLoadGenerator} into a single combined histogram, then prints its
 * percentiles as a one-line JSON summary for the bench harness.
 *
 * <p>Usage: {@code BenchAggregator <histogram-dir> [glob]} where the directory
 * holds the {@code *.hlog} files produced during a single run. The optional glob
 * scopes the merge to one generator (e.g. {@code client-*.hlog} vs
 * {@code batch-client-*.hlog}); it defaults to {@code *.hlog}.
 */
public final class BenchAggregator {

    private BenchAggregator() {}

    public static void main(final String[] args) throws IOException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("usage: BenchAggregator <histogram-dir> [glob]");
        }
        final Path dir = Path.of(args[0]);
        final String glob = args.length == 2 ? args[1] : "*.hlog";
        final List<Histogram> histograms = readAll(dir, glob);
        if (histograms.isEmpty()) {
            throw new IllegalStateException("no " + glob + " histograms found in " + dir);
        }
        final Histogram merged = BenchHistogram.merge(histograms.toArray(new Histogram[0]));
        System.out.printf(
                "{\"event\":\"merged\",\"samples\":%d,\"p50_us\":%d,\"p99_us\":%d,\"p999_us\":%d,\"max_us\":%d}%n",
                merged.getTotalCount(),
                TimeUnit.NANOSECONDS.toMicros(merged.getValueAtPercentile(50.0)),
                TimeUnit.NANOSECONDS.toMicros(merged.getValueAtPercentile(99.0)),
                TimeUnit.NANOSECONDS.toMicros(merged.getValueAtPercentile(99.9)),
                TimeUnit.NANOSECONDS.toMicros(merged.getMaxValue()));
    }

    private static List<Histogram> readAll(final Path dir, final String glob) throws IOException {
        final List<Histogram> histograms = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            for (final Path path : stream) {
                histograms.add(BenchHistogram.read(path));
            }
        }
        return histograms;
    }
}
