package io.justrade.ledgerd.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

/**
 * Deterministic tests for the bench histogram serialization and merge helpers.
 */
final class BenchHistogramTest {

    private static final long HIGHEST_TRACKABLE_VALUE = 3_600_000_000_000L;
    private static final int SIGNIFICANT_FIGURES = 3;

    @Test
    void encodeDecodeRoundtripPreservesPercentiles() {
        final Histogram original = new Histogram(HIGHEST_TRACKABLE_VALUE, SIGNIFICANT_FIGURES);
        for (long value = 1_000_000L; value <= 10_000_000L; value += 1_000_000L) {
            original.recordValue(value);
        }

        final Histogram decoded = BenchHistogram.decode(BenchHistogram.encode(original));

        assertEquals(original.getTotalCount(), decoded.getTotalCount());
        assertEquals(original.getValueAtPercentile(50.0), decoded.getValueAtPercentile(50.0));
        assertEquals(original.getValueAtPercentile(99.0), decoded.getValueAtPercentile(99.0));
        assertEquals(original.getMaxValue(), decoded.getMaxValue());
    }

    @Test
    void mergeCombinesSamplesAcrossHistograms() {
        final Histogram a = new Histogram(HIGHEST_TRACKABLE_VALUE, SIGNIFICANT_FIGURES);
        final Histogram b = new Histogram(HIGHEST_TRACKABLE_VALUE, SIGNIFICANT_FIGURES);
        for (long value = 1_000_000L; value <= 5_000_000L; value += 1_000_000L) {
            a.recordValue(value);
        }
        for (long value = 6_000_000L; value <= 10_000_000L; value += 1_000_000L) {
            b.recordValue(value);
        }

        final Histogram merged = BenchHistogram.merge(a, b);

        assertEquals(a.getTotalCount() + b.getTotalCount(), merged.getTotalCount());
        assertEquals(b.getMaxValue(), merged.getMaxValue());
    }

    @Test
    void percentileDistributionWritesHgrmFormat() throws IOException {
        final Histogram histogram = new Histogram(HIGHEST_TRACKABLE_VALUE, SIGNIFICANT_FIGURES);
        for (long value = 1_000_000L; value <= 10_000_000L; value += 1_000_000L) {
            histogram.recordValue(value);
        }
        final Path path = Files.createTempFile("bench", ".hgrm");
        try {
            BenchHistogram.writePercentileDistribution(path, histogram);
            final String content = Files.readString(path);
            assertTrue(content.contains("Percentile"));
            assertTrue(content.contains("TotalCount"));
            assertTrue(content.contains("1/(1-Percentile)"));
            // The Value column must stay in native nanoseconds, not be scaled.
            assertTrue(content.contains(Long.toString(histogram.getMaxValue())));
        } finally {
            Files.deleteIfExists(path);
        }
    }
}
