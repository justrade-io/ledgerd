package io.justrade.ledgerd.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.DataFormatException;
import org.HdrHistogram.Histogram;

/**
 * Serialization and merge helpers for the client-side latency histograms used by
 * the bench harness.
 *
 * <p>Each load client owns a single {@link Histogram}; to compute a true
 * end-to-end percentile across all concurrent clients, the harness persists each
 * client's histogram and {@link BenchAggregator} merges them via
 * {@link Histogram#add}. This class wraps the HdrHistogram encode/decode calls
 * so the generators and the aggregator share one representation.
 *
 * <p>Not part of the deterministic hot path: this code only runs on the bench
 * driver after the load has drained.
 */
public final class BenchHistogram {

    /** Highest trackable latency value, matching {@code WriteClient} (1 hour in ns). */
    private static final long HIGHEST_TRACKABLE_VALUE = 3_600_000_000_000L;

    private static final int SIGNIFICANT_FIGURES = 3;

    private BenchHistogram() {}

    /** Serializes a histogram into the compressed HdrHistogram wire form. */
    public static byte[] encode(final Histogram histogram) {
        final ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        histogram.encodeIntoCompressedByteBuffer(buffer);
        final byte[] bytes = new byte[buffer.position()];
        buffer.flip();
        buffer.get(bytes);
        return bytes;
    }

    /** Deserializes a histogram previously produced by {@link #encode}. */
    public static Histogram decode(final byte[] bytes) {
        try {
            return Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(bytes), HIGHEST_TRACKABLE_VALUE);
        } catch (final DataFormatException e) {
            throw new IllegalArgumentException("corrupt histogram encoding", e);
        }
    }

    /** Merges several histograms into a fresh histogram representing the combined sample set. */
    public static Histogram merge(final Histogram... histograms) {
        final Histogram merged = new Histogram(HIGHEST_TRACKABLE_VALUE, SIGNIFICANT_FIGURES);
        for (final Histogram histogram : histograms) {
            merged.add(histogram);
        }
        return merged;
    }

    /** Writes a histogram to disk in its compressed wire form. */
    public static void write(final Path path, final Histogram histogram) throws IOException {
        Files.write(path, encode(histogram));
    }

    /** Reads a histogram previously written by {@link #write}. */
    public static Histogram read(final Path path) throws IOException {
        return decode(Files.readAllBytes(path));
    }
}
