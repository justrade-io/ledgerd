package com.adbe.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.client.AdbeClient;
import com.adbe.client.BackpressureException;
import com.adbe.client.ResultHandler;
import com.adbe.client.config.ClientConfig;
import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Long-running steady-state load against a single-node cluster. Verifies that
 * every command completes and reports end-to-end latency percentiles, while
 * observing JVM garbage-collection activity during the measured window.
 *
 * <p>The core engine hot path is allocation-free (asserted separately by JMH
 * {@code -prof gc}); this soak exercises the full client/cluster path under
 * sustained load. Tagged {@code soak}: run via the opt-in {@code soakTest} task,
 * never wired into {@code check}.
 */
@Tag("soak")
class ChaosSoakTest {

    private static final int WARMUP = 20_000;
    private static final int STEADY = 200_000;
    private static final long P99_9_BUDGET_NS = TimeUnit.MILLISECONDS.toNanos(50);

    /**
     * Upper bound on GC collections during the steady window. The deterministic
     * core hot path is allocation-free (asserted by JMH {@code -prof gc}); this
     * full client/cluster path does allocate at the edge, so the soak asserts GC
     * stays bounded rather than literally zero - a gross breach signals an
     * allocation leak.
     */
    private static final long MAX_GC_COLLECTIONS = 1_000L;

    @Test
    @Timeout(600)
    void sustainedLoadCompletesWithinTailLatencyBudget(@TempDir final Path baseDir) {
        final ClusterConfig config = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final long[] completed = {0L};
        final ResultHandler handler =
                (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> completed[0]++;

        try (ClusterNode node = new ClusterNode(config, CoreConfig.defaults())) {
            final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                    .maxInFlight(1024)
                    .build();

            try (AdbeClient client = new AdbeClient(clientConfig, handler)) {
                // Warm up the JIT and the transport before measuring.
                drive(client, WARMUP);

                final long gcBefore = gcCollectionCount();
                client.latencyHistogram().reset();

                drive(client, STEADY);

                final long gcDelta = gcCollectionCount() - gcBefore;
                final Histogram histogram = client.latencyHistogram();

                assertEquals((long) (WARMUP + STEADY), client.completed(), "every submitted command must complete");
                assertTrue(histogram.getTotalCount() >= STEADY, "latency samples recorded for steady window");

                final long p99 = histogram.getValueAtPercentile(99.0);
                final long p999 = histogram.getValueAtPercentile(99.9);
                final long max = histogram.getMaxValue();
                System.out.printf(
                        "soak: commands=%d gcCollections=%d p99=%dus p99.9=%dus max=%dus%n",
                        STEADY,
                        gcDelta,
                        TimeUnit.NANOSECONDS.toMicros(p99),
                        TimeUnit.NANOSECONDS.toMicros(p999),
                        TimeUnit.NANOSECONDS.toMicros(max));

                assertTrue(
                        p999 <= P99_9_BUDGET_NS,
                        "p99.9 latency " + TimeUnit.NANOSECONDS.toMicros(p999) + "us exceeded budget");
                assertTrue(
                        gcDelta <= MAX_GC_COLLECTIONS,
                        "GC collections " + gcDelta + " exceeded bound " + MAX_GC_COLLECTIONS
                                + " (possible allocation leak)");
            }
        }
    }

    private static void drive(final AdbeClient client, final int count) {
        for (int i = 0; i < count; i++) {
            submitWithBackpressure(client);
        }
        // Drain any remaining in-flight commands.
        final long deadline = System.currentTimeMillis() + 60_000L;
        while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
            client.poll();
        }
    }

    private static void submitWithBackpressure(final AdbeClient client) {
        while (true) {
            try {
                client.submit(CommandType.CREDIT, 1L, 0L, 0L, 1L);
                client.poll();
                return;
            } catch (final BackpressureException e) {
                client.poll();
            }
        }
    }

    /** Total GC collections across all collectors (young and old) since JVM start. */
    private static long gcCollectionCount() {
        long total = 0L;
        for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            final long count = bean.getCollectionCount();
            if (count > 0) {
                total += count;
            }
        }
        return total;
    }
}
