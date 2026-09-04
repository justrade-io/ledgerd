package io.justrade.ledgerd.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.write.client.BackpressureException;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
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
 * Sustained two-phase hold churn: mixes {@code RESERVE -> RELEASE} (funds return
 * to the same account) with {@code RESERVE -> CAPTURE} (funds settle to another
 * account). Measures tail latency, bounds GC, and verifies supply conservation.
 *
 * <p>Each churn account is pre-reserved with a large buffer before the measured
 * window. The {@code WriteClient} uses a non-blocking offer with delayed retry,
 * so under ingress backpressure commands may apply out of submission order. The
 * buffer keeps {@code reserved} far above the per-cycle churn amount, so a
 * reordered {@code RELEASE}/{@code CAPTURE} never fails with
 * {@code INSUFFICIENT_RESERVED}; the churn is therefore order-robust. After the
 * churn the buffer is released and the account must return to its funded value.
 *
 * <p>Tagged {@code soak}: run via the opt-in {@code soakTest} task, never wired
 * into {@code check}.
 */
@Tag("soak")
class TwoPhaseChurnSoakTest {

    private static final long CHURN_ACCOUNT = 1L;
    private static final long CAPTURE_FROM = 10L;
    private static final long CAPTURE_TO = 11L;
    private static final long FUND_AMOUNT = 1_000_000_000L;
    private static final long RESERVE_BUFFER = 100_000_000L;
    private static final int WARMUP = 20_000;
    private static final int STEADY = 200_000;
    private static final long P99_9_BUDGET_NS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long MAX_GC_COLLECTIONS = 1_000L;

    @Test
    @Timeout(600)
    void holdChurnConservesSupplyWithinTailBudget(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                .maxInFlight(1024)
                .build();

        final long[] lastBalance = {-1L};
        final ResultHandler handler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            if (hasBalance) {
                lastBalance[0] = balance;
            }
        };

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());
                WriteClient client = new WriteClient(clientConfig, handler)) {

            submitWithBackpressure(client, CommandType.CREDIT, CHURN_ACCOUNT, 0L, 0L, FUND_AMOUNT);
            submitWithBackpressure(client, CommandType.CREDIT, CAPTURE_FROM, 0L, 0L, FUND_AMOUNT);
            submitWithBackpressure(client, CommandType.RESERVE, CHURN_ACCOUNT, 0L, 0L, RESERVE_BUFFER);
            submitWithBackpressure(client, CommandType.RESERVE, CAPTURE_FROM, 0L, 0L, RESERVE_BUFFER);
            drain(client);

            drive(client, WARMUP);
            drain(client);

            final long gcBefore = gcCollectionCount();
            client.latencyHistogram().reset();

            drive(client, STEADY);
            drain(client);

            final long gcDelta = gcCollectionCount() - gcBefore;
            final Histogram histogram = client.latencyHistogram();

            assertEquals(client.submitted(), client.completed(), "every submitted command must complete");
            assertEquals(0L, client.expired(), "no command may exhaust retries");
            assertTrue(histogram.getTotalCount() >= STEADY * 2L, "latency samples recorded for steady window");

            final long p50 = histogram.getValueAtPercentile(50.0);
            final long p99 = histogram.getValueAtPercentile(99.0);
            final long p999 = histogram.getValueAtPercentile(99.9);
            final long max = histogram.getMaxValue();
            System.out.printf(
                    "two-phase: cycles=%d p50=%dus p99=%dus p99.9=%dus max=%dus gc=%d%n",
                    STEADY,
                    TimeUnit.NANOSECONDS.toMicros(p50),
                    TimeUnit.NANOSECONDS.toMicros(p99),
                    TimeUnit.NANOSECONDS.toMicros(p999),
                    TimeUnit.NANOSECONDS.toMicros(max),
                    gcDelta);

            assertTrue(
                    p999 <= P99_9_BUDGET_NS,
                    "p99.9 latency " + TimeUnit.NANOSECONDS.toMicros(p999) + "us exceeded budget");
            assertTrue(
                    gcDelta <= MAX_GC_COLLECTIONS,
                    "GC collections " + gcDelta + " exceeded bound " + MAX_GC_COLLECTIONS
                            + " (possible allocation leak)");

            // Release the churn account's buffer: net-zero reserve/release churn must
            // leave reserved exactly at the buffer, so the account returns to funded.
            submitWithBackpressure(client, CommandType.RELEASE, CHURN_ACCOUNT, 0L, 0L, RESERVE_BUFFER);
            drain(client);
            assertEquals(FUND_AMOUNT, lastBalance[0], "reserve/release churn must conserve the available balance");

            assertCaptureSettlesToDestination(client, lastBalance);
        }
    }

    /** Serialized capture check: held funds settle to the destination exactly once. */
    private static void assertCaptureSettlesToDestination(final WriteClient client, final long[] lastBalance) {
        submitWithBackpressure(client, CommandType.CREDIT, 20L, 0L, 0L, 1_000L);
        drain(client);
        submitWithBackpressure(client, CommandType.RESERVE, 20L, 0L, 0L, 100L);
        drain(client);
        submitWithBackpressure(client, CommandType.CAPTURE, 20L, 21L, 0L, 100L);
        drain(client);
        submitWithBackpressure(client, CommandType.DEBIT, 21L, 0L, 0L, 1L);
        drain(client);
        assertEquals(99L, lastBalance[0], "capture must settle exactly 100 to the destination");
    }

    /** Each cycle is a deterministic reserve/settle pair on one of two buffered accounts. */
    private static void drive(final WriteClient client, final int cycles) {
        for (int i = 0; i < cycles; i++) {
            final long amount = 1L + (i % 100);
            if (i % 10 == 0) {
                submitWithBackpressure(client, CommandType.RESERVE, CAPTURE_FROM, 0L, 0L, amount);
                submitWithBackpressure(client, CommandType.CAPTURE, CAPTURE_FROM, CAPTURE_TO, 0L, amount);
            } else {
                submitWithBackpressure(client, CommandType.RESERVE, CHURN_ACCOUNT, 0L, 0L, amount);
                submitWithBackpressure(client, CommandType.RELEASE, CHURN_ACCOUNT, 0L, 0L, amount);
            }
        }
    }

    private static void submitWithBackpressure(
            final WriteClient client,
            final CommandType type,
            final long accountA,
            final long accountB,
            final long accountC,
            final long amount) {
        while (true) {
            try {
                client.submit(type, accountA, accountB, accountC, amount);
                client.poll();
                return;
            } catch (final BackpressureException e) {
                client.poll();
            }
        }
    }

    private static void drain(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + 60_000L;
        while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
            client.poll();
        }
        assertTrue(client.pendingCount() == 0, "all in-flight commands must drain");
    }

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
