package io.justrade.ledgerd.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.testkit.ParetoAccountSampler;
import io.justrade.ledgerd.write.client.BackpressureException;
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
 * Sustained single-command transfer load with Pareto hot-account contention:
 * the top {@code hotAccounts} accounts absorb {@code hotTrafficRatio} of the
 * traffic, the rest is spread uniformly. Measures end-to-end tail latency and
 * bounds GC, mirroring the canonical TigerBeetle hot-account workload.
 *
 * <p>Tagged {@code soak}: run via the opt-in {@code soakTest} task, never wired
 * into {@code check}.
 */
@Tag("soak")
class ParetoContentionSoakTest {

    private static final int HOT_ACCOUNTS = 8;
    private static final int COLD_ACCOUNTS = 1_000;
    private static final double HOT_TRAFFIC_RATIO = 0.9;
    private static final long FUND_AMOUNT = 1_000_000_000_000L;
    private static final int WARMUP = 20_000;
    private static final int STEADY = 200_000;
    private static final long P99_9_BUDGET_NS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long MAX_GC_COLLECTIONS = 1_000L;

    @Test
    @Timeout(600)
    void hotAccountContentionStaysWithinTailBudget(@TempDir final Path baseDir) {
        final int accountCount = HOT_ACCOUNTS + COLD_ACCOUNTS;
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                .maxInFlight(1024)
                .build();

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());
                WriteClient client = new WriteClient(
                        clientConfig, (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {})) {

            fundAccounts(client, accountCount);
            drain(client);

            final ParetoAccountSampler sampler =
                    new ParetoAccountSampler(0xC0FFEEL, HOT_ACCOUNTS, COLD_ACCOUNTS, HOT_TRAFFIC_RATIO);
            drive(client, sampler, WARMUP);
            drain(client);

            final long gcBefore = gcCollectionCount();
            client.latencyHistogram().reset();

            drive(client, sampler, STEADY);
            drain(client);

            final long gcDelta = gcCollectionCount() - gcBefore;
            final Histogram histogram = client.latencyHistogram();

            assertEquals(
                    (long) accountCount + WARMUP + STEADY, client.completed(), "every submitted command must complete");
            assertEquals(0L, client.expired(), "no command may exhaust retries");
            assertTrue(histogram.getTotalCount() >= STEADY, "latency samples recorded for steady window");

            final long p50 = histogram.getValueAtPercentile(50.0);
            final long p99 = histogram.getValueAtPercentile(99.0);
            final long p999 = histogram.getValueAtPercentile(99.9);
            final long max = histogram.getMaxValue();
            System.out.printf(
                    "pareto: hot=%d cold=%d hotTraffic=%.2f commands=%d p50=%dus p99=%dus p99.9=%dus max=%dus gc=%d%n",
                    HOT_ACCOUNTS,
                    COLD_ACCOUNTS,
                    HOT_TRAFFIC_RATIO,
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
        }
    }

    private static void fundAccounts(final WriteClient client, final int accountCount) {
        for (long account = 1L; account <= accountCount; account++) {
            submitWithBackpressure(client, CommandType.CREDIT, account, 0L, 0L, FUND_AMOUNT);
        }
    }

    private static void drive(final WriteClient client, final ParetoAccountSampler sampler, final int count) {
        for (int i = 0; i < count; i++) {
            long from = sampler.nextAccount();
            long to = sampler.nextAccount();
            while (to == from) {
                to = sampler.nextAccount();
            }
            submitWithBackpressure(client, CommandType.TRANSFER, from, to, 0L, 1L);
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
