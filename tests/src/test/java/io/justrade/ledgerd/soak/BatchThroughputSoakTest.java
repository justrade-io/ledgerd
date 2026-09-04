package io.justrade.ledgerd.soak;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.write.client.BackpressureException;
import io.justrade.ledgerd.write.client.TransferLeg;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end transfer-batch throughput sweep (ADR 0012). For a fixed number of
 * transfer legs, varies the batch size and measures transfers/sec and per-batch
 * latency. Validates the core batch promise: a larger batch amortizes the
 * per-message consensus/replication cost, so throughput rises with batch size.
 *
 * <p>Also measures a fully-linked chain at one size to quantify the undo-recording
 * overhead versus independent legs. Tagged {@code soak}: run via the opt-in
 * {@code soakTest} task, never wired into {@code check}.
 */
@Tag("soak")
class BatchThroughputSoakTest {

    private static final int[] SWEEP_SIZES = {1, 8, 64, 256, 1024};
    private static final int LEGS_PER_SIZE = 32_768;
    private static final int LINKED_SIZE = 256;
    private static final long SOURCE = 1L;
    private static final int DEST_POOL = 128;
    private static final long FUND_AMOUNT = 1_000_000_000_000L;

    @Test
    @Timeout(600)
    void largerBatchesAmortizeConsensusCost(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final ClientConfig clientConfig = ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1))
                .maxBatchSize(1024)
                .maxBatchInFlight(16)
                .build();

        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());
                WriteClient client = new WriteClient(
                        clientConfig, (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {})) {

            client.submit(CommandType.CREDIT, SOURCE, 0L, 0L, FUND_AMOUNT);
            drainBatches(client);

            // Warm the batch path so the first measured sweep is not JIT-cold.
            final TransferLeg[] warmupLegs = independentLegs(64);
            for (int i = 0; i < 128; i++) {
                submitBatch(client, warmupLegs);
            }
            drainBatches(client);

            final double[] throughputs = new double[SWEEP_SIZES.length];
            for (int i = 0; i < SWEEP_SIZES.length; i++) {
                final int size = SWEEP_SIZES[i];
                throughputs[i] =
                        sweepBatchSize(client, "batch-" + size, independentLegs(size), LEGS_PER_SIZE / size, size);
            }

            final double independent256 = throughputs[3];
            final double linked256 = sweepBatchSize(
                    client, "linked-" + LINKED_SIZE, linkedLegs(LINKED_SIZE), LEGS_PER_SIZE / LINKED_SIZE, LINKED_SIZE);
            System.out.printf(
                    "batch-sweep: linked/independent @256 = %.0f / %.0f transfers/s (undo overhead %.2f%%)%n",
                    linked256, independent256, (independent256 - linked256) / independent256 * 100.0);

            assertEquals(0L, client.expired(), "no batch may exhaust retries");
            // The largest batch must beat a single-leg batch: fewer consensus messages
            // for the same number of transfers is the whole point of F1.
            assertTrue(
                    throughputs[SWEEP_SIZES.length - 1] > throughputs[0],
                    "largest batch throughput " + throughputs[SWEEP_SIZES.length - 1] + " must exceed single-leg "
                            + throughputs[0]);
        }
    }

    /** Submits {@code batchCount} batches of {@code legCount} legs each and returns transfers/sec. */
    private static double sweepBatchSize(
            final WriteClient client,
            final String label,
            final TransferLeg[] legs,
            final int batchCount,
            final int legCount) {
        client.latencyHistogram().reset();
        final long startNanos = System.nanoTime();
        for (int b = 0; b < batchCount; b++) {
            submitBatch(client, legs);
        }
        drainBatches(client);
        final long elapsedNanos = System.nanoTime() - startNanos;
        final double transfersPerSec = (double) batchCount * legCount / (elapsedNanos / 1_000_000_000.0);
        final Histogram histogram = client.latencyHistogram();
        final long p99 = histogram.getValueAtPercentile(99.0);
        final long p999 = histogram.getValueAtPercentile(99.9);
        System.out.printf(
                "%s: legs=%d batches=%d throughput=%.0f transfers/s meanBatch=%dus p99=%dus p99.9=%dus%n",
                label,
                legCount,
                batchCount,
                transfersPerSec,
                TimeUnit.NANOSECONDS.toMicros(elapsedNanos / batchCount),
                TimeUnit.NANOSECONDS.toMicros(p99),
                TimeUnit.NANOSECONDS.toMicros(p999));
        return transfersPerSec;
    }

    private static TransferLeg[] independentLegs(final int size) {
        final TransferLeg[] legs = new TransferLeg[size];
        for (int i = 0; i < size; i++) {
            legs[i] = new TransferLeg(SOURCE, 2L + (i % DEST_POOL), 1L, 0L, false);
        }
        return legs;
    }

    private static TransferLeg[] linkedLegs(final int size) {
        final TransferLeg[] legs = new TransferLeg[size];
        for (int i = 0; i < size; i++) {
            legs[i] = new TransferLeg(SOURCE, 2L + (i % DEST_POOL), 1L, 0L, i < size - 1);
        }
        return legs;
    }

    private static void submitBatch(final WriteClient client, final TransferLeg[] legs) {
        while (true) {
            try {
                client.submitTransferBatch(legs);
                client.poll();
                return;
            } catch (final BackpressureException e) {
                client.poll();
            }
        }
    }

    private static void drainBatches(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + 120_000L;
        while (client.submitted() != client.completed() && System.currentTimeMillis() < deadline) {
            client.poll();
        }
        assertEquals(client.submitted(), client.completed(), "all submitted batches must complete");
    }
}
