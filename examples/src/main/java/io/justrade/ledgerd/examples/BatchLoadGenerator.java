package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.write.client.BackpressureException;
import io.justrade.ledgerd.write.client.BatchResultHandler;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.TransferLeg;
import io.justrade.ledgerd.write.client.TransferLegResult;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Drives a deterministic sustained transfer-batch load against a remote LEDGERD
 * cluster through the write-client SDK, for throughput and saturation
 * benchmarking of the batch path (ADR 0012).
 *
 * <p>The scenario credits account {@code 100} with {@code LEDGERD_TOTAL} units and
 * then submits {@code LEDGERD_TOTAL / LEDGERD_LEGS_PER_BATCH} batches, each with
 * {@code LEDGERD_LEGS_PER_BATCH} one-unit transfer legs from account 100 to
 * account 200. The final state is deterministic and verifiable against the read
 * replica: balance(100) = 0, balance(200) = actualLegs, supply = actualLegs.
 *
 * <p>Configuration (environment):
 *
 * <pre>
 *   LEDGERD_INGRESS_ENDPOINTS    cluster client form "0=host:20100,1=...,2=..."
 *   LEDGERD_EGRESS_ENDPOINT      routable host for this client's result channel
 *   LEDGERD_CLIENT_ID            client id (default 1)
 *   LEDGERD_TOTAL                total transfer legs to drive (default 1000000)
 *   LEDGERD_LEGS_PER_BATCH       legs per transfer batch (default 256, max 1024)
 *   LEDGERD_MAX_BATCH_IN_FLIGHT  in-flight batch window (default 16)
 * </pre>
 *
 * <p>Exits non-zero unless every submitted batch completes with every leg
 * SUCCESS. Deliberately not part of the deterministic hot path: it uses the
 * system clock for its drain deadlines and prints a summary.
 */
public final class BatchLoadGenerator {

    private static final long ACCOUNT_A = 100L;
    private static final long ACCOUNT_B = 200L;
    private static final long DRAIN_TIMEOUT_MS = 120_000L;

    private BatchLoadGenerator() {}

    public static void main(final String[] args) {
        final String ingress = requireEnv("LEDGERD_INGRESS_ENDPOINTS");
        final long clientId = envLong("LEDGERD_CLIENT_ID", 1L);
        final int totalLegs = envInt("LEDGERD_TOTAL", 1_000_000);
        final int legsPerBatch = envInt("LEDGERD_LEGS_PER_BATCH", 256);
        final int maxBatchInFlight = envInt("LEDGERD_MAX_BATCH_IN_FLIGHT", 16);
        final String egressHost = System.getenv("LEDGERD_EGRESS_ENDPOINT");

        final long[] nonSuccess = {0L};
        final long[] batchResults = {0L};
        final BatchResultHandler batchHandler = (batchIdHi, batchIdLo, results) -> {
            batchResults[0]++;
            for (final TransferLegResult leg : results) {
                if (leg.status() != StatusCode.SUCCESS) {
                    nonSuccess[0]++;
                }
            }
        };
        final ResultHandler commandHandler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            if (status != StatusCode.SUCCESS) {
                nonSuccess[0]++;
            }
        };

        final ClientConfig.Builder builder = ClientConfig.builder(clientId, ingress)
                .maxBatchSize(legsPerBatch)
                .maxBatchInFlight(maxBatchInFlight);
        if (egressHost != null && !egressHost.isBlank()) {
            builder.egressChannel("aeron:udp?endpoint=" + egressHost + ":0");
        }

        try (WriteClient client = new WriteClient(builder.build(), commandHandler)) {
            client.setBatchResultHandler(batchHandler);

            // Report the current leader so a fault harness can kill it mid-flight.
            final int leader = awaitLeader(client);
            System.out.printf("leader=%d%n", leader);
            System.out.flush();

            // Compute the batch count up front and fund account A with exactly the
            // number of legs that will be transferred, so balance(100) drains to
            // zero even when totalLegs is not a multiple of legsPerBatch.
            final int numBatches = totalLegs / legsPerBatch;
            final long actualLegs = (long) numBatches * legsPerBatch;
            submitCommand(client, CommandType.CREDIT, ACCOUNT_A, 0L, actualLegs);
            drainCommands(client);

            final TransferLeg[] legs = new TransferLeg[legsPerBatch];
            for (int i = 0; i < legsPerBatch; i++) {
                legs[i] = new TransferLeg(ACCOUNT_A, ACCOUNT_B, 1L, 0L, false);
            }

            final long startNanos = System.nanoTime();
            long batchesSubmitted = 0L;
            for (int b = 0; b < numBatches; b++) {
                submitBatch(client, legs);
                batchesSubmitted++;
            }
            drainBatches(client, batchResults, batchesSubmitted);
            final double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;

            final Histogram histogram = client.latencyHistogram();
            System.out.printf(
                    "batchload: clientId=%d batches=%d legs=%d legsPerBatch=%d maxBatchInFlight=%d"
                            + " nonSuccess=%d leaderChanges=%d backpressure=%d"
                            + " legsPerSec=%.0f batchesPerSec=%.0f"
                            + " p50=%dus p99=%dus p99.9=%dus max=%dus%n",
                    clientId,
                    batchesSubmitted,
                    actualLegs,
                    legsPerBatch,
                    maxBatchInFlight,
                    nonSuccess[0],
                    client.leaderChanges(),
                    client.backpressureEvents(),
                    actualLegs / seconds,
                    batchesSubmitted / seconds,
                    TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(50.0)),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99.0)),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99.9)),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getMaxValue()));

            if (nonSuccess[0] != 0L || batchResults[0] != batchesSubmitted) {
                throw new IllegalStateException("batch load incomplete: completed=" + batchResults[0] + " expected="
                        + batchesSubmitted + " nonSuccess=" + nonSuccess[0]);
            }
        }
    }

    /** Submits one command, polling on backpressure so the in-flight window never drops a command. */
    private static void submitCommand(
            final WriteClient client,
            final CommandType type,
            final long accountA,
            final long accountB,
            final long amount) {
        while (true) {
            try {
                client.submit(type, accountA, accountB, 0L, amount);
                client.poll();
                return;
            } catch (final BackpressureException e) {
                client.poll();
            }
        }
    }

    /** Submits one transfer batch, polling on backpressure so the batch window never drops a batch. */
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

    /** Polls until the leader is known, returning its member id. */
    private static int awaitLeader(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + 30_000L;
        while (client.leaderMemberId() < 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("no leader within timeout");
            }
            client.poll();
        }
        return client.leaderMemberId();
    }

    /** Polls until every in-flight single command has completed, or the drain deadline elapses. */
    private static void drainCommands(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (client.pendingCount() > 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("drain timeout: pending=" + client.pendingCount());
            }
            client.poll();
        }
    }

    /** Polls until every in-flight batch has completed, or the drain deadline elapses. */
    private static void drainBatches(final WriteClient client, final long[] batchResults, final long expected) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (batchResults[0] < expected) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException(
                        "batch drain timeout: completed=" + batchResults[0] + " expected=" + expected);
            }
            client.poll();
        }
    }

    private static String requireEnv(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static long envLong(final String name, final long fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value.trim());
    }

    private static int envInt(final String name, final int fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }
}
