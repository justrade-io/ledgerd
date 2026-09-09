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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Drives a deterministic sustained transfer-batch load across a Pareto-distributed
 * account set against a remote LEDGERD cluster, for throughput and contention
 * benchmarking of the batch path (ADR 0012).
 *
 * <p>The scenario funds each account on each asset with {@code LEDGERD_FUND_AMOUNT}
 * units, then submits {@code LEDGERD_TOTAL / LEDGERD_LEGS_PER_BATCH} batches, each
 * with {@code LEDGERD_LEGS_PER_BATCH} one-unit transfer legs between sampled
 * (asset, account) pairs. Transfers conserve supply, so the final total supply is
 * exactly {@code assets * (hot + cold) * fundAmount} per client, verifiable with a
 * supply-only check.
 *
 * <p>Configuration (environment):
 *
 * <pre>
 *   LEDGERD_INGRESS_ENDPOINTS    cluster client form "0=host:20100,1=...,2=..."
 *   LEDGERD_EGRESS_ENDPOINT      routable host for this client's result channel
 *   LEDGERD_CLIENT_ID            client id (default 1)
 *   LEDGERD_ASSETS               number of assets (default 1)
 *   LEDGERD_HOT_ACCOUNTS         hot account count per asset (default 8)
 *   LEDGERD_COLD_ACCOUNTS        cold account count per asset (default 1000)
 *   LEDGERD_HOT_RATIO            fraction of traffic to hot accounts (default 0.9)
 *   LEDGERD_FUND_AMOUNT          seed units credited per account (default 1e12)
 *   LEDGERD_TOTAL                total transfer legs (default 1000000)
 *   LEDGERD_LEGS_PER_BATCH       legs per transfer batch (default 256)
 *   LEDGERD_MAX_BATCH_IN_FLIGHT  in-flight batch window (default 16)
 *   LEDGERD_SEED                 reproducible sampler seed
 * </pre>
 *
 * <p>Exits non-zero unless every submitted batch completes with every leg
 * SUCCESS. Deliberately not part of the deterministic hot path: it uses the
 * system clock for its drain deadlines and prints a summary.
 */
public final class ParetoBatchGenerator {

    private static final long DRAIN_TIMEOUT_MS = 120_000L;
    private static final long ASSET_SEED_SALT = 0x9E3779B97F4A7C15L;

    private ParetoBatchGenerator() {}

    public static void main(final String[] args) {
        final String ingress = requireEnv("LEDGERD_INGRESS_ENDPOINTS");
        final long clientId = envLong("LEDGERD_CLIENT_ID", 1L);
        final int assets = envInt("LEDGERD_ASSETS", 1);
        final int hotAccounts = envInt("LEDGERD_HOT_ACCOUNTS", 8);
        final int coldAccounts = envInt("LEDGERD_COLD_ACCOUNTS", 1_000);
        final double hotRatio = envDouble("LEDGERD_HOT_RATIO", 0.9);
        final long fundAmount = envLong("LEDGERD_FUND_AMOUNT", 1_000_000_000_000L);
        final int totalLegs = envInt("LEDGERD_TOTAL", 1_000_000);
        final int legsPerBatch = envInt("LEDGERD_LEGS_PER_BATCH", 256);
        final int maxBatchInFlight = envInt("LEDGERD_MAX_BATCH_IN_FLIGHT", 16);
        final long seed = envLong("LEDGERD_SEED", 0xC0FFEEL);
        final String egressHost = System.getenv("LEDGERD_EGRESS_ENDPOINT");

        if (assets < 1) {
            throw new IllegalArgumentException("LEDGERD_ASSETS must be positive");
        }

        final long accountCount = (long) hotAccounts + coldAccounts;
        final long fundedCommands = (long) assets * accountCount;
        final int numBatches = totalLegs / legsPerBatch;
        final long actualLegs = (long) numBatches * legsPerBatch;

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

            final int leader = awaitLeader(client);
            System.out.printf("leader=%d%n", leader);
            System.out.flush();

            fundAccounts(client, assets, accountCount, fundAmount);
            drainCommands(client);

            final ParetoSampler accounts = new ParetoSampler(seed, hotAccounts, coldAccounts, hotRatio);
            final Random assetRandom = new Random(seed ^ ASSET_SEED_SALT);
            client.latencyHistogram().reset();

            final long startNanos = System.nanoTime();
            for (int b = 0; b < numBatches; b++) {
                submitBatch(client, buildLegs(legsPerBatch, assets, accounts, assetRandom));
            }
            drainBatches(client, batchResults, numBatches);
            final double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;

            final Histogram histogram = client.latencyHistogram();
            final long legsPerSec = Math.round(actualLegs / seconds);
            final long batchesPerSec = Math.round(numBatches / seconds);
            final long p50Us = TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(50.0));
            final long p99Us = TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99.0));
            final long p999Us = TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99.9));
            final long maxUs = TimeUnit.NANOSECONDS.toMicros(histogram.getMaxValue());

            System.out.printf(
                    "pareto-batch: clientId=%d assets=%d hot=%d cold=%d hotRatio=%.2f funded=%d"
                            + " batches=%d legs=%d legsPerBatch=%d maxBatchInFlight=%d"
                            + " nonSuccess=%d leaderChanges=%d backpressure=%d"
                            + " legsPerSec=%d batchesPerSec=%d"
                            + " p50=%dus p99=%dus p99.9=%dus max=%dus%n",
                    clientId,
                    assets,
                    hotAccounts,
                    coldAccounts,
                    hotRatio,
                    fundedCommands,
                    numBatches,
                    actualLegs,
                    legsPerBatch,
                    maxBatchInFlight,
                    nonSuccess[0],
                    client.leaderChanges(),
                    client.backpressureEvents(),
                    legsPerSec,
                    batchesPerSec,
                    p50Us,
                    p99Us,
                    p999Us,
                    maxUs);
            System.out.printf(
                    "{\"event\":\"summary\",\"clientId\":%d,\"assets\":%d,\"hotAccounts\":%d,\"coldAccounts\":%d,"
                            + "\"hotRatio\":%.2f,\"batches\":%d,\"legs\":%d,\"legsPerBatch\":%d,"
                            + "\"maxBatchInFlight\":%d,\"nonSuccess\":%d,\"leaderChanges\":%d,\"backpressure\":%d,"
                            + "\"legsPerSec\":%d,\"batchesPerSec\":%d,\"p50_us\":%d,\"p99_us\":%d,"
                            + "\"p999_us\":%d,\"max_us\":%d}%n",
                    clientId,
                    assets,
                    hotAccounts,
                    coldAccounts,
                    hotRatio,
                    numBatches,
                    actualLegs,
                    legsPerBatch,
                    maxBatchInFlight,
                    nonSuccess[0],
                    client.leaderChanges(),
                    client.backpressureEvents(),
                    legsPerSec,
                    batchesPerSec,
                    p50Us,
                    p99Us,
                    p999Us,
                    maxUs);

            writeHistogram(histogram);

            if (nonSuccess[0] != 0L || batchResults[0] != numBatches) {
                throw new IllegalStateException("pareto batch load incomplete: completed=" + batchResults[0]
                        + " expected=" + numBatches + " nonSuccess=" + nonSuccess[0]);
            }
        }
    }

    /** Builds one batch of Pareto-sampled transfer legs. */
    private static TransferLeg[] buildLegs(
            final int legsPerBatch, final int assets, final ParetoSampler accounts, final Random assetRandom) {
        final TransferLeg[] legs = new TransferLeg[legsPerBatch];
        for (int i = 0; i < legsPerBatch; i++) {
            final long asset = assetRandom.nextInt(assets);
            long from = accounts.nextAccount();
            long to = accounts.nextAccount();
            while (to == from) {
                to = accounts.nextAccount();
            }
            legs[i] = new TransferLeg(from, to, 1L, asset, false);
        }
        return legs;
    }

    /** Credits every account on every asset with the seed amount, so no transfer leg starves. */
    private static void fundAccounts(
            final WriteClient client, final int assets, final long accountCount, final long fundAmount) {
        for (int asset = 0; asset < assets; asset++) {
            for (long account = 1L; account <= accountCount; account++) {
                submitCommand(client, CommandType.CREDIT, asset, account, 0L, fundAmount);
            }
        }
    }

    /** Submits one command, polling on backpressure so the in-flight window never drops a command. */
    private static void submitCommand(
            final WriteClient client,
            final CommandType type,
            final long assetId,
            final long accountA,
            final long accountB,
            final long amount) {
        while (true) {
            try {
                client.submit(type, assetId, accountA, accountB, 0L, amount);
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

    /** Persists the latency histogram when the harness requests a cross-client merge. */
    private static void writeHistogram(final Histogram histogram) {
        final String path = System.getenv("LEDGERD_HISTOGRAM_PATH");
        if (path == null || path.isBlank()) {
            return;
        }
        try {
            BenchHistogram.write(Path.of(path), histogram);
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to write histogram to " + path, e);
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

    private static double envDouble(final String name, final double fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Double.parseDouble(value.trim());
    }
}
