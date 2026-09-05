package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.write.client.BackpressureException;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Drives a deterministic sustained load against a remote LEDGERD cluster through
 * the write-client SDK, for smoke and scale verification of a containerized
 * deployment.
 *
 * <p>The scenario credits account {@code 100} with {@code LEDGERD_TOTAL} units and
 * then transfers one unit at a time to account {@code 200}, so the final state is
 * deterministic and verifiable against the read replica: balance(100) = 0,
 * balance(200) = LEDGERD_TOTAL, totalSupply(asset 0) = LEDGERD_TOTAL.
 *
 * <p>Configuration (environment):
 *
 * <pre>
 *   LEDGERD_INGRESS_ENDPOINTS  cluster client form "0=host:20100,1=...,2=..."
 *   LEDGERD_EGRESS_ENDPOINT    routable host for this client's result channel
 *   LEDGERD_CLIENT_ID          client id (default 1)
 *   LEDGERD_TOTAL              number of unit transfers to drive (default 10000)
 *   LEDGERD_MAX_IN_FLIGHT      in-flight window size (default 1024)
 * </pre>
 *
 * <p>Exits non-zero unless every submitted command completes successfully with no
 * expiry. Deliberately not part of the deterministic hot path: it uses the system
 * clock for its drain deadline and prints a summary.
 */
public final class LoadGenerator {

    private static final long ACCOUNT_A = 100L;
    private static final long ACCOUNT_B = 200L;
    private static final long DRAIN_TIMEOUT_MS = 120_000L;

    private LoadGenerator() {}

    public static void main(final String[] args) {
        final String ingress = requireEnv("LEDGERD_INGRESS_ENDPOINTS");
        final long clientId = envLong("LEDGERD_CLIENT_ID", 1L);
        final int total = envInt("LEDGERD_TOTAL", 10_000);
        final int maxInFlight = envInt("LEDGERD_MAX_IN_FLIGHT", 1024);
        final String egressHost = System.getenv("LEDGERD_EGRESS_ENDPOINT");

        final long[] nonSuccess = {0L};
        final ResultHandler handler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            if (status != StatusCode.SUCCESS) {
                nonSuccess[0]++;
            }
        };

        final ClientConfig.Builder builder =
                ClientConfig.builder(clientId, ingress).maxInFlight(maxInFlight);
        if (egressHost != null && !egressHost.isBlank()) {
            builder.egressChannel("aeron:udp?endpoint=" + egressHost + ":0");
        }

        try (WriteClient client = new WriteClient(builder.build(), handler)) {
            // Report the current leader so a fault harness can kill it mid-flight.
            final int leader = awaitLeader(client);
            System.out.printf("leader=%d%n", leader);
            System.out.flush();

            // Seed account A up front and wait for the credit to be acknowledged
            // before the transfer storm, so a mid-flight leader kill can never
            // reorder a transfer ahead of the credit and cause INSUFFICIENT_BALANCE.
            submit(client, CommandType.CREDIT, ACCOUNT_A, 0L, total);
            drain(client);
            for (int i = 0; i < total; i++) {
                submit(client, CommandType.TRANSFER, ACCOUNT_A, ACCOUNT_B, 1L);
            }

            drain(client);

            final Histogram histogram = client.latencyHistogram();
            System.out.printf(
                    "load: clientId=%d submitted=%d completed=%d expired=%d nonSuccess=%d leaderChanges=%d"
                            + " backpressure=%d p50=%dus p99=%dus p99.9=%dus max=%dus%n",
                    clientId,
                    client.submitted(),
                    client.completed(),
                    client.expired(),
                    nonSuccess[0],
                    client.leaderChanges(),
                    client.backpressureEvents(),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(50.0)),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99.0)),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99.9)),
                    TimeUnit.NANOSECONDS.toMicros(histogram.getMaxValue()));

            final long expected = (long) total + 1L;
            if (client.completed() != expected || client.expired() != 0L || nonSuccess[0] != 0L) {
                throw new IllegalStateException("load incomplete: completed=" + client.completed() + " expected="
                        + expected + " expired=" + client.expired() + " nonSuccess=" + nonSuccess[0]);
            }
        }
    }

    /** Submits one command, polling on backpressure so the in-flight window never drops a command. */
    private static void submit(
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

    /** Polls until the cluster leader is known, returning its member id. */
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

    /** Polls until every in-flight command has completed, or the drain deadline elapses. */
    private static void drain(final WriteClient client) {
        final long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (client.pendingCount() > 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("drain timeout: pending=" + client.pendingCount());
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
