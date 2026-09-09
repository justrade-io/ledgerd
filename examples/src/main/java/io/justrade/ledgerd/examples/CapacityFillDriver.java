package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.write.client.BackpressureException;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;

/**
 * Fills the cluster's preallocated balance maps with a deterministic hot/cold
 * account distribution so the capacity model in {@code docs/CAPACITY-PLANNING.md}
 * can be validated against real heap usage.
 *
 * <p>The scenario credits one unit to each of {@code LEDGERD_HOT_ACCOUNTS}
 * distinct accounts on the hot asset (asset {@code 0}), then
 * {@code LEDGERD_COLD_ACCOUNTS} distinct accounts on each of the remaining
 * {@code LEDGERD_ASSETS - 1} cold assets. Touching an asset preallocates one
 * {@code accountCapacity} map, and filling the hot asset past
 * {@code 0.65 * accountCapacity} triggers the map resize, so the hot/cold split
 * exercises both terms of the balance-memory model.
 *
 * <p>Multiple clients share the fill through {@code LEDGERD_CLIENT_INDEX} /
 * {@code LEDGERD_CLIENT_COUNT}, each taking a disjoint contiguous slice of the
 * account-id space, so the distinct (asset, account) pairs never collide.
 *
 * <p>Configuration (environment):
 *
 * <pre>
 *   LEDGERD_INGRESS_ENDPOINTS  cluster client form "0=host:20100,1=...,2=..."
 *   LEDGERD_EGRESS_ENDPOINT    routable host for this client's result channel
 *   LEDGERD_CLIENT_ID          write-client id (default 1)
 *   LEDGERD_CLIENT_INDEX       0-based index of this client (default 0)
 *   LEDGERD_CLIENT_COUNT       total clients sharing the fill (default 1)
 *   LEDGERD_ASSETS             number of assets to touch (default 32)
 *   LEDGERD_HOT_ACCOUNTS       distinct accounts on the hot asset (default 681000)
 *   LEDGERD_COLD_ACCOUNTS      distinct accounts per cold asset (default 1000)
 *   LEDGERD_MAX_IN_FLIGHT      in-flight window size (default 1024)
 * </pre>
 *
 * <p>Exits non-zero unless every submitted credit completes successfully.
 * Deliberately not part of the deterministic hot path: it uses the system clock
 * for its drain deadline and prints a summary.
 */
public final class CapacityFillDriver {

    private static final long CREDIT_AMOUNT = 1L;
    private static final long DRAIN_TIMEOUT_MS = 120_000L;

    private CapacityFillDriver() {}

    public static void main(final String[] args) {
        final String ingress = requireEnv("LEDGERD_INGRESS_ENDPOINTS");
        final long clientId = envLong("LEDGERD_CLIENT_ID", 1L);
        final int clientIndex = envInt("LEDGERD_CLIENT_INDEX", 0);
        final int clientCount = envInt("LEDGERD_CLIENT_COUNT", 1);
        final int assets = envInt("LEDGERD_ASSETS", 32);
        final long hotAccounts = envLong("LEDGERD_HOT_ACCOUNTS", 681_000L);
        final long coldAccounts = envLong("LEDGERD_COLD_ACCOUNTS", 1_000L);
        final int maxInFlight = envInt("LEDGERD_MAX_IN_FLIGHT", 1024);
        final String egressHost = System.getenv("LEDGERD_EGRESS_ENDPOINT");

        if (assets < 1) {
            throw new IllegalArgumentException("LEDGERD_ASSETS must be positive");
        }
        if (clientCount < 1 || clientIndex < 0 || clientIndex >= clientCount) {
            throw new IllegalArgumentException("LEDGERD_CLIENT_INDEX must be in [0, LEDGERD_CLIENT_COUNT)");
        }

        // Disjoint contiguous slices of the account-id space for this client.
        final long hotStart = clientIndex * hotAccounts / clientCount;
        final long hotEnd = (clientIndex + 1) * hotAccounts / clientCount;
        final long coldStart = clientIndex * coldAccounts / clientCount;
        final long coldEnd = (clientIndex + 1) * coldAccounts / clientCount;
        final long hotCount = hotEnd - hotStart;
        final long coldCount = coldEnd - coldStart;
        final long expectedCommands = hotCount + (assets - 1L) * coldCount;

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
            final int leader = awaitLeader(client);
            System.out.printf("leader=%d%n", leader);
            System.out.flush();

            final long startNanos = System.nanoTime();

            // Hot asset: account ids [hotStart+1, hotEnd].
            for (long k = 0L; k < hotCount; k++) {
                submit(client, 0L, hotStart + k + 1L);
            }

            // Cold assets: account ids [coldStart+1, coldEnd] on each of 1..assets-1.
            for (int asset = 1; asset < assets; asset++) {
                for (long k = 0L; k < coldCount; k++) {
                    submit(client, asset, coldStart + k + 1L);
                }
            }

            drain(client);
            final double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;

            System.out.printf(
                    "fill: clientId=%d clientIndex=%d/%d assets=%d hotAccounts=%d coldAccounts=%d"
                            + " submitted=%d completed=%d nonSuccess=%d leaderChanges=%d backpressure=%d%n",
                    clientId,
                    clientIndex,
                    clientCount,
                    assets,
                    hotCount,
                    coldCount,
                    client.submitted(),
                    client.completed(),
                    nonSuccess[0],
                    client.leaderChanges(),
                    client.backpressureEvents());
            System.out.printf(
                    "{\"event\":\"summary\",\"clientId\":%d,\"clientIndex\":%d,\"clientCount\":%d,"
                            + "\"assets\":%d,\"hotAccounts\":%d,\"coldAccounts\":%d,"
                            + "\"submitted\":%d,\"completed\":%d,\"nonSuccess\":%d,"
                            + "\"leaderChanges\":%d,\"backpressure\":%d,\"elapsedSec\":%.3f}%n",
                    clientId,
                    clientIndex,
                    clientCount,
                    assets,
                    hotCount,
                    coldCount,
                    client.submitted(),
                    client.completed(),
                    nonSuccess[0],
                    client.leaderChanges(),
                    client.backpressureEvents(),
                    seconds);

            if (client.completed() != expectedCommands || nonSuccess[0] != 0L) {
                throw new IllegalStateException("fill incomplete: completed=" + client.completed() + " expected="
                        + expectedCommands + " nonSuccess=" + nonSuccess[0]);
            }
        }
    }

    /** Submits one credit, polling on backpressure so the in-flight window never drops a command. */
    private static void submit(final WriteClient client, final long assetId, final long accountId) {
        while (true) {
            try {
                client.submit(CommandType.CREDIT, assetId, accountId, 0L, 0L, CREDIT_AMOUNT);
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
