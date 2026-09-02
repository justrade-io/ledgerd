package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import java.util.Random;

/**
 * Connects to an already-running LEDGERD cluster (for example the three-node
 * cluster from {@code docker-compose.yml}), submits a credit and a transfer, and
 * prints the results. Unlike {@link QuickStartExample}, it does not start its own
 * cluster; it drives a remote one over the network.
 *
 * <p>Ingress endpoints are read from the first program argument or, failing that,
 * the {@code LEDGERD_INGRESS_ENDPOINTS} environment variable, in Aeron cluster
 * client form:
 *
 * <pre>{@code
 * 0=ledgerd-node-0:20100,1=ledgerd-node-1:20200,2=ledgerd-node-2:20300
 * }</pre>
 *
 * <p>An optional {@code LEDGERD_SCENARIO} selects an alternate traffic pattern:
 * {@code multiasset} exercises multi-asset and holds commands; {@code risk} drives
 * a population of accounts exchanging money, plus transaction-velocity spikes and
 * money-flow hubs, so the AI risk dashboard (ADR 0012) fills with data and flags
 * accounts. Its scale is env-tunable ({@code LEDGERD_RISK_POPULATION},
 * {@code LEDGERD_RISK_BACKGROUND_TX}, {@code LEDGERD_RISK_SPIKE_ACCOUNTS},
 * {@code LEDGERD_RISK_HUBS}, {@code LEDGERD_RISK_HUB_SPOKES}, {@code LEDGERD_RISK_BURST});
 * with {@code LEDGERD_SCENARIO_LOOP=true} it repeats until the process is stopped.
 */
public final class RemoteClientExample {

    private static final long AWAIT_TIMEOUT_MS = 30_000L;

    // Risk scenario (ADR 0012). A population of accounts exchanging money builds a
    // dense money-flow graph; a few accounts get a slow baseline then a fast burst
    // (velocity anomaly) and a few hubs fan out to many counterparties (high graph
    // centrality). Scale knobs are env-overridable so the dashboard can be driven
    // with as much data as wanted.
    private static final long POPULATION_BASE = 1L;
    private static final int POPULATION = 120;
    private static final long POPULATION_SEED_BALANCE = 1_000_000L;
    private static final int BACKGROUND_TX = 400;
    private static final int BACKGROUND_MAX_AMOUNT = 50;
    private static final long SPIKE_ACCOUNT = 800L;
    private static final int SPIKE_ACCOUNTS = 4;
    private static final int SPIKE_EDGES = 20;
    private static final int WARMUP_TXNS = 8;
    private static final long WARMUP_SPACING_MS = 300L;
    private static final int BURST_TXNS = 30;
    private static final long HUB_ACCOUNT = 900L;
    private static final int HUBS = 3;
    private static final int HUB_SPOKES = 30;
    private static final long HUB_SPOKE_AMOUNT = 100L;
    private static final long RANDOM_SEED = 42L;
    private static final long LOOP_PAUSE_MS = 5_000L;

    private RemoteClientExample() {}

    public static void main(final String[] args) {
        final String ingressEndpoints = resolveEndpoints(args);
        final long clientId = Long.parseLong(envOrDefault("LEDGERD_CLIENT_ID", "1"));

        System.out.println("Connecting client " + clientId + " to cluster at " + ingressEndpoints);

        final long[] lastCommandIdLo = {-1L};
        final long[] lastBalance = {Long.MIN_VALUE};
        final StatusCode[] lastStatus = {StatusCode.NULL_VAL};

        final ResultHandler handler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            lastCommandIdLo[0] = idLo;
            lastStatus[0] = status;
            if (hasBalance) {
                lastBalance[0] = balance;
            }
            System.out.printf(
                    "  <- result: status=%s balance=%s%n", status, hasBalance ? Long.toString(balance) : "n/a");
        };

        final ClientConfig.Builder configBuilder = ClientConfig.builder(clientId, ingressEndpoints);
        final String egressEndpoint = System.getenv("LEDGERD_EGRESS_ENDPOINT");
        if (egressEndpoint != null && !egressEndpoint.isBlank()) {
            // When the client and cluster are on different hosts, advertise an
            // endpoint the nodes can route back to (see docker/client-entrypoint.sh).
            configBuilder.egressChannel("aeron:udp?endpoint=" + egressEndpoint);
        }
        final ClientConfig config = configBuilder.build();

        final String scenario = envOrDefault("LEDGERD_SCENARIO", "");

        try (WriteClient client = new WriteClient(config, handler)) {
            if ("multiasset".equals(scenario)) {
                runMultiAssetScenario(client, lastStatus, lastCommandIdLo);
                return;
            }
            if ("risk".equals(scenario)) {
                runRiskScenario(client, lastStatus, lastCommandIdLo);
                return;
            }

            System.out.println("-> CREDIT account 100 with 500");
            final long creditId = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
            awaitResult(client, creditId, lastCommandIdLo);
            requireStatus(lastStatus[0], 500L, lastBalance[0]);

            System.out.println("-> TRANSFER 150 from account 100 to account 200");
            final long transferId = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            awaitResult(client, transferId, lastCommandIdLo);
            requireStatus(lastStatus[0], 350L, lastBalance[0]);

            System.out.printf(
                    "OK: cluster processed commands (leaderChanges=%d, completed=%d). Final balance=%d.%n",
                    client.leaderChanges(), client.completed(), lastBalance[0]);
        }
    }

    /**
     * Exercises the multi-asset (ADR 0009) and holds (ADR 0010) commands on fresh
     * accounts and assets so an operational check can observe both over the read
     * HTTP API (available balances and per-asset supply). Held funds are inferred
     * from the drop in available balance together with conserved supply, since the
     * read API exposes available balances, not the reserved bucket.
     */
    private static void runMultiAssetScenario(
            final WriteClient client, final StatusCode[] lastStatus, final long[] lastId) {
        // Asset 1: plain multi-asset credit + transfer on fresh accounts 700/701.
        submitAndAwait(client, lastStatus, lastId, CommandType.CREDIT, 1L, 700L, 0L, 0L, 1000L);
        submitAndAwait(client, lastStatus, lastId, CommandType.TRANSFER, 1L, 700L, 701L, 0L, 400L);

        // Asset 2: two-phase holds. reserve moves available -> reserved; capture
        // settles held funds to 701; release returns the remainder to available.
        submitAndAwait(client, lastStatus, lastId, CommandType.CREDIT, 2L, 700L, 0L, 0L, 1000L);
        submitAndAwait(client, lastStatus, lastId, CommandType.RESERVE, 2L, 700L, 0L, 0L, 300L);
        submitAndAwait(client, lastStatus, lastId, CommandType.CAPTURE, 2L, 700L, 701L, 0L, 200L);
        submitAndAwait(client, lastStatus, lastId, CommandType.RELEASE, 2L, 700L, 0L, 0L, 50L);

        System.out.printf(
                "OK: multi-asset + holds scenario committed (leaderChanges=%d, completed=%d).%n",
                client.leaderChanges(), client.completed());
    }

    /**
     * Drives the AI risk dashboard (ADR 0012): a transaction-velocity spike on one
     * account and a money-flow hub fanning out to many counterparties, so the
     * follower's velocity z-score and graph centrality features flag those
     * accounts. With {@code LEDGERD_SCENARIO_LOOP=true} it repeats until stopped so the
     * dashboard keeps showing live spike-then-decay behaviour.
     */
    private static void runRiskScenario(final WriteClient client, final StatusCode[] lastStatus, final long[] lastId) {
        final boolean loop = Boolean.parseBoolean(envOrDefault("LEDGERD_SCENARIO_LOOP", "false"));
        final int population = envInt("LEDGERD_RISK_POPULATION", POPULATION);
        final int backgroundTx = envInt("LEDGERD_RISK_BACKGROUND_TX", BACKGROUND_TX);
        final int spikeAccounts = envInt("LEDGERD_RISK_SPIKE_ACCOUNTS", SPIKE_ACCOUNTS);
        final int spikeEdges = envInt("LEDGERD_RISK_SPIKE_EDGES", SPIKE_EDGES);
        final int hubs = envInt("LEDGERD_RISK_HUBS", HUBS);
        final int hubSpokes = envInt("LEDGERD_RISK_HUB_SPOKES", HUB_SPOKES);
        final int burst = envInt("LEDGERD_RISK_BURST", BURST_TXNS);
        final Random rnd = new Random(RANDOM_SEED);

        // Seed the population once; balances persist across loop iterations.
        System.out.println("-> risk: seeding " + population + " accounts");
        for (int a = 0; a < population; a++) {
            drive(client, lastStatus, lastId, CommandType.CREDIT, POPULATION_BASE + a, 0L, POPULATION_SEED_BALANCE);
        }

        do {
            System.out.println("-> risk: " + backgroundTx + " background transfers across the population");
            for (int t = 0; t < backgroundTx; t++) {
                final long from = POPULATION_BASE + rnd.nextInt(population);
                long to = POPULATION_BASE + rnd.nextInt(population);
                if (to == from) {
                    to = POPULATION_BASE + (from - POPULATION_BASE + 1) % population;
                }
                drive(
                        client,
                        lastStatus,
                        lastId,
                        CommandType.TRANSFER,
                        from,
                        to,
                        1L + rnd.nextInt(BACKGROUND_MAX_AMOUNT));
            }

            System.out.println("-> risk: velocity spikes on " + spikeAccounts + " accounts");
            for (int s = 0; s < spikeAccounts; s++) {
                final long account = SPIKE_ACCOUNT + s;
                // Give the spike account graph centrality too: a flagged account is
                // both fast and well-connected, and the extra weight keeps the
                // decaying peak above the flag threshold after the spike passes.
                drive(
                        client,
                        lastStatus,
                        lastId,
                        CommandType.CREDIT,
                        account,
                        0L,
                        (long) spikeEdges * HUB_SPOKE_AMOUNT * 2L);
                for (int e = 0; e < spikeEdges; e++) {
                    final long spoke = POPULATION_BASE + (s * spikeEdges + e) % population;
                    drive(client, lastStatus, lastId, CommandType.TRANSFER, account, spoke, HUB_SPOKE_AMOUNT);
                }
                for (int i = 0; i < WARMUP_TXNS; i++) {
                    drive(client, lastStatus, lastId, CommandType.CREDIT, account, 0L, 10L);
                    if (i < WARMUP_TXNS - 1) {
                        quietSleep(WARMUP_SPACING_MS);
                    }
                }
                for (int i = 0; i < burst; i++) {
                    drive(client, lastStatus, lastId, CommandType.CREDIT, account, 0L, 1L);
                }
            }

            System.out.println("-> risk: " + hubs + " money-flow hubs x " + hubSpokes + " spokes");
            for (int h = 0; h < hubs; h++) {
                final long hub = HUB_ACCOUNT + h;
                drive(
                        client,
                        lastStatus,
                        lastId,
                        CommandType.CREDIT,
                        hub,
                        0L,
                        (long) hubSpokes * HUB_SPOKE_AMOUNT * 2L);
                for (int k = 0; k < hubSpokes; k++) {
                    final long spoke = POPULATION_BASE + (h * hubSpokes + k) % population;
                    drive(client, lastStatus, lastId, CommandType.TRANSFER, hub, spoke, HUB_SPOKE_AMOUNT);
                }
            }

            System.out.printf(
                    "OK: risk scenario committed (accounts=%d, leaderChanges=%d, completed=%d). Watch :8090.%n",
                    population, client.leaderChanges(), client.completed());
            if (loop) {
                quietSleep(LOOP_PAUSE_MS);
            }
        } while (loop);
    }

    // Submits one asset-0 command and blocks for its result, failing the scenario
    // on any non-success status. Quiet (no per-command log) for high-volume runs.
    private static void drive(
            final WriteClient client,
            final StatusCode[] lastStatus,
            final long[] lastId,
            final CommandType type,
            final long accountA,
            final long accountB,
            final long amount) {
        final long id = client.submit(type, 0L, accountA, accountB, 0L, amount);
        awaitResult(client, id, lastId);
        if (lastStatus[0] != StatusCode.SUCCESS) {
            throw new IllegalStateException("risk scenario " + type + " a=" + accountA + " b=" + accountB + " amount="
                    + amount + " returned " + lastStatus[0]);
        }
    }

    private static void quietSleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during risk scenario", e);
        }
    }

    private static void submitAndAwait(
            final WriteClient client,
            final StatusCode[] lastStatus,
            final long[] lastId,
            final CommandType type,
            final long assetId,
            final long a,
            final long b,
            final long c,
            final long amount) {
        System.out.printf("-> %s asset=%d a=%d b=%d amount=%d%n", type, assetId, a, b, amount);
        final long id = client.submit(type, assetId, a, b, c, amount);
        awaitResult(client, id, lastId);
        if (lastStatus[0] != StatusCode.SUCCESS) {
            throw new IllegalStateException("scenario command " + type + " on asset " + assetId + " returned "
                    + lastStatus[0] + " (expected SUCCESS)");
        }
    }

    private static String resolveEndpoints(final String[] args) {
        if (args.length > 0 && !args[0].isBlank()) {
            return args[0];
        }
        final String fromEnv = System.getenv("LEDGERD_INGRESS_ENDPOINTS");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        throw new IllegalArgumentException("ingress endpoints required as arg[0] or LEDGERD_INGRESS_ENDPOINTS, "
                + "e.g. 0=ledgerd-node-0:20100,1=ledgerd-node-1:20200,2=ledgerd-node-2:20300");
    }

    private static String envOrDefault(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int envInt(final String name, final int fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }

    private static void requireStatus(final StatusCode status, final long expectedBalance, final long actualBalance) {
        if (status != StatusCode.SUCCESS || actualBalance != expectedBalance) {
            throw new IllegalStateException("unexpected result: status=" + status + " balance=" + actualBalance
                    + " (expected SUCCESS, " + expectedBalance + ")");
        }
    }

    private static void awaitResult(final WriteClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("no result for commandIdLo=" + commandIdLo + " within timeout");
    }
}
