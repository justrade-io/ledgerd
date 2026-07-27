package com.adbe.examples;

import com.adbe.client.AdbeClient;
import com.adbe.client.ResultHandler;
import com.adbe.client.config.ClientConfig;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;

/**
 * Connects to an already-running ADBE cluster (for example the three-node
 * cluster from {@code docker-compose.yml}), submits a credit and a transfer, and
 * prints the results. Unlike {@link QuickStartExample}, it does not start its own
 * cluster; it drives a remote one over the network.
 *
 * <p>Ingress endpoints are read from the first program argument or, failing that,
 * the {@code ADBE_INGRESS_ENDPOINTS} environment variable, in Aeron cluster
 * client form:
 *
 * <pre>{@code
 * 0=adbe-node-0:20100,1=adbe-node-1:20200,2=adbe-node-2:20300
 * }</pre>
 *
 * <p>The client id defaults to {@code 1} and can be overridden with
 * {@code ADBE_CLIENT_ID}. Exits with a non-zero status if the expected results do
 * not arrive, so it can be used as a smoke test in CI or compose.
 */
public final class RemoteClientExample {

    private static final long AWAIT_TIMEOUT_MS = 30_000L;

    private RemoteClientExample() {}

    public static void main(final String[] args) {
        final String ingressEndpoints = resolveEndpoints(args);
        final long clientId = Long.parseLong(envOrDefault("ADBE_CLIENT_ID", "1"));

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
        final String egressEndpoint = System.getenv("ADBE_EGRESS_ENDPOINT");
        if (egressEndpoint != null && !egressEndpoint.isBlank()) {
            // When the client and cluster are on different hosts, advertise an
            // endpoint the nodes can route back to (see docker/client-entrypoint.sh).
            configBuilder.egressChannel("aeron:udp?endpoint=" + egressEndpoint);
        }
        final ClientConfig config = configBuilder.build();

        try (AdbeClient client = new AdbeClient(config, handler)) {
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

    private static String resolveEndpoints(final String[] args) {
        if (args.length > 0 && !args[0].isBlank()) {
            return args[0];
        }
        final String fromEnv = System.getenv("ADBE_INGRESS_ENDPOINTS");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        throw new IllegalArgumentException("ingress endpoints required as arg[0] or ADBE_INGRESS_ENDPOINTS, "
                + "e.g. 0=adbe-node-0:20100,1=adbe-node-1:20200,2=adbe-node-2:20300");
    }

    private static String envOrDefault(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void requireStatus(final StatusCode status, final long expectedBalance, final long actualBalance) {
        if (status != StatusCode.SUCCESS || actualBalance != expectedBalance) {
            throw new IllegalStateException("unexpected result: status=" + status + " balance=" + actualBalance
                    + " (expected SUCCESS, " + expectedBalance + ")");
        }
    }

    private static void awaitResult(final AdbeClient client, final long commandIdLo, final long[] lastCommandIdLo) {
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
