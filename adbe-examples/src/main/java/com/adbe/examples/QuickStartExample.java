package com.adbe.examples;

import com.adbe.client.AdbeClient;
import com.adbe.client.ResultHandler;
import com.adbe.client.config.ClientConfig;
import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * End-to-end quick start: boots a single-node ADBE cluster in-process, connects
 * an {@link AdbeClient}, submits a couple of commands, and prints the results.
 *
 * <p>This is the smallest thing a newcomer can clone and run to see the engine
 * work:
 *
 * <pre>{@code
 * ./gradlew :adbe-examples:run
 * }</pre>
 *
 * <p>It is deliberately not part of the deterministic hot path: it uses the
 * system clock for its await loop and prints to stdout, both of which the core
 * forbids.
 */
public final class QuickStartExample {

    private static final long CLIENT_ID = 1L;
    private static final long AWAIT_TIMEOUT_MS = 15_000L;

    private QuickStartExample() {}

    public static void main(final String[] args) throws IOException {
        final Path baseDir = Files.createTempDirectory("adbe-example-");
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        System.out.println("Starting single-node ADBE cluster in " + baseDir + " ...");
        final ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());

        // Tracks the id and balance of the most recently correlated result so the
        // await loop below can block until a specific command has been applied.
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

        final ClientConfig config = ClientConfig.builder(CLIENT_ID, ClusterConfig.ingressEndpoints(1))
                .build();

        try (AdbeClient client = new AdbeClient(config, handler)) {
            System.out.println("Connected client " + CLIENT_ID + " to the cluster.");

            System.out.println("-> CREDIT account 100 with 500");
            final long creditId = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
            awaitResult(client, creditId, lastCommandIdLo);

            System.out.println("-> TRANSFER 150 from account 100 to account 200");
            final long transferId = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            awaitResult(client, transferId, lastCommandIdLo);

            System.out.printf(
                    "Done. Final observed status=%s balance=%d (expected SUCCESS, 350).%n",
                    lastStatus[0], lastBalance[0]);
        } finally {
            node.close();
            deleteRecursively(baseDir);
        }
    }

    /** Polls the client until the result for {@code commandIdLo} arrives or timeout. */
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

    private static void deleteRecursively(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException e) {
                    // Best-effort cleanup of the temp cluster directory.
                    System.err.println("Failed to delete " + path + ": " + e.getMessage());
                }
            });
        }
    }
}
