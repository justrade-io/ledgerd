package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.write.client.BatchResultHandler;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.TransferLeg;
import io.justrade.ledgerd.write.client.TransferLegResult;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * End-to-end transfer-batch example (ADR 0012): boots a single-node cluster
 * in-process, funds two accounts, then submits three batches to show independent
 * legs, an atomic linked chain, and a linked chain that rolls back on failure.
 *
 * <pre>{@code
 * ./gradlew :examples:run -PmainClass=io.justrade.ledgerd.examples.BatchTransferExample
 * }</pre>
 *
 * <p>Deliberately not part of the deterministic hot path: it uses the system
 * clock for its await loop and prints to stdout, both of which the core forbids.
 */
public final class BatchTransferExample {

    private static final long CLIENT_ID = 1L;
    private static final long AWAIT_TIMEOUT_MS = 15_000L;

    private BatchTransferExample() {}

    public static void main(final String[] args) throws IOException {
        final Path baseDir = Files.createTempDirectory("ledgerd-batch-example-");
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        System.out.println("Starting single-node LEDGERD cluster in " + baseDir + " ...");
        final ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());

        final long[] lastCommandIdLo = {-1L};
        final long[] lastBatchIdLo = {-1L};

        final ResultHandler commandHandler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            lastCommandIdLo[0] = idLo;
            System.out.printf(
                    "  <- command result: status=%s balance=%s%n", status, hasBalance ? Long.toString(balance) : "n/a");
        };

        final BatchResultHandler batchHandler = (batchIdHi, batchIdLo, results) -> {
            lastBatchIdLo[0] = batchIdLo;
            for (int i = 0; i < results.length; i++) {
                final TransferLegResult r = results[i];
                System.out.printf(
                        "  <- leg %d: status=%s balance=%s%n",
                        i, r.status(), r.hasBalance() ? Long.toString(r.resultBalance()) : "n/a");
            }
        };

        final ClientConfig config = ClientConfig.builder(CLIENT_ID, ClusterConfig.ingressEndpoints(1))
                .build();

        try (WriteClient client = new WriteClient(config, commandHandler)) {
            client.setBatchResultHandler(batchHandler);
            System.out.println("Connected client " + CLIENT_ID + " to the cluster.");

            System.out.println("-> CREDIT account 100 with 500");
            awaitCommand(client, client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L), lastCommandIdLo);

            System.out.println("-> CREDIT account 300 with 300");
            awaitCommand(client, client.submit(CommandType.CREDIT, 300L, 0L, 0L, 300L), lastCommandIdLo);

            System.out.println("-> BATCH [100->200 100, 100->201 50] (independent legs)");
            awaitBatch(
                    client,
                    new TransferLeg[] {
                        new TransferLeg(100L, 200L, 100L, 0L, false), new TransferLeg(100L, 201L, 50L, 0L, false),
                    },
                    lastBatchIdLo);

            System.out.println("-> BATCH [100->200 30 (linked), 300->400 20] (atomic chain, succeeds)");
            awaitBatch(
                    client,
                    new TransferLeg[] {
                        new TransferLeg(100L, 200L, 30L, 0L, true), new TransferLeg(300L, 400L, 20L, 0L, false),
                    },
                    lastBatchIdLo);

            System.out.println("-> BATCH [100->999 10000 (linked), 300->400 20] (fails, rolls back)");
            awaitBatch(
                    client,
                    new TransferLeg[] {
                        new TransferLeg(100L, 999L, 10_000L, 0L, true), new TransferLeg(300L, 400L, 20L, 0L, false),
                    },
                    lastBatchIdLo);

            System.out.println("Done. The failed chain returned the same error status for every leg.");
        } finally {
            node.close();
            deleteRecursively(baseDir);
        }
    }

    /** Polls the client until the result for {@code commandIdLo} arrives or timeout. */
    private static void awaitCommand(final WriteClient client, final long commandIdLo, final long[] lastCommandIdLo) {
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

    /** Submits a batch and polls until its result arrives or timeout. */
    private static void awaitBatch(final WriteClient client, final TransferLeg[] legs, final long[] lastBatchIdLo) {
        final long batchIdLo = client.submitTransferBatch(legs);
        final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastBatchIdLo[0] == batchIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("no result for batchIdLo=" + batchIdLo + " within timeout");
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
