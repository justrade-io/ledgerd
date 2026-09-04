package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.read.ReadReplicaNode;
import io.justrade.ledgerd.read.client.AllowanceResult;
import io.justrade.ledgerd.read.client.BalanceResult;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.client.TotalSupplyResult;
import io.justrade.ledgerd.read.client.config.ReadClientConfig;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.write.client.ResultHandler;
import io.justrade.ledgerd.write.client.WriteClient;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * End-to-end CQRS quick start: boots a single-node LEDGERD write cluster and a
 * read replica in-process, submits commands through {@link WriteClient}, then
 * reads the results back through the read-client SDK ({@link ReadClient}).
 *
 * <p>This is the smallest thing a newcomer can run to see the read path work:
 *
 * <pre>{@code
 * ./gradlew :examples:run -PmainClass=io.justrade.ledgerd.examples.ReadClientExample
 * }</pre>
 *
 * <p>The read replica follows the cluster's committed log (no snapshot required
 * on a fresh cluster), so reads are eventually consistent: each await helper
 * polls until the replica converges. Reads see both sides of a transfer, unlike
 * the egress stream which only carries the sender's balance.
 *
 * <p>Deliberately not part of the deterministic hot path: it uses the system
 * clock for its await loops and prints to stdout, both of which the core
 * forbids.
 */
public final class ReadClientExample {

    private static final long CLIENT_ID = 1L;
    private static final long WRITE_TIMEOUT_MS = 15_000L;
    private static final long READ_TIMEOUT_MS = 30_000L;

    private ReadClientExample() {}

    public static void main(final String[] args) throws IOException {
        final Path baseDir = Files.createTempDirectory("ledgerd-read-example-");
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

        System.out.println("Starting single-node LEDGERD cluster in " + baseDir + " ...");
        final ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults());

        // The replica follows the cluster's Archive (localhost:20104 for a
        // single-node localhost cluster). Its own media driver lives in a
        // sibling temp directory so it never collides with the cluster's driver
        // or with a concurrently running :read:run service.
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel(clusterConfig.archiveControlChannel())
                .aeronDir(baseDir.resolve("read-replica-driver").toString())
                .build();

        System.out.println("Starting read replica following " + clusterConfig.archiveControlChannel() + " ...");
        final ReadReplicaNode replica = new ReadReplicaNode(replicaConfig, CoreConfig.defaults());

        final long[] lastCommandIdLo = {-1L};

        final ResultHandler handler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
            lastCommandIdLo[0] = idLo;
            System.out.printf(
                    "  <- write result: status=%s balance=%s%n", status, hasBalance ? Long.toString(balance) : "n/a");
        };

        final ClientConfig config = ClientConfig.builder(CLIENT_ID, ClusterConfig.ingressEndpoints(1))
                .build();

        try (WriteClient write = new WriteClient(config, handler);
                ReadClient read = new ReadClient(ReadClientConfig.builder().build())) {
            System.out.println("Connected write client " + CLIENT_ID + " and read client to the cluster.");

            System.out.println("-> CREDIT account 100 with 500");
            awaitWrite(write, write.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L), lastCommandIdLo);

            System.out.println("-> TRANSFER 150 from account 100 to account 200");
            awaitWrite(write, write.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L), lastCommandIdLo);

            System.out.println("-> APPROVE delegate 9 to spend 200 from owner 1");
            awaitWrite(write, write.submit(CommandType.APPROVE, 1L, 9L, 0L, 200L), lastCommandIdLo);

            System.out.println("Reading back through the read replica (eventually consistent):");

            final BalanceResult sender = awaitBalance(read, 0L, 100L, 350L);
            System.out.printf("  balance(account=100) = %d (sender lost 150)%n", sender.balance());

            final BalanceResult recipient = awaitBalance(read, 0L, 200L, 150L);
            System.out.printf(
                    "  balance(account=200) = %d (recipient gained 150; only the read model sees this side)%n",
                    recipient.balance());

            final TotalSupplyResult supply = awaitSupply(read, 0L, 500L);
            System.out.printf("  totalSupply(asset=0) = %d (conserved by the transfer)%n", supply.totalSupply());

            final AllowanceResult allowance = awaitAllowance(read, 0L, 1L, 9L, 200L);
            System.out.printf("  allowance(owner=1, delegate=9) = %d%n", allowance.allowance());

            final List<BalanceResult> batch = read.batchBalances(0L, 100L, 200L, 999L);
            for (final BalanceResult result : batch) {
                System.out.printf(
                        "  batch balance(account=%d) = %s%n",
                        result.accountId(), result.found() ? Long.toString(result.balance()) : "missing");
            }

            System.out.printf(
                    "Done. Read replica appliedPosition=%d (queries submitted=%d completed=%d).%n",
                    read.lastAppliedPosition(), read.submitted(), read.completed());
        } finally {
            replica.close();
            node.close();
            deleteRecursively(baseDir);
        }
    }

    /** Polls the write client until the result for {@code commandIdLo} arrives or timeout. */
    private static void awaitWrite(final WriteClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + WRITE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("no write result for commandIdLo=" + commandIdLo + " within timeout");
    }

    /** Polls the replica until the balance for {@code (assetId, accountId)} reaches {@code expected}. */
    private static BalanceResult awaitBalance(
            final ReadClient client, final long assetId, final long accountId, final long expected) {
        final long deadline = System.currentTimeMillis() + READ_TIMEOUT_MS;
        BalanceResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.balance(assetId, accountId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && last.found() && last.balance() == expected) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("balance(asset=" + assetId + ", account=" + accountId + ") never reached "
                + expected + ", last=" + last);
    }

    /** Polls the replica until the total supply for {@code assetId} reaches {@code expected}. */
    private static TotalSupplyResult awaitSupply(final ReadClient client, final long assetId, final long expected) {
        final long deadline = System.currentTimeMillis() + READ_TIMEOUT_MS;
        TotalSupplyResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.totalSupply(assetId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && last.totalSupply() == expected) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("supply(asset=" + assetId + ") never reached " + expected + ", last=" + last);
    }

    /**
     * Polls the replica until the allowance for {@code (assetId, ownerId, delegateId)} reaches
     * {@code expected}.
     */
    private static AllowanceResult awaitAllowance(
            final ReadClient client,
            final long assetId,
            final long ownerId,
            final long delegateId,
            final long expected) {
        final long deadline = System.currentTimeMillis() + READ_TIMEOUT_MS;
        AllowanceResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.allowance(assetId, ownerId, delegateId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && last.allowance() == expected) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("allowance(asset=" + assetId + ", owner=" + ownerId + ", delegate=" + delegateId
                + ") never reached " + expected + ", last=" + last);
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
