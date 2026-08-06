package com.adbe.bench.store;

import com.adbe.bench.Op;
import com.adbe.client.AdbeClient;
import com.adbe.client.BackpressureException;
import com.adbe.client.ResultHandler;
import com.adbe.client.config.ClientConfig;
import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;

/**
 * ADBE backend: drives an Aeron cluster through {@link AdbeClient}. By default it
 * boots an in-process three-node cluster on localhost, so the measurement includes
 * the full Raft replication and Archive-durability cost; set {@code externalEndpoints}
 * to instead drive an already-running cluster (for example the docker-compose stack).
 *
 * <p>Submission is asynchronous and pipelined up to {@code maxInFlight}; the client
 * records end-to-end submit-to-result latency in its own histogram, which this store
 * exposes directly.
 */
public final class AdbeDataStore implements DataStore {

    private static final int NODE_COUNT = 3;
    private static final long CLIENT_ID = 1L;
    private static final long SUBMIT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(60);
    private static final long DRAIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(120);

    private final ClusterNode[] nodes;
    private final Path clusterDir;
    private final AdbeClient client;

    public AdbeDataStore(final String externalEndpoints, final int maxInFlight) {
        final String ingressEndpoints;
        if (externalEndpoints != null && !externalEndpoints.isBlank()) {
            this.nodes = new ClusterNode[0];
            this.clusterDir = null;
            ingressEndpoints = externalEndpoints;
        } else {
            this.clusterDir = createTempDir();
            final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODE_COUNT, clusterDir);
            this.nodes = new ClusterNode[NODE_COUNT];
            for (int i = 0; i < NODE_COUNT; i++) {
                nodes[i] = new ClusterNode(configs[i], CoreConfig.defaults(), true);
            }
            ingressEndpoints = ClusterConfig.ingressEndpoints(NODE_COUNT);
        }

        final ResultHandler handler = (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {};
        final ClientConfig config = ClientConfig.builder(CLIENT_ID, ingressEndpoints)
                .maxInFlight(maxInFlight)
                .build();
        this.client = new AdbeClient(config, handler);
    }

    @Override
    public String name() {
        return "adbe";
    }

    @Override
    public void setup(final int accounts, final long initialBalance) {
        final Op[] seed = new Op[accounts];
        for (int i = 0; i < accounts; i++) {
            seed[i] = new Op(com.adbe.bench.OpType.CREDIT, i + 1L, 0L, initialBalance);
        }
        executeAll(seed);
    }

    @Override
    public void run(final Op[] ops) {
        executeAll(ops);
    }

    private void executeAll(final Op[] ops) {
        for (final Op op : ops) {
            submitWithBackpressure(op);
        }
        final long deadline = System.nanoTime() + DRAIN_TIMEOUT_NS;
        while (client.pendingCount() > 0) {
            client.poll();
            if (System.nanoTime() - deadline > 0) {
                throw new IllegalStateException("ADBE drain timed out with " + client.pendingCount() + " in flight");
            }
        }
    }

    private void submitWithBackpressure(final Op op) {
        final long deadline = System.nanoTime() + SUBMIT_TIMEOUT_NS;
        while (true) {
            try {
                submit(op);
                client.poll();
                return;
            } catch (final BackpressureException e) {
                client.poll();
                if (System.nanoTime() - deadline > 0) {
                    throw new IllegalStateException("ADBE submit timed out (cluster not accepting commands)", e);
                }
            }
        }
    }

    private void submit(final Op op) {
        switch (op.type()) {
            case CREDIT -> client.submit(CommandType.CREDIT, 0L, op.accountA(), 0L, 0L, op.amount());
            case DEBIT -> client.submit(CommandType.DEBIT, 0L, op.accountA(), 0L, 0L, op.amount());
            case TRANSFER -> client.submit(CommandType.TRANSFER, 0L, op.accountA(), op.accountB(), 0L, op.amount());
            default -> throw new IllegalArgumentException("unknown op type: " + op.type());
        }
    }

    @Override
    public Histogram latencyHistogram() {
        return client.latencyHistogram();
    }

    @Override
    public void resetLatency() {
        client.latencyHistogram().reset();
    }

    @Override
    public void verify(final long expectedSupply) {
        // The client cannot query balances (that is the read side), so the health
        // invariant is that every submitted command completed and none was dropped.
        if (client.expired() != 0) {
            throw new IllegalStateException("ADBE expired " + client.expired() + " commands (dropped work)");
        }
        if (client.completed() != client.submitted()) {
            throw new IllegalStateException("ADBE completed " + client.completed() + " of " + client.submitted());
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } finally {
            for (final ClusterNode node : nodes) {
                if (node != null) {
                    try {
                        node.close();
                    } catch (final RuntimeException ignored) {
                        // Best-effort teardown; keep closing the remaining nodes.
                    }
                }
            }
            if (clusterDir != null) {
                deleteRecursively(clusterDir);
            }
        }
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("adbe-bench-cluster-");
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to create cluster temp dir", e);
        }
    }

    private static void deleteRecursively(final Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException ignored) {
                    // Best-effort cleanup of the temp cluster directory.
                }
            });
        } catch (final IOException ignored) {
            // Best-effort cleanup; a stale temp dir is harmless.
        }
    }
}
