package com.adbe.bench;

import com.adbe.bench.store.AdbeDataStore;
import com.adbe.bench.store.DataStore;
import com.adbe.bench.store.PostgresDataStore;
import com.adbe.bench.store.RedisDataStore;
import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.Histogram;

/**
 * Runs the same wallet workload against each requested backend and reports
 * throughput and tail latency side by side. Postgres and Redis are provisioned via
 * Testcontainers (Docker required); ADBE boots an in-process three-node cluster
 * unless {@code ADBE_INGRESS_ENDPOINTS} points at an external one.
 *
 * <pre>{@code
 * ./gradlew :adbe-bench:run --args="--accounts=1000 --ops=50000 --warmup=10000"
 * }</pre>
 *
 * <p>Not part of the deterministic hot path: it uses the system clock, heap
 * allocation, threads, and blocking clients, all of which the core forbids. See ADR
 * 0013 for the methodology and fairness caveats.
 */
public final class BenchmarkHarness {

    private BenchmarkHarness() {}

    public static void main(final String[] args) {
        // docker-java defaults to API 1.32, which modern daemons reject (min 1.40);
        // pin a widely supported version unless the operator already chose one.
        if (System.getProperty("api.version") == null && System.getenv("DOCKER_API_VERSION") == null) {
            System.setProperty("api.version", "1.41");
        }

        final BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

        final Op[] warmup =
                WorkloadGenerator.generate(config.warmupOps(), config.accounts(), config.mix(), config.seed() + 1L);
        final Op[] measure =
                WorkloadGenerator.generate(config.measureOps(), config.accounts(), config.mix(), config.seed());

        final long expectedSupply =
                (long) config.accounts() * config.initialBalance() + netSupplyDelta(warmup) + netSupplyDelta(measure);

        System.out.printf(
                "ADBE datastore benchmark: accounts=%d warmup=%d ops=%d concurrency=%d mix(c/d/t)=%d/%d/%d seed=%d%n",
                config.accounts(),
                config.warmupOps(),
                config.measureOps(),
                config.concurrency(),
                config.mix()[0],
                config.mix()[1],
                config.mix()[2],
                config.seed());

        final List<BenchmarkResult> results = new ArrayList<>();
        for (final String backend : config.backends()) {
            results.add(runBackend(backend, config, warmup, measure, expectedSupply));
        }

        Reporter.printTable(results);
        Reporter.writeCsv(config.csvPath(), results);
    }

    private static BenchmarkResult runBackend(
            final String backend,
            final BenchmarkConfig config,
            final Op[] warmup,
            final Op[] measure,
            final long expectedSupply) {
        System.out.println("\n=== " + backend + " ===");
        try (DataStore store = create(backend, config)) {
            store.setup(config.accounts(), config.initialBalance());
            System.out.println(backend + ": setup complete, running warmup...");
            store.run(warmup);
            store.resetLatency();

            System.out.println(backend + ": measuring " + measure.length + " ops...");
            final long began = System.nanoTime();
            store.run(measure);
            final long elapsedNanos = System.nanoTime() - began;

            store.verify(expectedSupply);

            final Histogram h = store.latencyHistogram();
            final double throughput = measure.length / (elapsedNanos / 1_000_000_000.0);
            System.out.printf("%s: done (%.0f ops/s)%n", backend, throughput);
            return BenchmarkResult.ok(
                    backend,
                    measure.length,
                    throughput,
                    h.getValueAtPercentile(50.0),
                    h.getValueAtPercentile(99.0),
                    h.getValueAtPercentile(99.9),
                    h.getMaxValue());
        } catch (final Exception e) {
            System.err.println(backend + ": FAILED - " + e.getMessage());
            return BenchmarkResult.failed(backend, e.getMessage());
        }
    }

    private static DataStore create(final String backend, final BenchmarkConfig config) {
        return switch (backend) {
            case "adbe" -> new AdbeDataStore(config.adbeEndpoints(), config.concurrency());
            case "postgres" -> new PostgresDataStore(config.concurrency());
            case "redis" -> new RedisDataStore(config.concurrency());
            default -> throw new IllegalArgumentException("unknown backend: " + backend);
        };
    }

    private static long netSupplyDelta(final Op[] ops) {
        long delta = 0L;
        for (final Op op : ops) {
            delta += op.supplyDelta();
        }
        return delta;
    }
}
