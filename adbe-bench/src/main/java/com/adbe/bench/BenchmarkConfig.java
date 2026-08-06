package com.adbe.bench;

import java.util.List;

/**
 * Parsed benchmark parameters. Populated from {@code --key=value} arguments with
 * sensible defaults; the ADBE ingress endpoints also fall back to the
 * {@code ADBE_INGRESS_ENDPOINTS} environment variable so the harness can drive an
 * external cluster instead of booting one in process.
 */
public final class BenchmarkConfig {

    private final int accounts;
    private final int warmupOps;
    private final int measureOps;
    private final int concurrency;
    private final long seed;
    private final long initialBalance;
    private final int[] mix;
    private final List<String> backends;
    private final String adbeEndpoints;
    private final String csvPath;

    private BenchmarkConfig(
            final int accounts,
            final int warmupOps,
            final int measureOps,
            final int concurrency,
            final long seed,
            final long initialBalance,
            final int[] mix,
            final List<String> backends,
            final String adbeEndpoints,
            final String csvPath) {
        this.accounts = accounts;
        this.warmupOps = warmupOps;
        this.measureOps = measureOps;
        this.concurrency = concurrency;
        this.seed = seed;
        this.initialBalance = initialBalance;
        this.mix = mix;
        this.backends = backends;
        this.adbeEndpoints = adbeEndpoints;
        this.csvPath = csvPath;
    }

    public static BenchmarkConfig fromArgs(final String[] args) {
        int accounts = 1000;
        int warmupOps = 10_000;
        int measureOps = 50_000;
        int concurrency = 64;
        long seed = 42L;
        long initialBalance = 1_000_000_000L;
        int[] mix = {40, 30, 30};
        List<String> backends = List.of("adbe", "postgres", "redis");
        String csvPath = "build/bench/results.csv";

        for (final String arg : args) {
            final String[] kv = split(arg);
            final String key = kv[0];
            final String value = kv[1];
            switch (key) {
                case "--accounts" -> accounts = Integer.parseInt(value);
                case "--warmup" -> warmupOps = Integer.parseInt(value);
                case "--ops" -> measureOps = Integer.parseInt(value);
                case "--concurrency" -> concurrency = Integer.parseInt(value);
                case "--seed" -> seed = Long.parseLong(value);
                case "--initial" -> initialBalance = Long.parseLong(value);
                case "--mix" -> mix = parseMix(value);
                case "--backends" -> backends = List.of(value.split(","));
                case "--csv" -> csvPath = value;
                default -> throw new IllegalArgumentException("unknown argument: " + key + " (see class Javadoc)");
            }
        }

        final String envEndpoints = System.getenv("ADBE_INGRESS_ENDPOINTS");
        final String adbeEndpoints = envEndpoints == null || envEndpoints.isBlank() ? null : envEndpoints;

        return new BenchmarkConfig(
                accounts,
                warmupOps,
                measureOps,
                concurrency,
                seed,
                initialBalance,
                mix,
                backends,
                adbeEndpoints,
                csvPath);
    }

    private static String[] split(final String arg) {
        final int eq = arg.indexOf('=');
        if (!arg.startsWith("--") || eq < 0) {
            throw new IllegalArgumentException("expected --key=value, got: " + arg);
        }
        return new String[] {arg.substring(0, eq), arg.substring(eq + 1)};
    }

    private static int[] parseMix(final String value) {
        final String[] parts = value.split(",");
        if (parts.length != 3) {
            throw new IllegalArgumentException("--mix must be credit,debit,transfer, got: " + value);
        }
        return new int[] {
            Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()), Integer.parseInt(parts[2].trim())
        };
    }

    public int accounts() {
        return accounts;
    }

    public int warmupOps() {
        return warmupOps;
    }

    public int measureOps() {
        return measureOps;
    }

    public int concurrency() {
        return concurrency;
    }

    public long seed() {
        return seed;
    }

    public long initialBalance() {
        return initialBalance;
    }

    public int[] mix() {
        return mix.clone();
    }

    public List<String> backends() {
        return backends;
    }

    public String adbeEndpoints() {
        return adbeEndpoints;
    }

    public String csvPath() {
        return csvPath;
    }
}
