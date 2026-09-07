package io.justrade.ledgerd.read;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.justrade.ledgerd.telemetry.CounterSink;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.agrona.concurrent.status.CountersReader;

/**
 * A tiny HTTP endpoint for the read replica that exports the replicated core
 * counters in Prometheus text format at {@code /metrics}, plus a
 * replication-aware {@code /healthz} probe (ADR 0008).
 *
 * <p>The server runs on its own daemon thread and only reads counter values via
 * {@link CountersReader} (acquire ordering) and replication health from
 * {@link ReplicationHealth} (volatile fields). It never touches the
 * single-writer agent thread, honouring the rule that observability must not
 * perturb replication or query serving.
 */
public final class ReadMetricsHttpServer implements AutoCloseable {

    private static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final CountersReader reader;
    private final ReplicationHealth health;
    private final HttpServer server;
    private final ExecutorService executor;

    /**
     * Starts the server bound to {@code port} on all network interfaces
     * ({@code 0.0.0.0}), so a Prometheus scraper on another host can reach it.
     * A port of {@code 0} binds an ephemeral port, which is convenient for tests.
     */
    public ReadMetricsHttpServer(final CountersReader reader, final ReplicationHealth health, final int port) {
        this.reader = reader;
        this.health = health;
        try {
            this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to start read metrics HTTP server on port " + port, e);
        }
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/healthz", this::handleHealth);
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "ledgerd-read-metrics-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.start();
    }

    /** The actual port the server is listening on (useful when 0 was requested). */
    public int port() {
        return server.getAddress().getPort();
    }

    private void handleMetrics(final HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
            return;
        }
        respond(exchange, 200, scrape(), PROMETHEUS_CONTENT_TYPE);
    }

    private void handleHealth(final HttpExchange exchange) throws IOException {
        if (health.isHealthy()) {
            respond(exchange, 200, "ok\n", "text/plain; charset=utf-8");
            return;
        }
        final String body =
                "stale appliedPosition=" + health.appliedPosition() + " endpoint=" + health.activeEndpoint() + "\n";
        respond(exchange, 503, body, "text/plain; charset=utf-8");
    }

    private String scrape() {
        final StringBuilder sb = new StringBuilder(1024);
        final int maxCounterId = reader.maxCounterId();
        for (int counterId = 0; counterId <= maxCounterId; counterId++) {
            if (reader.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED) {
                continue;
            }
            final String metric = sanitize(reader.getCounterLabel(counterId));
            final long value = reader.getCounterValue(counterId);
            final String type = reader.getCounterTypeId(counterId) == CounterSink.TYPE_GAUGE ? "gauge" : "counter";
            sb.append("# TYPE ").append(metric).append(' ').append(type).append('\n');
            sb.append(metric).append(' ').append(value).append('\n');
        }

        // Replication health lives in ReplicationHealth (volatile fields), not in
        // the off-heap counters manager, so render it here as extra series.
        sb.append("# TYPE ledgerd_read_applied_position gauge\n")
                .append("ledgerd_read_applied_position ")
                .append(health.appliedPosition())
                .append('\n');
        sb.append("# TYPE ledgerd_read_failovers counter\n")
                .append("ledgerd_read_failovers ")
                .append(health.failovers())
                .append('\n');
        sb.append("# TYPE ledgerd_read_integrity_failures counter\n")
                .append("ledgerd_read_integrity_failures ")
                .append(health.integrityFailures())
                .append('\n');
        sb.append("# TYPE ledgerd_read_healthy gauge\n")
                .append("ledgerd_read_healthy ")
                .append(health.isHealthy() ? 1 : 0)
                .append('\n');
        return sb.toString();
    }

    private static String sanitize(final String label) {
        final StringBuilder sb = new StringBuilder(label.length());
        for (int i = 0; i < label.length(); i++) {
            final char c = label.charAt(i);
            final boolean valid =
                    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':';
            sb.append(valid ? c : '_');
        }
        return sb.toString();
    }

    private static void respond(
            final HttpExchange exchange, final int status, final String body, final String contentType)
            throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        // HttpServer.stop does not shut down a user-supplied executor.
        executor.shutdown();
    }
}
