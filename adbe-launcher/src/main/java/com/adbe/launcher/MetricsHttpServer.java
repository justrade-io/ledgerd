package com.adbe.launcher;

import com.adbe.telemetry.CounterSink;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import org.agrona.concurrent.status.CountersReader;

/**
 * A tiny HTTP endpoint that exports the node's off-heap counters in Prometheus
 * text format at {@code /metrics}, plus a {@code /healthz} liveness probe.
 *
 * <p>The server runs on its own daemon thread and only reads counter values via
 * {@link CountersReader}, which observes the single-writer updates with acquire
 * ordering. It never touches the clustered-service hot path, honouring the rule
 * that observability must not perturb the deterministic state machine.
 */
public final class MetricsHttpServer implements AutoCloseable {

    private static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final CountersReader reader;
    private final HttpServer server;

    /**
     * Starts the server bound to {@code port} on all network interfaces
     * ({@code 0.0.0.0}), so a Prometheus scraper on another host or the Docker
     * host can reach it. A port of {@code 0} binds an ephemeral port, which is
     * convenient for tests. The endpoint serves only read-only counters and a
     * liveness probe.
     */
    public MetricsHttpServer(final CountersReader reader, final int port) {
        this.reader = reader;
        try {
            this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to start metrics HTTP server on port " + port, e);
        }
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/healthz", this::handleHealth);
        server.setExecutor(Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "adbe-metrics-http");
            thread.setDaemon(true);
            return thread;
        }));
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
        respond(exchange, 200, "ok\n", "text/plain; charset=utf-8");
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
    }
}
