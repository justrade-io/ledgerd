package com.adbe.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Verifies that {@link MetricsHttpServer} exposes off-heap counter values over
 * HTTP in Prometheus text format, reading them from another thread.
 */
class MetricsHttpServerTest {

    private static final int MAX_COUNTERS = 8;

    @Test
    @Timeout(30)
    void exportsCountersInPrometheusFormat() throws Exception {
        final UnsafeBuffer values = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                MAX_COUNTERS * CountersManager.COUNTER_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final UnsafeBuffer metadata = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                MAX_COUNTERS * CountersManager.METADATA_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final CountersManager countersManager = new CountersManager(metadata, values);

        final AtomicCounter commands = countersManager.newCounter("adbe.commands_processed");
        final AtomicCounter duplicates = countersManager.newCounter("adbe.duplicates_detected");
        final AtomicCounter balanceGauge = countersManager.newCounter("adbe.balance_count", 1);
        commands.setOrdered(42L);
        duplicates.setOrdered(3L);
        balanceGauge.setOrdered(7L);

        try (MetricsHttpServer server = new MetricsHttpServer(countersManager, 0)) {
            final HttpClient client = HttpClient.newHttpClient();

            final HttpResponse<String> metrics = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/metrics"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            assertEquals(200, metrics.statusCode());
            final String body = metrics.body();
            assertTrue(body.contains("adbe_commands_processed 42"), body);
            assertTrue(body.contains("adbe_duplicates_detected 3"), body);
            assertTrue(body.contains("# TYPE adbe_commands_processed counter"), body);
            assertTrue(body.contains("# TYPE adbe_balance_count gauge"), body);
            assertTrue(body.contains("adbe_balance_count 7"), body);

            final HttpResponse<String> health = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/healthz"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode());
            assertEquals("ok\n", health.body());
        }
    }
}
