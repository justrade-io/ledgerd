package io.justrade.ledgerd.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke test for the launcher bootstrap path (M2). Unlike the cluster integration
 * test, which only checks egress results, this exercises the launcher-specific
 * wiring that {@link ClusterNode} assembles: the clustered media driver, archive,
 * consensus module, and service container must come up; a command must flow
 * through consensus; the off-heap counter mirror the launcher stands up must
 * reflect that work; the metrics endpoint must serve those counters; and the
 * whole stack must shut down cleanly.
 */
@Tag("integration")
class LauncherBootstrapSmokeTest {

    private static final long TIMEOUT_MS = 15_000L;

    @Test
    @Timeout(60)
    void nodeBootsProcessesCommandMirrorsCountersAndServesMetrics(@TempDir final Path baseDir) throws Exception {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {
            try (ClusterTestClient client =
                    new ClusterTestClient(clusterConfig.aeronDirectoryName(), "0=localhost:20100")) {
                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, TIMEOUT_MS), "credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());
                assertEquals(500L, client.lastBalance());
            }

            // The launcher mirrors CoreMetrics into the off-heap counters manager;
            // the applied command must be visible there for cross-thread readers.
            final long commandsProcessed = counterValue(node.countersManager(), "ledgerd.commands_processed");
            assertTrue(commandsProcessed >= 1L, "commands_processed mirrored, was " + commandsProcessed);

            try (MetricsHttpServer server = new MetricsHttpServer(node.countersManager(), 0)) {
                final HttpClient http = HttpClient.newHttpClient();

                final HttpResponse<String> health = http.send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/healthz"))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(200, health.statusCode());
                assertEquals("ok\n", health.body());

                final HttpResponse<String> metrics = http.send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/metrics"))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(200, metrics.statusCode());
                assertTrue(metrics.body().contains("ledgerd_commands_processed"), metrics.body());
            }
        }
    }

    private static long counterValue(final CountersReader reader, final String label) {
        final int maxId = reader.maxCounterId();
        for (int id = 0; id <= maxId; id++) {
            if (reader.getCounterState(id) == CountersReader.RECORD_ALLOCATED
                    && label.equals(reader.getCounterLabel(id))) {
                return reader.getCounterValue(id);
            }
        }
        throw new AssertionError("counter not found: " + label);
    }
}
