package io.justrade.ledgerd.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.journal.EventJournalConfig;
import io.justrade.ledgerd.read.journal.EventJournalFollower;
import io.justrade.ledgerd.risk.RiskScoringService;
import io.justrade.ledgerd.risk.RiskServiceConfig;
import io.justrade.ledgerd.risk.http.RiskHttpServer;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for the Phase 3 AI risk substrate (ADR 0012): a
 * {@link RiskScoringService} follows a journaling cluster's event stream, and the
 * {@link RiskHttpServer} dashboard exposes the resulting scores and money-flow
 * graph over HTTP.
 */
@Tag("integration")
class RiskServiceIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long CONVERGE_TIMEOUT_MS = 25_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    @Test
    @Timeout(120)
    void dashboardExposesScoresAndGraphFromEvents(@TempDir final Path tempDir) throws Exception {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        final CoreConfig coreConfig = CoreConfig.defaults().withEventJournal(CoreConfig.DEFAULT_EVENT_JOURNAL_CAPACITY);

        try (ClusterNode node = new ClusterNode(clusterConfig, coreConfig, true)) {
            final RiskScoringService service = new RiskScoringService();
            final EventJournalConfig followerConfig = EventJournalConfig.builder()
                    .archiveControlChannel(clusterConfig.archiveControlChannel())
                    .aeronDir(tempDir.resolve("follower").toString())
                    .build();

            try (EventJournalFollower follower = new EventJournalFollower(followerConfig, service);
                    RiskHttpServer http = new RiskHttpServer(
                            service, RiskServiceConfig.builder().httpPort(0).build(), adapt(follower));
                    ClusterTestClient client =
                            new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {

                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
                assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "transfer result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                awaitConverged(service);

                final int port = http.port();
                final HttpClient httpClient = HttpClient.newHttpClient();

                final String scores = get(httpClient, port, "/risk/scores");
                assertTrue(scores.contains("\"account\":100"), "scores expose the sender: " + scores);
                assertTrue(scores.contains("\"account\":200"), "scores expose the recipient: " + scores);

                final String graph = get(httpClient, port, "/risk/graph");
                assertTrue(
                        graph.contains("\"from\":100,\"to\":200,\"amount\":150"),
                        "graph exposes the transfer edge: " + graph);

                final String metrics = get(httpClient, port, "/metrics");
                assertTrue(metrics.contains("\"transfers\":1"), "metrics count the transfer: " + metrics);

                final HttpResponse<String> health = getResponse(httpClient, port, "/healthz");
                assertEquals(200, health.statusCode(), "follower is healthy");

                final String dashboard = get(httpClient, port, "/");
                assertTrue(dashboard.contains("LEDGERD Risk Substrate"), "dashboard HTML is served");
            }
        }
    }

    private static RiskHttpServer.FollowerHealth adapt(final EventJournalFollower follower) {
        return new RiskHttpServer.FollowerHealth() {
            @Override
            public boolean isHealthy() {
                return follower.isHealthy();
            }

            @Override
            public long failovers() {
                return follower.failovers();
            }

            @Override
            public long appliedPosition() {
                return follower.appliedPosition();
            }
        };
    }

    private void awaitConverged(final RiskScoringService service) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + CONVERGE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (service.transfers() >= 1 && service.risk(100L) != null && service.risk(200L) != null) {
                Thread.sleep(500L);
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("risk service did not converge: transfers=" + service.transfers() + " scoredAccounts="
                + service.scoredAccounts());
    }

    private static String get(final HttpClient client, final int port, final String path) throws Exception {
        return getResponse(client, port, path).body();
    }

    private static HttpResponse<String> getResponse(final HttpClient client, final int port, final String path)
            throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
