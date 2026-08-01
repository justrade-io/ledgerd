package com.adbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.config.StandbyConfig;
import com.adbe.testkit.ClusterTestClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end read-side test over the standby path: writes via a
 * {@link ClusterTestClient} against a single-node write cluster, then reads back
 * over HTTP from a {@link StandbyReadNode}. The standby follows the consensus
 * log from position 0 (no snapshot required), so the read model is complete - it
 * reflects both sides of a transfer, unlike the egress stream which only carries
 * the sender's balance. Also covers the HTTP boundary's rejection of malformed
 * requests, previously covered by the removed cluster-mode integration test.
 */
@Tag("integration")
class StandbyReadQueryIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long READ_TIMEOUT_MS = 30_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    private ClusterNode clusterNode;
    private ClusterConfig clusterConfig;
    private StandbyReadNode standbyNode;
    private HttpClient http;
    private String baseUrl;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

        // Live log following from position 0 means reads converge without any
        // snapshot; a long poll interval keeps snapshot loading out of the test.
        final StandbyConfig standbyConfig = StandbyConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .liveLogEnabled(true)
                .pollIntervalMs(60_000L)
                .build();
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(0).build();
        standbyNode = new StandbyReadNode(standbyConfig, CoreConfig.defaults(), readConfig);
        http = HttpClient.newHttpClient();
        baseUrl = "http://localhost:" + standbyNode.httpPort();
    }

    @AfterEach
    void stop() {
        if (standbyNode != null) {
            standbyNode.close();
        }
        if (clusterNode != null) {
            clusterNode.close();
        }
    }

    @Test
    @Timeout(90)
    void readsReflectCommittedWrites() {
        try (ClusterTestClient client = new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
            assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "transfer result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            client.send(1L, 2L, 0L, 3L, CommandType.APPROVE, 1L, 9L, 0L, 200L);
            assertTrue(client.awaitResult(3L, RESULT_TIMEOUT_MS), "approve result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
        }

        // Sender lost 150 (500 -> 350); this side is visible on egress.
        awaitBody("/balance/100", "\"balance\":350");
        // Recipient gained 150 (0 -> 150); only the read model has this side.
        awaitBody("/balance/200", "\"balance\":150");
        // Total supply is conserved by the transfer.
        awaitBody("/supply", "\"totalSupply\":500");
        // Allowance set by APPROVE.
        awaitBody("/allowance/1/9", "\"allowance\":200");

        // Batch: two known accounts plus one that does not exist.
        final String batch = awaitBody("POST", "/balances", "{\"ids\":[100,200,999]}", "\"account\":999");
        assertTrue(batch.contains("\"account\":100,\"exists\":true,\"balance\":350"), batch);
        assertTrue(batch.contains("\"account\":200,\"exists\":true,\"balance\":150"), batch);
        assertTrue(batch.contains("\"account\":999,\"exists\":false"), batch);
    }

    @Test
    @Timeout(60)
    void rejectsMalformedRequestsWithoutCrashing() {
        assertEquals(404, statusOf("GET", "/nope", null), "unknown GET route");
        assertEquals(404, statusOf("POST", "/nope", "{}"), "unknown POST route");
        assertEquals(400, statusOf("GET", "/balance/not-a-number", null), "non-numeric balance id");
        assertEquals(400, statusOf("GET", "/allowance/1", null), "allowance missing delegate");
        assertEquals(400, statusOf("GET", "/allowance/1/", null), "allowance empty delegate");
        assertEquals(400, statusOf("GET", "/allowance/x/y", null), "allowance non-numeric");
        assertEquals(400, statusOf("POST", "/balances", "   "), "batch with no ids");

        // The node still serves valid queries after rejecting the malformed ones.
        assertEquals(200, statusOf("GET", "/healthz", null), "healthz after malformed requests");
    }

    private int statusOf(final String method, final String path, final String body) {
        try {
            final HttpRequest.Builder builder =
                    HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(5));
            if ("POST".equals(method)) {
                builder.header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
            } else {
                builder.GET();
            }
            return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
                    .statusCode();
        } catch (final Exception e) {
            throw new IllegalStateException("HTTP " + method + " " + path + " failed", e);
        }
    }

    private String awaitBody(final String path, final String mustContain) {
        return awaitBody("GET", path, null, mustContain);
    }

    private String awaitBody(final String method, final String path, final String body, final String mustContain) {
        final long deadline = System.currentTimeMillis() + READ_TIMEOUT_MS;
        String last = "";
        while (System.currentTimeMillis() < deadline) {
            last = request(method, path, body);
            if (last.contains(mustContain)) {
                return last;
            }
            try {
                Thread.sleep(50L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail("read of " + path + " never contained '" + mustContain + "'; last body=" + last);
        return last;
    }

    private String request(final String method, final String path, final String body) {
        try {
            final HttpRequest.Builder builder =
                    HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(5));
            if ("POST".equals(method)) {
                builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
            } else {
                builder.GET();
            }
            final HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), "unexpected status for " + path + ": " + response.body());
            return response.body();
        } catch (final Exception e) {
            throw new IllegalStateException("HTTP " + method + " " + path + " failed", e);
        }
    }
}
