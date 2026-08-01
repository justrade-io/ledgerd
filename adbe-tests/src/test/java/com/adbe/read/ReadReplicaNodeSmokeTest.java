package com.adbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.read.config.ReadReplicaConfig;
import com.adbe.read.config.ReadServiceConfig;
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
 * Integration test: starts a single-node write cluster (BalanceService),
 * then starts a read replica node that connects to the cluster's Archive.
 * Verifies the read replica HTTP server responds correctly even when no snapshot
 * has been taken yet (empty engine).
 */
@Tag("integration")
class ReadReplicaNodeSmokeTest {

    private ClusterNode clusterNode;
    private ReadReplicaNode replicaNode;
    private HttpClient http;
    private String baseUrl;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        // Start a single-node write cluster with BalanceService. The
        // ClusterNode brings up an embedded Media Driver + Archive that
        // the read replica node connects to for snapshot polling.
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .pollIntervalMs(60_000L)
                .build();
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(0).build();

        replicaNode = new ReadReplicaNode(replicaConfig, CoreConfig.defaults(), readConfig);
        http = HttpClient.newHttpClient();
        baseUrl = "http://localhost:" + replicaNode.httpPort();
    }

    @AfterEach
    void stop() {
        if (replicaNode != null) {
            replicaNode.close();
        }
        if (clusterNode != null) {
            clusterNode.close();
        }
    }

    @Test
    @Timeout(30)
    void healthzReturns200() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/healthz"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        final HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    @Timeout(30)
    void supplyReturnsZeroWhenNoSnapshotYet() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/supply"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        final HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(
                response.body().contains("\"totalSupply\":0"),
                "expected totalSupply=0 when no snapshot loaded, got: " + response.body());
    }

    @Test
    @Timeout(30)
    void balanceReturnsMissingAccount() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/balance/999"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        final HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"exists\":false"), "expected exists=false, got: " + response.body());
    }

    @Test
    @Timeout(30)
    void gatewayIsAccessible() {
        assertNotNull(replicaNode.gateway());
        assertTrue(replicaNode.httpPort() > 0);
    }
}
