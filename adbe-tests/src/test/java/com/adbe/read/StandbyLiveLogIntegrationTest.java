package com.adbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.config.StandbyConfig;
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
 * Integration test: starts a write cluster, then a standby read node with
 * live log following enabled. Verifies the standby node starts and serves
 * HTTP reads, even when the live log subscriber cannot find a consensus
 * recording (graceful degradation).
 */
@Tag("integration")
class StandbyLiveLogIntegrationTest {

    private ClusterNode clusterNode;
    private StandbyReadNode standbyNode;
    private HttpClient http;
    private String baseUrl;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

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
    @Timeout(30)
    void startsWithLiveLogEnabled() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/healthz"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        final HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    @Timeout(30)
    void gatewayIsAccessible() {
        assertNotNull(standbyNode.gateway());
        assertTrue(standbyNode.httpPort() > 0);
    }
}
