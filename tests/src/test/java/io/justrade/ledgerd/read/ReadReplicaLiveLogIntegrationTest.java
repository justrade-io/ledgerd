package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import io.justrade.ledgerd.read.config.ReadServiceConfig;
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
 * Integration test: starts a write cluster, then a read replica node with
 * live log following enabled. Verifies the read replica node starts and serves
 * HTTP reads, even when the live log subscriber cannot find a consensus
 * recording (graceful degradation).
 */
@Tag("integration")
class ReadReplicaLiveLogIntegrationTest {

    private ClusterNode clusterNode;
    private ReadReplicaNode replicaNode;
    private HttpClient http;
    private String baseUrl;

    @BeforeEach
    void start(@TempDir final Path tempDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);

        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .liveLogEnabled(true)
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
        assertNotNull(replicaNode.gateway());
        assertTrue(replicaNode.httpPort() > 0);
    }
}
