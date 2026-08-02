package com.adbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import com.adbe.read.config.ReadReplicaConfig;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.testkit.ClusterTestClient;
import io.aeron.cluster.ClusterTool;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end replication test for the read replica node. Unlike the smoke
 * tests, this drives real commands into the write cluster, forces a snapshot
 * via {@link ClusterTool}, and asserts the read replica downloads and serves that
 * state over HTTP - then keeps writing and asserts the live log converges
 * without another snapshot.
 *
 * <p>This exercises the code paths the smoke tests miss: snapshot load into the
 * engine (previously a startup NPE when a snapshot already existed), concurrent
 * query serving during replication (previously a data race), and repeated
 * snapshot polling without leaking subscribers or clobbering live-log state.
 */
@Tag("integration")
class ReadReplicaReplicationIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long HTTP_AWAIT_MS = 30_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    private ClusterNode clusterNode;
    private ClusterConfig clusterConfig;
    private ReadReplicaNode replicaNode;
    private HttpClient http;
    private String baseUrl;

    @AfterEach
    void stop() {
        if (replicaNode != null) {
            replicaNode.close();
        }
        if (clusterNode != null) {
            clusterNode.close();
        }
    }

    private void startCluster(final Path tempDir) {
        clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        clusterNode = new ClusterNode(clusterConfig, CoreConfig.defaults(), true);
    }

    private void startReplica() {
        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .pollIntervalMs(250L)
                .liveLogEnabled(true)
                .build();
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(0).build();
        replicaNode = new ReadReplicaNode(replicaConfig, CoreConfig.defaults(), readConfig);
        http = HttpClient.newHttpClient();
        baseUrl = "http://localhost:" + replicaNode.httpPort();
    }

    @Test
    @Timeout(120)
    void readReplicaLoadsSnapshotAndServesReplicatedState(@TempDir final Path tempDir) throws Exception {
        startCluster(tempDir);

        try (ClusterTestClient client = new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit 100 result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            client.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 200L, 0L, 0L, 300L);
            assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "credit 200 result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
        }

        // Force a snapshot so the read replica has a recording to download.
        assertTrue(ClusterTool.snapshot(clusterConfig.clusterDir(), System.out), "snapshot trigger accepted");

        startReplica();

        awaitHttp("/supply", "\"totalSupply\":800");
        awaitHttp("/balance/100", "\"balance\":500");
        awaitHttp("/balance/200", "\"balance\":300");
        awaitHttp("/balance/999", "\"exists\":false");
    }

    @Test
    @Timeout(120)
    void readReplicaFollowsLiveLogBetweenSnapshots(@TempDir final Path tempDir) throws Exception {
        startCluster(tempDir);

        try (ClusterTestClient client = new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
        }

        assertTrue(ClusterTool.snapshot(clusterConfig.clusterDir(), System.out), "snapshot trigger accepted");

        startReplica();
        awaitHttp("/supply", "\"totalSupply\":500");

        // A new command committed AFTER the snapshot must reach the read replica via
        // the live log, with no further snapshot taken.
        try (ClusterTestClient client = new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            client.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 100L, 0L, 0L, 250L);
            assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "post-snapshot credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
        }

        awaitHttp("/supply", "\"totalSupply\":750");
        awaitHttp("/balance/100", "\"balance\":750");
    }

    @Test
    @Timeout(120)
    void readReplicaLoadsLargeSnapshotWithLiveLogDisabled(@TempDir final Path tempDir) throws Exception {
        startCluster(tempDir);

        // Credit 100 accounts so the service snapshot (header + 100 balances + 100
        // dedup entries + footer, plus cluster-schema framing) exceeds the read
        // node's 64-fragment poll batch. Live log is DISABLED, so the replica can
        // only serve state loaded from the snapshot. A loader that mishandles the
        // cluster-schema framing prefix loads only the first batch and never
        // completes; a correct loader skips the framing and loads the whole
        // snapshot, converging on the full supply.
        final int accounts = 100;
        try (ClusterTestClient client = new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
            for (int i = 0; i < accounts; i++) {
                client.send(1L, i, 0L, i + 1L, CommandType.CREDIT, i + 1L, 0L, 0L, 1L);
            }
            assertTrue(client.awaitResult(accounts, RESULT_TIMEOUT_MS), "last credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
        }

        assertTrue(ClusterTool.snapshot(clusterConfig.clusterDir(), System.out), "snapshot trigger accepted");

        final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
                .pollIntervalMs(250L)
                .liveLogEnabled(false)
                .build();
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(0).build();
        replicaNode = new ReadReplicaNode(replicaConfig, CoreConfig.defaults(), readConfig);
        http = HttpClient.newHttpClient();
        baseUrl = "http://localhost:" + replicaNode.httpPort();

        awaitHttp("/supply", "\"totalSupply\":100");
        awaitHttp("/balance/100", "\"balance\":1");
    }

    private void awaitHttp(final String path, final String expectedFragment) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        final long deadline = System.currentTimeMillis() + HTTP_AWAIT_MS;
        String body = "";
        while (System.currentTimeMillis() < deadline) {
            final HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            body = response.body();
            if (response.statusCode() == 200 && body.contains(expectedFragment)) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError(
                "timed out waiting for " + path + " to contain '" + expectedFragment + "', last: " + body);
    }
}
