package com.adbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.StatusCode;
import com.adbe.read.config.ReadReplicaConfig;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.testkit.ClusterTestClient;
import com.adbe.testkit.MultiNodeCluster;
import io.aeron.cluster.ClusterTool;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Multi-node regression test for the read replica's snapshot loader, exercising
 * the production (three-node) topology.
 *
 * <p>In a multi-node cluster the ADBE service snapshot recording (stream 106) is
 * prefixed with cluster-schema framing records before the ADBE
 * {@code SnapshotHeader}. The loader must skip that framing and then load the
 * whole snapshot. The snapshot is made large (100 accounts) so it spans more
 * than one 64-fragment poll batch: a loader that mishandles the framing loads
 * only the first batch and never completes, whereas a correct loader converges
 * on the full supply. With the live log disabled the replica can only serve
 * state from a loaded snapshot.
 *
 * <p>Tagged {@code cluster}: multi-node and timing-sensitive, run via the opt-in
 * {@code clusterTest} task, never wired into {@code check}.
 */
@Tag("cluster")
class ReadReplicaSnapshotLoadClusterTest {

    private static final int NODE_COUNT = 3;
    private static final int ACCOUNTS = 100;
    private static final long RESULT_TIMEOUT_MS = 30_000L;
    private static final long HTTP_AWAIT_MS = 30_000L;

    @Test
    @Timeout(180)
    void readReplicaLoadsLargeServiceSnapshotFromClusterArchive(@TempDir final Path baseDir) throws Exception {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODE_COUNT, baseDir);

            for (int i = 0; i < ACCOUNTS; i++) {
                client.send(1L, i, 0L, i + 1L, CommandType.CREDIT, i + 1L, 0L, 0L, 1L);
            }
            assertTrue(client.awaitResult(ACCOUNTS, RESULT_TIMEOUT_MS), "last credit result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known before triggering a snapshot");
            assertTrue(ClusterTool.snapshot(configs[leader].clusterDir(), System.out), "snapshot trigger accepted");

            // Live log DISABLED: the replica must build state purely from a service
            // snapshot loaded from a member's archive.
            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                    .archiveControlChannel(configs[leader].archiveControlChannel())
                    .pollIntervalMs(250L)
                    .liveLogEnabled(false)
                    .build();
            final ReadServiceConfig readConfig =
                    ReadServiceConfig.builder().httpPort(0).build();

            try (ReadReplicaNode replica = new ReadReplicaNode(replicaConfig, CoreConfig.defaults(), readConfig)) {
                final HttpClient http = HttpClient.newHttpClient();
                final String baseUrl = "http://localhost:" + replica.httpPort();
                awaitHttp(http, baseUrl, "/supply", "\"totalSupply\":100");
                awaitHttp(http, baseUrl, "/balance/100", "\"balance\":1");
            }
        }
    }

    private static void awaitHttp(
            final HttpClient http, final String baseUrl, final String path, final String expectedFragment)
            throws Exception {
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
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "timed out waiting for " + path + " to contain '" + expectedFragment + "', last: " + body);
    }
}
