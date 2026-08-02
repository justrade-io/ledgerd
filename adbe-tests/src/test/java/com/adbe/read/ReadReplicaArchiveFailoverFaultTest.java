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
 * Acceptance test for the read-node HA design (future ADR 0008): when the
 * cluster member whose Archive the read replica follows (node 0) dies, the read
 * replica must fail over to a surviving member's Archive and keep converging on
 * the committed log.
 *
 * <p>EXPECTED TO FAIL against the current code. The read replica connects to a
 * single, statically configured Archive endpoint (node 0) once in its
 * constructor and never reconnects, so once node 0 dies it serves a frozen state
 * forever. This test is the proof of that gap and the acceptance bar for the
 * fix: it goes green once the read replica accepts multiple Archive endpoints
 * and fails over between them (at which point the {@link ReadReplicaConfig}
 * below is updated to pass all three member Archive channels).
 *
 * <p>The write cluster keeps quorum (2 of 3 survive), so writes still commit
 * after node 0 dies; only the read replica's data source is gone.
 *
 * <p>Tagged {@code fault}: timing-sensitive, run via the opt-in {@code faultTest}
 * task, never wired into {@code check}.
 */
@Tag("fault")
class ReadReplicaArchiveFailoverFaultTest {

    private static final int NODE_COUNT = 3;
    private static final long RESULT_TIMEOUT_MS = 30_000L;
    private static final long RETRY_WINDOW_MS = 5_000L;
    private static final long HTTP_AWAIT_MS = 30_000L;

    @Test
    @Timeout(240)
    void readReplicaFollowsClusterAfterArchiveSourceNodeDies(@TempDir final Path baseDir) throws Exception {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            // ClusterConfig.multiNodeLocalhost is a pure function of baseDir, so these
            // channels match the nodes MultiNodeCluster launched above.
            final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODE_COUNT, baseDir);

            // Read replica pinned to node 0's Archive: the current single-endpoint
            // behaviour. When ADR 0008 lands this becomes the list of all member
            // Archive endpoints so the node can fail over.
            final ReadReplicaConfig replicaConfig = ReadReplicaConfig.builder()
                    .archiveControlChannel(configs[0].archiveControlChannel())
                    .pollIntervalMs(250L)
                    .liveLogEnabled(true)
                    .build();
            final ReadServiceConfig readConfig =
                    ReadServiceConfig.builder().httpPort(0).build();

            try (ReadReplicaNode replica = new ReadReplicaNode(replicaConfig, CoreConfig.defaults(), readConfig)) {
                final HttpClient http = HttpClient.newHttpClient();
                final String baseUrl = "http://localhost:" + replica.httpPort();

                // Initial credit; the replica converges via node 0's Archive live log.
                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "initial credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());
                awaitHttp(http, baseUrl, "/supply", "\"totalSupply\":500");

                // Kill the Archive source node. Quorum survives (2 of 3).
                cluster.stopNode(0);

                // Commit more through the surviving members, retrying across any leader
                // change with the same command id (idempotent).
                boolean acked = false;
                final long deadline = System.currentTimeMillis() + RESULT_TIMEOUT_MS;
                while (!acked && System.currentTimeMillis() < deadline) {
                    client.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 100L, 0L, 0L, 250L);
                    acked = client.awaitResult(2L, RETRY_WINDOW_MS);
                }
                assertTrue(acked, "post-kill credit must be acknowledged by the surviving quorum");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                // ACCEPTANCE: the read replica must fail over to a surviving member's
                // Archive and converge on the new state. RED until ADR 0008 lands.
                awaitHttp(http, baseUrl, "/supply", "\"totalSupply\":750");
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
