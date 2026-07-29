package com.adbe.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.adbe.client.AdbeClient;
import com.adbe.client.ResultHandler;
import com.adbe.client.config.ClientConfig;
import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.protocol.CommandType;
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
 * End-to-end read-side test: writes via the Edge {@link AdbeClient} against a
 * single-node cluster hosting the {@link ReadModelService}, then reads back over
 * HTTP. Proves the read model is complete - it reflects both sides of a transfer,
 * unlike the egress stream which only carries the sender's balance.
 */
@Tag("integration")
class ReadServiceIntegrationTest {

    private static final long WRITE_TIMEOUT_MS = 15_000L;
    private static final long READ_TIMEOUT_MS = 15_000L;

    private ReadNode readNode;
    private HttpClient http;
    private String baseUrl;

    @BeforeEach
    void startNode(@TempDir final Path baseDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(0).build();
        readNode = new ReadNode(clusterConfig, CoreConfig.defaults(), readConfig, true);
        http = HttpClient.newHttpClient();
        baseUrl = "http://localhost:" + readNode.httpPort();
    }

    @AfterEach
    void stopNode() {
        if (readNode != null) {
            readNode.close();
        }
    }

    @Test
    @Timeout(90)
    void readsReflectBothSidesOfATransfer() {
        final long[] lastCommandIdLo = {-1L};
        final ResultHandler handler =
                (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> lastCommandIdLo[0] = idLo;

        final ClientConfig config =
                ClientConfig.builder(1L, ClusterConfig.ingressEndpoints(1)).build();

        try (AdbeClient client = new AdbeClient(config, handler)) {
            awaitWrite(client, client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L), lastCommandIdLo);
            awaitWrite(client, client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L), lastCommandIdLo);
            awaitWrite(client, client.submit(CommandType.APPROVE, 1L, 9L, 0L, 200L), lastCommandIdLo);

            // Sender lost 150 (500 -> 350); this side is visible on egress.
            awaitBody("/balance/100", "\"balance\":350");
            // Recipient gained 150 (0 -> 150); this side is NOT on egress, only the read model has it.
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
    }

    private void awaitWrite(final AdbeClient client, final long commandIdLo, final long[] lastCommandIdLo) {
        final long deadline = System.currentTimeMillis() + WRITE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            client.poll();
            if (lastCommandIdLo[0] == commandIdLo) {
                return;
            }
            Thread.onSpinWait();
        }
        fail("write not acknowledged for commandIdLo=" + commandIdLo);
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
