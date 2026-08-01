package com.adbe.read;

import com.adbe.config.CoreConfig;
import com.adbe.read.config.ReadReplicaConfig;
import com.adbe.read.config.ReadServiceConfig;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for a read-service process. Runs a read replica node: it connects
 * to the write cluster's Aeron Archive, follows the consensus log (from the
 * latest snapshot when one exists, otherwise from the start of the log), loads
 * snapshots as they appear, and serves reads over HTTP. The read replica node does
 * NOT appear in {@code clusterMembers} and does not vote or affect quorum. See
 * ADR 0006 and 0007.
 *
 * <p>Configuration is read from environment variables with sensible localhost
 * defaults:
 *
 * <pre>
 *   ADBE_ARCHIVE_CHANNEL  Archive control channel        (default localhost:20104)
 *   ADBE_LOCAL_HOST       routable host for Archive call-backs (default localhost)
 *   ADBE_HTTP_PORT        HTTP query port                (default 8080)
 *   ADBE_SNAPSHOT_POLL_MS interval between snapshot polls (default 5000)
 *   ADBE_LIVE_LOG         follow the consensus log       (default true)
 * </pre>
 */
public final class ReadServiceLauncher {

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws InterruptedException {
        final int httpPort = Integer.parseInt(envOrDefault("ADBE_HTTP_PORT", "8080"));
        final ReadServiceConfig readConfig =
                ReadServiceConfig.builder().httpPort(httpPort).build();
        final ReadReplicaConfig replicaConfig = resolveReadReplicaConfig();

        try (ReadReplicaNode node = new ReadReplicaNode(replicaConfig, CoreConfig.defaults(), readConfig)) {
            System.out.printf(
                    "ADBE read replica service started: httpPort=%d archiveChannel=%s liveLog=%s%n",
                    node.httpPort(), replicaConfig.archiveControlChannel(), replicaConfig.liveLogEnabled());
            parkUntilShutdown("adbe-read-replica-shutdown");
        }
    }

    private static ReadReplicaConfig resolveReadReplicaConfig() {
        final ReadReplicaConfig.Builder builder = ReadReplicaConfig.builder();

        final String archiveChannel = System.getenv("ADBE_ARCHIVE_CHANNEL");
        if (archiveChannel != null && !archiveChannel.isBlank()) {
            builder.archiveControlChannel(archiveChannel);
        }
        final String localHost = System.getenv("ADBE_LOCAL_HOST");
        if (localHost != null && !localHost.isBlank()) {
            builder.localHost(localHost);
        }
        final String pollInterval = System.getenv("ADBE_SNAPSHOT_POLL_MS");
        if (pollInterval != null && !pollInterval.isBlank()) {
            builder.pollIntervalMs(Long.parseLong(pollInterval));
        }
        final String liveLog = System.getenv("ADBE_LIVE_LOG");
        if (liveLog != null && !liveLog.isBlank()) {
            builder.liveLogEnabled(Boolean.parseBoolean(liveLog));
        }
        return builder.build();
    }

    private static void parkUntilShutdown(final String hookName) {
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown, hookName));
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String envOrDefault(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
