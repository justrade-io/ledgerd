package io.justrade.ledgerd.read;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for a read-service process. Runs a read replica node: it connects
 * to the write cluster's Aeron Archive, follows the consensus log (from the
 * latest snapshot when one exists, otherwise from the start of the log), loads
 * snapshots as they appear, and serves reads over a plain Aeron query protocol
 * ({@code QueryRequest} / {@code QueryResponse}). The read replica node does NOT
 * appear in {@code clusterMembers} and does not vote or affect quorum. See ADR
 * 0006 and 0007.
 *
 * <p>Configuration is read from environment variables with sensible localhost
 * defaults:
 *
 * <pre>
 *   LEDGERD_ARCHIVE_CHANNELS comma-separated Archive control channels, one per
 *                         cluster member; the replica fails over across them
 *                         (ADR 0008). Falls back to LEDGERD_ARCHIVE_CHANNEL.
 *   LEDGERD_ARCHIVE_CHANNEL  single Archive control channel (default localhost:20104)
 *   LEDGERD_LOCAL_HOST       routable host for Archive call-backs (default localhost)
 *   LEDGERD_AERON_DIR        embedded media driver directory (default build/read-replica/driver)
 *   LEDGERD_QUERY_CHANNEL    query request channel      (default aeron:udp?endpoint=localhost:44000)
 *   LEDGERD_QUERY_STREAM_ID  query request stream id    (default 300)
 *   LEDGERD_SNAPSHOT_POLL_MS interval between snapshot polls (default 5000)
 *   LEDGERD_LIVE_LOG         follow the consensus log       (default true)
 * </pre>
 */
public final class ReadServiceLauncher {

    private static final System.Logger LOG = System.getLogger(ReadServiceLauncher.class.getName());

    private ReadServiceLauncher() {}

    public static void main(final String[] args) throws InterruptedException {
        final ReadReplicaConfig replicaConfig = resolveReadReplicaConfig();

        try (ReadReplicaNode node = new ReadReplicaNode(replicaConfig, CoreConfig.defaults())) {
            LOG.log(
                    System.Logger.Level.INFO,
                    "LEDGERD read replica service started: queryChannel={0} archiveChannels={1} liveLog={2} healthy={3}",
                    replicaConfig.queryRequestChannel(),
                    replicaConfig.archiveControlChannels(),
                    replicaConfig.liveLogEnabled(),
                    node.isHealthy());
            parkUntilShutdown("ledgerd-read-replica-shutdown");
        }
    }

    private static ReadReplicaConfig resolveReadReplicaConfig() {
        final ReadReplicaConfig.Builder builder = ReadReplicaConfig.builder();

        // Prefer the multi-endpoint list (ADR 0008); fall back to the legacy
        // single channel so existing deployments keep working unchanged.
        final String archiveChannels = System.getenv("LEDGERD_ARCHIVE_CHANNELS");
        if (archiveChannels != null && !archiveChannels.isBlank()) {
            builder.archiveControlChannels(splitChannels(archiveChannels));
        } else {
            final String archiveChannel = System.getenv("LEDGERD_ARCHIVE_CHANNEL");
            if (archiveChannel != null && !archiveChannel.isBlank()) {
                builder.archiveControlChannel(archiveChannel);
            }
        }
        final String localHost = System.getenv("LEDGERD_LOCAL_HOST");
        if (localHost != null && !localHost.isBlank()) {
            builder.localHost(localHost);
        }
        final String aeronDir = System.getenv("LEDGERD_AERON_DIR");
        if (aeronDir != null && !aeronDir.isBlank()) {
            builder.aeronDir(aeronDir);
        }
        final String queryChannel = System.getenv("LEDGERD_QUERY_CHANNEL");
        if (queryChannel != null && !queryChannel.isBlank()) {
            builder.queryRequestChannel(queryChannel);
        }
        final String queryStreamId = System.getenv("LEDGERD_QUERY_STREAM_ID");
        if (queryStreamId != null && !queryStreamId.isBlank()) {
            builder.queryRequestStreamId(Integer.parseInt(queryStreamId));
        }
        final String pollInterval = System.getenv("LEDGERD_SNAPSHOT_POLL_MS");
        if (pollInterval != null && !pollInterval.isBlank()) {
            builder.pollIntervalMs(Long.parseLong(pollInterval));
        }
        final String liveLog = System.getenv("LEDGERD_LIVE_LOG");
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

    /** Splits a comma-separated channel list, trimming whitespace and dropping blanks. */
    private static java.util.List<String> splitChannels(final String value) {
        final java.util.List<String> channels = new java.util.ArrayList<>();
        for (final String channel : value.split(",")) {
            final String trimmed = channel.trim();
            if (!trimmed.isBlank()) {
                channels.add(trimmed);
            }
        }
        return channels;
    }
}
