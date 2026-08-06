package com.adbe.risk;

import com.adbe.read.journal.EventJournalConfig;
import com.adbe.read.journal.EventJournalFollower;
import com.adbe.risk.http.RiskHttpServer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the AI risk service (ADR 0012). It follows the domain event
 * journal (ADR 0011) via an {@link EventJournalFollower}, feeds every decoded
 * event to a {@link RiskScoringService}, and serves the live risk dashboard over
 * HTTP. It is an Edge consumer: it never joins Raft and never affects the write
 * path.
 *
 * <p>Configuration is read from environment variables with localhost defaults,
 * mirroring the read service:
 *
 * <pre>
 *   ADBE_ARCHIVE_CHANNELS comma-separated Archive control channels, one per
 *                         cluster member; the follower fails over across them.
 *                         Falls back to ADBE_ARCHIVE_CHANNEL.
 *   ADBE_ARCHIVE_CHANNEL  single Archive control channel
 *   ADBE_LOCAL_HOST       routable host for Archive call-backs (default localhost)
 *   ADBE_AERON_DIR        embedded media driver directory
 *   ADBE_HTTP_PORT        dashboard HTTP port (default 8090)
 * </pre>
 */
public final class RiskServiceLauncher {

    private static final System.Logger LOG = System.getLogger(RiskServiceLauncher.class.getName());

    private RiskServiceLauncher() {}

    public static void main(final String[] args) {
        final List<String> channels = resolveChannels();
        if (channels.isEmpty()) {
            System.err.println("ADBE_ARCHIVE_CHANNELS (comma-separated) or ADBE_ARCHIVE_CHANNEL is required");
            System.exit(1);
            return;
        }

        final EventJournalConfig.Builder journalBuilder =
                EventJournalConfig.builder().archiveControlChannels(channels);
        final String localHost = System.getenv("ADBE_LOCAL_HOST");
        if (localHost != null && !localHost.isBlank()) {
            journalBuilder.localHost(localHost);
        }
        final String aeronDir = System.getenv("ADBE_AERON_DIR");
        if (aeronDir != null && !aeronDir.isBlank()) {
            journalBuilder.aeronDir(aeronDir);
        }

        final int httpPort = Integer.parseInt(envOrDefault("ADBE_HTTP_PORT", "8090"));
        final RiskServiceConfig serviceConfig =
                RiskServiceConfig.builder().httpPort(httpPort).build();

        final RiskScoringService service = new RiskScoringService();
        try (EventJournalFollower follower = new EventJournalFollower(journalBuilder.build(), service);
                RiskHttpServer http = new RiskHttpServer(service, serviceConfig, adapt(follower))) {
            LOG.log(
                    System.Logger.Level.INFO,
                    "ADBE risk service started: httpPort={0} archiveChannels={1}",
                    http.port(),
                    channels);
            parkUntilShutdown("adbe-risk-shutdown");
        }
    }

    private static RiskHttpServer.FollowerHealth adapt(final EventJournalFollower follower) {
        return new RiskHttpServer.FollowerHealth() {
            @Override
            public boolean isHealthy() {
                return follower.isHealthy();
            }

            @Override
            public long failovers() {
                return follower.failovers();
            }

            @Override
            public long appliedPosition() {
                return follower.appliedPosition();
            }
        };
    }

    private static List<String> resolveChannels() {
        final List<String> channels = new ArrayList<>();
        final String multi = System.getenv("ADBE_ARCHIVE_CHANNELS");
        if (multi != null && !multi.isBlank()) {
            for (final String channel : multi.split(",")) {
                final String trimmed = channel.trim();
                if (!trimmed.isBlank()) {
                    channels.add(trimmed);
                }
            }
        } else {
            final String single = System.getenv("ADBE_ARCHIVE_CHANNEL");
            if (single != null && !single.isBlank()) {
                channels.add(single.trim());
            }
        }
        return channels;
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
