package io.justrade.ledgerd.read.journal;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One-shot operational verifier for the domain event journal (ADR 0011). It runs
 * an {@link EventJournalFollower} against the cluster's Archives and waits until
 * it has observed at least {@code LEDGERD_EVENT_MIN} recorded events, printing a
 * machine-parseable result line and exiting 0 on success or 1 on timeout.
 *
 * <p>Used by {@code docker/verify-read.sh} to prove the journal is recorded and
 * consumable end to end in the compose deployment. Configuration mirrors the read
 * replica's environment.
 *
 * <pre>
 *   LEDGERD_ARCHIVE_CHANNELS  comma-separated Archive control channels (preferred)
 *   LEDGERD_ARCHIVE_CHANNEL   single Archive control channel (fallback)
 *   LEDGERD_LOCAL_HOST        routable host for Archive call-backs (default localhost)
 *   LEDGERD_AERON_DIR         embedded media driver directory
 *   LEDGERD_EVENT_MIN         minimum events to observe before success (default 1)
 *   LEDGERD_EVENT_TIMEOUT_MS  wait budget in milliseconds (default 30000)
 * </pre>
 */
public final class EventJournalVerifier {

    private EventJournalVerifier() {}

    public static void main(final String[] args) {
        final List<String> channels = resolveChannels();
        if (channels.isEmpty()) {
            System.err.println("LEDGERD_ARCHIVE_CHANNELS (comma-separated) or LEDGERD_ARCHIVE_CHANNEL is required");
            System.exit(1);
            return;
        }
        final long min = Long.parseLong(envOrDefault("LEDGERD_EVENT_MIN", "1"));
        final long timeoutMs = Long.parseLong(envOrDefault("LEDGERD_EVENT_TIMEOUT_MS", "30000"));

        final EventJournalConfig.Builder builder = EventJournalConfig.builder().archiveControlChannels(channels);
        final String localHost = System.getenv("LEDGERD_LOCAL_HOST");
        if (localHost != null && !localHost.isBlank()) {
            builder.localHost(localHost);
        }
        final String aeronDir = System.getenv("LEDGERD_AERON_DIR");
        if (aeronDir != null && !aeronDir.isBlank()) {
            builder.aeronDir(aeronDir);
        }

        final CountingListener listener = new CountingListener();
        try (EventJournalFollower follower = new EventJournalFollower(builder.build(), listener)) {
            final long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline && listener.total.get() < min) {
                sleep(100L);
            }
            final long observed = listener.total.get();
            if (observed >= min) {
                System.out.printf(
                        "EVENT JOURNAL VERIFIED: observed=%d balanceChanged=%d transfers=%d allowance=%d holds=%d"
                                + " appliedPosition=%d failovers=%d%n",
                        observed,
                        listener.balanceChanged.get(),
                        listener.transfers.get(),
                        listener.allowance.get(),
                        listener.holds.get(),
                        follower.appliedPosition(),
                        follower.failovers());
                System.exit(0);
            } else {
                System.out.printf(
                        "EVENT JOURNAL VERIFICATION FAILED: observed=%d < min=%d within %dms (failovers=%d)%n",
                        observed, min, timeoutMs, follower.failovers());
                System.exit(1);
            }
        }
    }

    private static List<String> resolveChannels() {
        final List<String> channels = new ArrayList<>();
        final String multi = System.getenv("LEDGERD_ARCHIVE_CHANNELS");
        if (multi != null && !multi.isBlank()) {
            for (final String channel : multi.split(",")) {
                final String trimmed = channel.trim();
                if (!trimmed.isBlank()) {
                    channels.add(trimmed);
                }
            }
        } else {
            final String single = System.getenv("LEDGERD_ARCHIVE_CHANNEL");
            if (single != null && !single.isBlank()) {
                channels.add(single.trim());
            }
        }
        return channels;
    }

    private static String envOrDefault(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class CountingListener implements DomainEventListener {
        private final AtomicLong total = new AtomicLong();
        private final AtomicLong balanceChanged = new AtomicLong();
        private final AtomicLong transfers = new AtomicLong();
        private final AtomicLong allowance = new AtomicLong();
        private final AtomicLong holds = new AtomicLong();

        @Override
        public void onBalanceChanged(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long accountId,
                final long newBalance,
                final long delta,
                final EventCause cause) {
            balanceChanged.incrementAndGet();
            total.incrementAndGet();
        }

        @Override
        public void onTransfer(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long fromAccount,
                final long toAccount,
                final long amount) {
            transfers.incrementAndGet();
            total.incrementAndGet();
        }

        @Override
        public void onAllowanceChanged(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long ownerId,
                final long delegateId,
                final long newAllowance) {
            allowance.incrementAndGet();
            total.incrementAndGet();
        }

        @Override
        public void onReserved(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long accountId,
                final long newAvailable,
                final long newReserved) {
            holds.incrementAndGet();
            total.incrementAndGet();
        }

        @Override
        public void onCaptured(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long accountId,
                final long newAvailable,
                final long newReserved) {
            holds.incrementAndGet();
            total.incrementAndGet();
        }

        @Override
        public void onReleased(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long accountId,
                final long newAvailable,
                final long newReserved) {
            holds.incrementAndGet();
            total.incrementAndGet();
        }

        @Override
        public void onCommandRejected(
                final long logPosition,
                final long timestamp,
                final int eventIndex,
                final long assetId,
                final long accountId,
                final long amount,
                final CommandType commandType,
                final StatusCode reason) {
            total.incrementAndGet();
        }
    }
}
