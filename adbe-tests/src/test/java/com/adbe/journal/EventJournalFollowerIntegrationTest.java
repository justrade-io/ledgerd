package com.adbe.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.EventCause;
import com.adbe.protocol.StatusCode;
import com.adbe.read.journal.DomainEventListener;
import com.adbe.read.journal.EventJournalConfig;
import com.adbe.read.journal.EventJournalFollower;
import com.adbe.testkit.ClusterTestClient;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for the Phase 2 event-journal consumer: an
 * {@link EventJournalFollower} follows a journaling cluster's Archive and
 * delivers decoded domain events to a listener, with idempotent replay.
 */
@Tag("integration")
class EventJournalFollowerIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long CONVERGE_TIMEOUT_MS = 25_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";

    @Test
    @Timeout(120)
    void followerReceivesDecodedEvents(@TempDir final Path tempDir) throws Exception {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        final CoreConfig coreConfig = CoreConfig.defaults().withEventJournal(CoreConfig.DEFAULT_EVENT_JOURNAL_CAPACITY);

        try (ClusterNode node = new ClusterNode(clusterConfig, coreConfig, true)) {
            final CollectingListener listener = new CollectingListener();
            final EventJournalConfig followerConfig = EventJournalConfig.builder()
                    .archiveControlChannel(clusterConfig.archiveControlChannel())
                    .aeronDir(tempDir.resolve("follower").toString())
                    .build();

            try (EventJournalFollower follower = new EventJournalFollower(followerConfig, listener);
                    ClusterTestClient client =
                            new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {

                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
                assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "transfer result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                // The follower must converge on the balances and the transfer edge.
                awaitConverged(listener);

                assertEquals(350L, listener.balances.get(100L), "sender balance after transfer");
                assertEquals(150L, listener.balances.get(200L), "recipient balance after transfer");
                assertEquals(1, listener.transfers.size(), "exactly one transfer edge (dedup holds)");
                final Transfer t = listener.transfers.get(0);
                assertEquals(100L, t.from);
                assertEquals(200L, t.to);
                assertEquals(150L, t.amount);
                assertTrue(listener.creditsSeen.get() >= 1, "at least one CREDIT balance change");
            }
        }
    }

    private void awaitConverged(final CollectingListener listener) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + CONVERGE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (listener.balances.containsKey(100L)
                    && listener.balances.get(100L) == 350L
                    && listener.balances.containsKey(200L)
                    && !listener.transfers.isEmpty()) {
                // Small settle window so any duplicate re-delivery would surface.
                Thread.sleep(500L);
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError(
                "follower did not converge: balances=" + listener.balances + " transfers=" + listener.transfers);
    }

    private static final class CollectingListener implements DomainEventListener {
        private final ConcurrentHashMap<Long, Long> balances = new ConcurrentHashMap<>();
        private final List<Transfer> transfers = new CopyOnWriteArrayList<>();
        private final AtomicInteger creditsSeen = new AtomicInteger();

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
            balances.put(accountId, newBalance);
            if (cause == EventCause.CREDIT) {
                creditsSeen.incrementAndGet();
            }
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
            transfers.add(new Transfer(fromAccount, toAccount, amount));
        }
    }

    private record Transfer(long from, long to, long amount) {}
}
