package com.adbe.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.launcher.EventJournaler;
import com.adbe.protocol.BalanceChangedEventDecoder;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.EventCause;
import com.adbe.protocol.MessageHeaderDecoder;
import com.adbe.protocol.StatusCode;
import com.adbe.protocol.TransferEventDecoder;
import com.adbe.testkit.ClusterTestClient;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for the domain event journal (ADR 0011): a journaling cluster
 * applies commands, and the emitted semantic events are recorded to the member
 * Archive on stream {@link EventJournaler#STREAM_ID} and replay in order.
 */
@Tag("integration")
class EventJournalIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long REPLAY_TIMEOUT_MS = 20_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";
    private static final int REPLAY_STREAM_ID = 47;

    @Test
    @Timeout(120)
    void recordsAndReplaysDomainEvents(@TempDir final Path tempDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        final CoreConfig coreConfig = CoreConfig.defaults().withEventJournal(CoreConfig.DEFAULT_EVENT_JOURNAL_CAPACITY);

        try (ClusterNode node = new ClusterNode(clusterConfig, coreConfig, true)) {
            try (ClusterTestClient client =
                    new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {
                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                client.send(1L, 1L, 0L, 2L, CommandType.TRANSFER, 100L, 200L, 0L, 150L);
                assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "transfer result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());
            }

            final CollectedEvents events = replayEvents(clusterConfig);

            // The credit produced one balance change to 500 on account 100.
            assertTrue(
                    events.balanceChanges.stream()
                            .anyMatch(b -> b.account == 100L && b.newBalance == 500L && b.cause == EventCause.CREDIT),
                    "expected a CREDIT balance-changed event for account 100 -> 500, saw: " + events.balanceChanges);

            // The transfer produced a graph edge 100 -> 200 for 150.
            assertTrue(
                    events.transfers.stream().anyMatch(t -> t.from == 100L && t.to == 200L && t.amount == 150L),
                    "expected a transfer edge 100 -> 200 amount 150, saw: " + events.transfers);

            // The transfer's paired balance changes net to zero.
            final long transferDebit = events.balanceChanges.stream()
                    .filter(b -> b.cause == EventCause.TRANSFER_DEBIT)
                    .mapToLong(b -> b.delta)
                    .sum();
            final long transferCredit = events.balanceChanges.stream()
                    .filter(b -> b.cause == EventCause.TRANSFER_CREDIT)
                    .mapToLong(b -> b.delta)
                    .sum();
            assertEquals(0L, transferDebit + transferCredit, "paired transfer deltas must cancel");
        }
    }

    private CollectedEvents replayEvents(final ClusterConfig clusterConfig) {
        final CollectedEvents collected = new CollectedEvents();
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final BalanceChangedEventDecoder balanceDecoder = new BalanceChangedEventDecoder();
        final TransferEventDecoder transferDecoder = new TransferEventDecoder();

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(clusterConfig.aeronDirectoryName()));
                AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                        .aeron(aeron)
                        .ownsAeronClient(false)
                        .controlRequestChannel(clusterConfig.archiveControlChannel())
                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {

            final long recordingId = findEventRecording(archive);
            assertTrue(recordingId >= 0, "event journal recording (stream 108) must exist");

            final Subscription subscription = aeron.addSubscription("aeron:ipc", REPLAY_STREAM_ID);
            archive.startReplay(recordingId, 0L, AeronArchive.NULL_LENGTH, "aeron:ipc", REPLAY_STREAM_ID);

            final FragmentHandler handler = (buffer, offset, length, fragmentHeader) -> {
                header.wrap(buffer, offset);
                final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
                if (header.templateId() == BalanceChangedEventDecoder.TEMPLATE_ID) {
                    balanceDecoder.wrap(buffer, bodyOffset, header.blockLength(), header.version());
                    collected.balanceChanges.add(new BalanceChange(
                            balanceDecoder.accountId(),
                            balanceDecoder.newBalance(),
                            balanceDecoder.delta(),
                            balanceDecoder.cause()));
                } else if (header.templateId() == TransferEventDecoder.TEMPLATE_ID) {
                    transferDecoder.wrap(buffer, bodyOffset, header.blockLength(), header.version());
                    collected.transfers.add(new Transfer(
                            transferDecoder.fromAccount(), transferDecoder.toAccount(), transferDecoder.amount()));
                }
            };

            final long deadline = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                subscription.poll(handler, 32);
                if (!collected.balanceChanges.isEmpty() && !collected.transfers.isEmpty()) {
                    break;
                }
                Thread.onSpinWait();
            }
            subscription.close();
        }
        return collected;
    }

    private long findEventRecording(final AeronArchive archive) {
        final long[] latest = {-1L};
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == EventJournaler.STREAM_ID && recordingId > latest[0]) {
                        latest[0] = recordingId;
                    }
                };
        archive.listRecordings(0L, 100, consumer);
        return latest[0];
    }

    private static final class CollectedEvents {
        private final List<BalanceChange> balanceChanges = new ArrayList<>();
        private final List<Transfer> transfers = new ArrayList<>();
    }

    private record BalanceChange(long account, long newBalance, long delta, EventCause cause) {}

    private record Transfer(long from, long to, long amount) {}
}
