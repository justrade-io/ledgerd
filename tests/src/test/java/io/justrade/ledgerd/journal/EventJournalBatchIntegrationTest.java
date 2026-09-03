package io.justrade.ledgerd.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import io.justrade.ledgerd.launcher.EventJournaler;
import io.justrade.ledgerd.protocol.BalanceChangedEventDecoder;
import io.justrade.ledgerd.protocol.CommandRejectedEventDecoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.protocol.TransferEventDecoder;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for the domain event journal (ADR 0011) with transfer batches
 * (ADR 0012): committed legs emit their transfer edges and balance changes,
 * while a rolled-back chain emits a rejection event per leg and no transfer
 * edges.
 */
@Tag("integration")
class EventJournalBatchIntegrationTest {

    private static final long RESULT_TIMEOUT_MS = 15_000L;
    private static final long REPLAY_TIMEOUT_MS = 20_000L;
    private static final String INGRESS_ENDPOINTS = "0=localhost:20100";
    private static final int REPLAY_STREAM_ID = 47;

    @Test
    @Timeout(120)
    void recordsCommittedLegsAndRejectsRolledBackChains(@TempDir final Path tempDir) {
        final ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, tempDir.resolve("write"));
        final CoreConfig coreConfig = CoreConfig.defaults().withEventJournal(CoreConfig.DEFAULT_EVENT_JOURNAL_CAPACITY);

        try (ClusterNode node = new ClusterNode(clusterConfig, coreConfig, true)) {
            try (ClusterTestClient client =
                    new ClusterTestClient(clusterConfig.aeronDirectoryName(), INGRESS_ENDPOINTS)) {

                client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
                assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit result");
                assertEquals(StatusCode.SUCCESS, client.lastStatus());

                // Two independent legs: both commit and emit transfer edges.
                client.sendBatch(
                        1L,
                        1L,
                        0L,
                        2L,
                        new long[] {100L, 100L},
                        new long[] {200L, 300L},
                        new long[] {100L, 50L},
                        new long[] {0L, 0L},
                        new boolean[] {false, false});
                assertTrue(client.awaitBatchResult(2L, RESULT_TIMEOUT_MS), "batch result");
                assertEquals(2, client.lastBatchLegCount());
                assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(0));
                assertEquals(StatusCode.SUCCESS, client.lastBatchStatus(1));

                // A linked chain that fails: both legs roll back and emit rejections.
                client.sendBatch(
                        1L,
                        2L,
                        0L,
                        3L,
                        new long[] {100L, 100L},
                        new long[] {999L, 888L},
                        new long[] {9_999L, 10L},
                        new long[] {0L, 0L},
                        new boolean[] {true, false});
                assertTrue(client.awaitBatchResult(3L, RESULT_TIMEOUT_MS), "failed batch result");
                assertEquals(StatusCode.INSUFFICIENT_BALANCE, client.lastBatchStatus(0));
                assertEquals(StatusCode.INSUFFICIENT_BALANCE, client.lastBatchStatus(1));
            }

            final CollectedEvents events = replayEvents(clusterConfig);

            // Committed legs emit exactly two transfer edges (100->200, 100->300).
            assertEquals(2, events.transfers.size(), "two committed legs -> two transfer edges");
            assertTrue(
                    events.transfers.stream().anyMatch(t -> t.from == 100L && t.to == 200L && t.amount == 100L),
                    "expected edge 100->200 amount 100, saw: " + events.transfers);
            assertTrue(
                    events.transfers.stream().anyMatch(t -> t.from == 100L && t.to == 300L && t.amount == 50L),
                    "expected edge 100->300 amount 50, saw: " + events.transfers);

            // The rolled-back chain emits one rejection per leg, with the real reason.
            assertEquals(2, events.rejections.size(), "two rolled-back legs -> two rejections");
            assertTrue(
                    events.rejections.stream().allMatch(r -> r.reason == StatusCode.INSUFFICIENT_BALANCE),
                    "rejections must carry the failing status, saw: " + events.rejections);

            // The committed transfers' paired balance changes net to zero.
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
        final CommandRejectedEventDecoder rejectedDecoder = new CommandRejectedEventDecoder();

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
                } else if (header.templateId() == CommandRejectedEventDecoder.TEMPLATE_ID) {
                    rejectedDecoder.wrap(buffer, bodyOffset, header.blockLength(), header.version());
                    collected.rejections.add(new Rejection(rejectedDecoder.accountId(), rejectedDecoder.reason()));
                }
            };

            final long deadline = System.currentTimeMillis() + REPLAY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                subscription.poll(handler, 32);
                if (collected.transfers.size() >= 2 && collected.rejections.size() >= 2) {
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
        private final List<Rejection> rejections = new ArrayList<>();
    }

    private record BalanceChange(long account, long newBalance, long delta, EventCause cause) {}

    private record Transfer(long from, long to, long amount) {}

    private record Rejection(long account, StatusCode reason) {}
}
