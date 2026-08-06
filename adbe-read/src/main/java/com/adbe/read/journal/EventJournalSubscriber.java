package com.adbe.read.journal;

import com.adbe.pipeline.EventJournalStreams;
import com.adbe.protocol.AllowanceChangedEventDecoder;
import com.adbe.protocol.BalanceChangedEventDecoder;
import com.adbe.protocol.CapturedEventDecoder;
import com.adbe.protocol.CommandRejectedEventDecoder;
import com.adbe.protocol.MessageHeaderDecoder;
import com.adbe.protocol.ReleasedEventDecoder;
import com.adbe.protocol.ReservedEventDecoder;
import com.adbe.protocol.TransferEventDecoder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;

/**
 * Follows the recorded domain event journal (stream
 * {@link EventJournalStreams#STREAM_ID}) on one Archive and delivers decoded
 * events to a {@link DomainEventListener} (ADR 0011). Unlike the consensus log,
 * event records are published directly by the journaler, so a fragment is a bare
 * ADBE message ({@code MessageHeader} + event body) with no consensus framing.
 *
 * <p>De-duplication uses a {@code (logPosition, eventIndex)} high-water mark read
 * uniformly from every event's fixed prefix, so a re-followed prefix after a
 * reconnect or failover is idempotent. The mark is seeded from and exposed to the
 * owning follower, which carries it across a new subscriber on failover.
 *
 * <p>Single-writer: the follower's one agent thread calls {@link #connect()} and
 * {@link #poll(int)}; no concurrency control is required.
 */
final class EventJournalSubscriber implements AutoCloseable {

    /** Delivery stream for the event journal replay (distinct from read streams 42/43). */
    static final int EVENT_JOURNAL_REPLAY = 44;

    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;
    private static final int LOG_POSITION_OFFSET = 0;
    private static final int EVENT_INDEX_OFFSET = 16;

    private final AeronArchive archive;
    private final long startPosition;
    private final String localHost;
    private final DomainEventListener listener;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final BalanceChangedEventDecoder balanceDecoder = new BalanceChangedEventDecoder();
    private final ReservedEventDecoder reservedDecoder = new ReservedEventDecoder();
    private final CapturedEventDecoder capturedDecoder = new CapturedEventDecoder();
    private final ReleasedEventDecoder releasedDecoder = new ReleasedEventDecoder();
    private final TransferEventDecoder transferDecoder = new TransferEventDecoder();
    private final AllowanceChangedEventDecoder allowanceDecoder = new AllowanceChangedEventDecoder();
    private final CommandRejectedEventDecoder rejectedDecoder = new CommandRejectedEventDecoder();
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Subscription subscription;
    private long lastPosition;
    private boolean hadImage;

    // (logPosition, eventIndex) high-water mark for idempotent re-delivery.
    private long hwmLogPosition;
    private int hwmEventIndex;

    EventJournalSubscriber(
            final AeronArchive archive,
            final long startPosition,
            final String localHost,
            final DomainEventListener listener,
            final long hwmLogPosition,
            final int hwmEventIndex) {
        this.archive = archive;
        this.startPosition = startPosition;
        this.localHost = localHost;
        this.listener = listener;
        this.lastPosition = startPosition;
        this.hwmLogPosition = hwmLogPosition;
        this.hwmEventIndex = hwmEventIndex;
    }

    /**
     * Locates the event journal recording and starts a replay plus the
     * subscription {@link #poll(int)} drains. Returns {@code false} if no event
     * recording exists yet (a normal transient state on a fresh cluster).
     */
    boolean connect() {
        final long recordingId = findEventRecording();
        if (recordingId < 0) {
            return false;
        }
        final Subscription sub = archive.context()
                .aeron()
                .addSubscription("aeron:udp?endpoint=" + localHost + ":0", EVENT_JOURNAL_REPLAY);
        final String endpoint = awaitResolvedEndpoint(sub);
        if (endpoint == null) {
            sub.close();
            return false;
        }
        final String replayChannel = "aeron:udp?endpoint=" + endpoint;
        archive.startReplay(recordingId, startPosition, AeronArchive.NULL_LENGTH, replayChannel, EVENT_JOURNAL_REPLAY);
        this.subscription = sub;
        this.lastPosition = startPosition;
        return true;
    }

    /** Polls the replay and delivers up to {@code fragmentLimit} events. */
    int poll(final int fragmentLimit) {
        if (subscription == null) {
            return 0;
        }
        if (subscription.imageCount() > 0) {
            hadImage = true;
        }
        return subscription.poll(fragmentHandler, fragmentLimit);
    }

    /** Whether the bounded replay caught up to an idle recording and its image closed. */
    boolean isReplayEnded() {
        return subscription != null && hadImage && subscription.imageCount() == 0;
    }

    /** The Aeron log position consumed up to; the replay restart point on reconnect. */
    long lastPosition() {
        return lastPosition;
    }

    long hwmLogPosition() {
        return hwmLogPosition;
    }

    int hwmEventIndex() {
        return hwmEventIndex;
    }

    @Override
    public void close() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    private void onFragment(
            final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header hdr) {
        lastPosition = hdr.position();
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        header.wrap(buffer, offset);
        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }
        final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        // Peek the dedup key from the fixed event prefix without a typed decoder.
        final long logPosition = buffer.getLong(body + LOG_POSITION_OFFSET);
        final int eventIndex = buffer.getShort(body + EVENT_INDEX_OFFSET) & 0xFFFF;
        if (logPosition < hwmLogPosition || (logPosition == hwmLogPosition && eventIndex <= hwmEventIndex)) {
            return; // already delivered
        }
        hwmLogPosition = logPosition;
        hwmEventIndex = eventIndex;

        dispatch(buffer, body);
    }

    private void dispatch(final DirectBuffer buffer, final int body) {
        final int blockLength = header.blockLength();
        final int version = header.version();
        switch (header.templateId()) {
            case BalanceChangedEventDecoder.TEMPLATE_ID -> {
                balanceDecoder.wrap(buffer, body, blockLength, version);
                listener.onBalanceChanged(
                        balanceDecoder.logPosition(),
                        balanceDecoder.timestamp(),
                        balanceDecoder.eventIndex(),
                        balanceDecoder.assetId(),
                        balanceDecoder.accountId(),
                        balanceDecoder.newBalance(),
                        balanceDecoder.delta(),
                        balanceDecoder.cause());
            }
            case ReservedEventDecoder.TEMPLATE_ID -> {
                reservedDecoder.wrap(buffer, body, blockLength, version);
                listener.onReserved(
                        reservedDecoder.logPosition(),
                        reservedDecoder.timestamp(),
                        reservedDecoder.eventIndex(),
                        reservedDecoder.assetId(),
                        reservedDecoder.accountId(),
                        reservedDecoder.newAvailable(),
                        reservedDecoder.newReserved());
            }
            case CapturedEventDecoder.TEMPLATE_ID -> {
                capturedDecoder.wrap(buffer, body, blockLength, version);
                listener.onCaptured(
                        capturedDecoder.logPosition(),
                        capturedDecoder.timestamp(),
                        capturedDecoder.eventIndex(),
                        capturedDecoder.assetId(),
                        capturedDecoder.accountId(),
                        capturedDecoder.newAvailable(),
                        capturedDecoder.newReserved());
            }
            case ReleasedEventDecoder.TEMPLATE_ID -> {
                releasedDecoder.wrap(buffer, body, blockLength, version);
                listener.onReleased(
                        releasedDecoder.logPosition(),
                        releasedDecoder.timestamp(),
                        releasedDecoder.eventIndex(),
                        releasedDecoder.assetId(),
                        releasedDecoder.accountId(),
                        releasedDecoder.newAvailable(),
                        releasedDecoder.newReserved());
            }
            case TransferEventDecoder.TEMPLATE_ID -> {
                transferDecoder.wrap(buffer, body, blockLength, version);
                listener.onTransfer(
                        transferDecoder.logPosition(),
                        transferDecoder.timestamp(),
                        transferDecoder.eventIndex(),
                        transferDecoder.assetId(),
                        transferDecoder.fromAccount(),
                        transferDecoder.toAccount(),
                        transferDecoder.amount());
            }
            case AllowanceChangedEventDecoder.TEMPLATE_ID -> {
                allowanceDecoder.wrap(buffer, body, blockLength, version);
                listener.onAllowanceChanged(
                        allowanceDecoder.logPosition(),
                        allowanceDecoder.timestamp(),
                        allowanceDecoder.eventIndex(),
                        allowanceDecoder.assetId(),
                        allowanceDecoder.ownerId(),
                        allowanceDecoder.delegateId(),
                        allowanceDecoder.newAllowance());
            }
            case CommandRejectedEventDecoder.TEMPLATE_ID -> {
                rejectedDecoder.wrap(buffer, body, blockLength, version);
                listener.onCommandRejected(
                        rejectedDecoder.logPosition(),
                        rejectedDecoder.timestamp(),
                        rejectedDecoder.eventIndex(),
                        rejectedDecoder.assetId(),
                        rejectedDecoder.accountId(),
                        rejectedDecoder.amount(),
                        rejectedDecoder.commandType(),
                        rejectedDecoder.reason());
            }
            default -> {
                // Unknown template (a newer schema): ignore rather than fail.
            }
        }
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + RESOLVE_ENDPOINT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return endpoint;
            }
            Thread.onSpinWait();
        }
        return null;
    }

    private long findEventRecording() {
        final long[] latest = {-1L};
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPos,
                        stopPos,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == EventJournalStreams.STREAM_ID && recordingId > latest[0]) {
                        latest[0] = recordingId;
                    }
                };
        archive.listRecordings(0L, 200, consumer);
        return latest[0];
    }
}
