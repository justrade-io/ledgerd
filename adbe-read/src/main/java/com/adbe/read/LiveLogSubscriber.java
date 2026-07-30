package com.adbe.read;

import com.adbe.core.BalanceEngine;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.CommandEnvelopeDecoder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;

/**
 * Subscribes to the consensus module log recording on the cluster Archive and
 * applies service messages (CommandEnvelope) to a {@link BalanceEngine} in near
 * real-time. This provides live log following between snapshots, reducing read
 * staleness from the snapshot interval to microseconds.
 *
 * <p>Each consensus log fragment starts with a cluster-schema
 * {@link MessageHeader}; when its templateId is
 * {@link SessionMessageHeaderDecoder#TEMPLATE_ID} the fragment contains a
 * wrapped service message. The subscriber skips the consensus framing (32 bytes)
 * and feeds the raw service message to the engine.
 *
 * <p>Thread safety: the subscriber runs on its own thread and calls
 * {@link BalanceEngine#process(CommandEnvelopeDecoder, CommandOutcome)} which
 * modifies the engine's single-writer stores. In a standby node this thread is
 * the sole writer, so no concurrency control is needed between replay and query
 * serving (the query drainer polls the same gateway on its own thread but only
 * reads the stores, never writes).
 */
final class LiveLogSubscriber implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final int CONSENSUS_FRAMING_LENGTH =
            io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;

    private final AeronArchive archive;
    private final BalanceEngine engine;
    private final long startPosition;
    private final io.aeron.cluster.codecs.MessageHeaderDecoder consensusHeader =
            new io.aeron.cluster.codecs.MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeader = new SessionMessageHeaderDecoder();
    private final com.adbe.protocol.MessageHeaderDecoder adbeHeader = new com.adbe.protocol.MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final CommandOutcome outcome = new CommandOutcome();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private Subscription subscription;
    private Thread thread;

    /**
     * @param archive       connected AeronArchive client for the cluster
     * @param engine        the balance engine to apply messages to
     * @param startPosition log position to start replaying from (typically the
     *                      position of the last loaded snapshot)
     */
    LiveLogSubscriber(final AeronArchive archive, final BalanceEngine engine, final long startPosition) {
        this.archive = archive;
        this.engine = engine;
        this.startPosition = startPosition;
    }

    /** Returns the consensus framing overhead in bytes (32). */
    static int consensusFramingLength() {
        return CONSENSUS_FRAMING_LENGTH;
    }

    void start() {
        final long recordingId = findConsensusRecording();
        if (recordingId < 0) {
            System.err.println("LiveLogSubscriber: no consensus recording found, live log following disabled");
            return;
        }

        final String replayChannel = "aeron:ipc?term-length=256k";
        final int replayStreamId = 43;
        final long sessionId = archive.startReplay(
                recordingId, startPosition, AeronArchive.NULL_LENGTH, replayChannel, replayStreamId);

        this.subscription = archive.context().aeron().addSubscription(replayChannel, replayStreamId);

        System.out.printf(
                "LiveLogSubscriber: following recording=%d from position=%d session=%d%n",
                recordingId, startPosition, sessionId);

        this.thread = new Thread(this::runLoop, "adbe-standby-livelog");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void close() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2000L);
            } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (subscription != null) {
            subscription.close();
        }
    }

    private void runLoop() {
        final FragmentHandler handler =
                (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
                        onFragment(buffer, offset);

        while (running.get()) {
            final int fragments = subscription.poll(handler, FRAGMENT_LIMIT);
            if (fragments == 0) {
                Thread.onSpinWait();
            }
        }
    }

    private void onFragment(final DirectBuffer buffer, final int offset) {
        consensusHeader.wrap(buffer, offset);
        if (consensusHeader.schemaId() != io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }

        if (consensusHeader.templateId() != SessionMessageHeaderDecoder.TEMPLATE_ID) {
            return; // consensus protocol message, not a service message
        }

        // Skip consensus framing to reach the service message.
        final int serviceOffset = offset + CONSENSUS_FRAMING_LENGTH;
        adbeHeader.wrap(buffer, serviceOffset);

        if (adbeHeader.templateId() != CommandEnvelopeDecoder.TEMPLATE_ID) {
            return; // not a command we process
        }

        envelopeDecoder.wrap(
                buffer,
                serviceOffset + com.adbe.protocol.MessageHeaderDecoder.ENCODED_LENGTH,
                adbeHeader.blockLength(),
                adbeHeader.version());

        engine.process(envelopeDecoder, outcome);
    }

    private long findConsensusRecording() {
        final long logStreamId = 100; // ConsensusModule.Configuration.LOG_STREAM_ID_DEFAULT
        final long[] latest = {-1L, -1L}; // recordingId, stopTimestamp

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
                    if (streamId == logStreamId && stopPosition > startPosition && stopTimestamp > latest[1]) {
                        latest[0] = recordingId;
                        latest[1] = stopTimestamp;
                    }
                };

        archive.listRecordings(0L, 200, consumer);
        return latest[0];
    }
}
