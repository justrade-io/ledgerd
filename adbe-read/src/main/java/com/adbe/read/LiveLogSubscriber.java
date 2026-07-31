package com.adbe.read;

import com.adbe.core.BalanceEngine;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.CommandEnvelopeDecoder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;

/**
 * Subscribes to the consensus module log recording on the cluster Archive and
 * applies service messages (CommandEnvelope) to a {@link BalanceEngine} in near
 * real-time. This provides live log following between snapshots, reducing read
 * staleness from the snapshot interval to microseconds.
 *
 * <p>Each consensus log fragment starts with a cluster-schema
 * {@link io.aeron.cluster.codecs.MessageHeader}; when its templateId is
 * {@link SessionMessageHeaderDecoder#TEMPLATE_ID} the fragment contains a
 * wrapped service message. The subscriber skips the consensus framing and feeds
 * the raw service message to the engine.
 *
 * <p>Single-writer: this class owns no thread. The standby node's single agent
 * thread calls {@link #connect()} once and then drives {@link #poll(int)} from
 * its event loop, so {@link BalanceEngine#process} is only ever invoked from
 * that one thread - the same thread that serves queries. No concurrency control
 * is required.
 */
final class LiveLogSubscriber implements AutoCloseable {

    private static final int CONSENSUS_FRAMING_LENGTH =
            io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;

    private final AeronArchive archive;
    private final BalanceEngine engine;
    private final long startPosition;
    private final io.aeron.cluster.codecs.MessageHeaderDecoder consensusHeader =
            new io.aeron.cluster.codecs.MessageHeaderDecoder();
    private final com.adbe.protocol.MessageHeaderDecoder adbeHeader = new com.adbe.protocol.MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final CommandOutcome outcome = new CommandOutcome();
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Subscription subscription;

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

    /** Returns the consensus framing overhead in bytes. */
    static int consensusFramingLength() {
        return CONSENSUS_FRAMING_LENGTH;
    }

    /**
     * Locates the consensus recording and starts a bounded replay plus the
     * subscription that {@link #poll(int)} drains. Must be called on the agent
     * thread.
     *
     * @return {@code true} if a consensus recording was found and the replay was
     *     started; {@code false} if live log following is unavailable (the
     *     subscriber then does nothing on {@link #poll(int)}).
     */
    boolean connect() {
        final long recordingId = findConsensusRecording();
        if (recordingId < 0) {
            System.err.println("LiveLogSubscriber: no consensus recording found, live log following disabled");
            return false;
        }

        final int replayStreamId = 43;

        // The standby runs its own media driver, so the replay travels over UDP.
        // Bind an ephemeral-port subscription, resolve the port, then replay to it.
        final Subscription sub =
                archive.context().aeron().addSubscription("aeron:udp?endpoint=localhost:0", replayStreamId);
        final String endpoint = awaitResolvedEndpoint(sub);
        if (endpoint == null) {
            sub.close();
            System.err.println("LiveLogSubscriber: timed out resolving replay endpoint");
            return false;
        }

        final String replayChannel = "aeron:udp?endpoint=" + endpoint;
        final long sessionId = archive.startReplay(
                recordingId, startPosition, AeronArchive.NULL_LENGTH, replayChannel, replayStreamId);
        this.subscription = sub;

        System.out.printf(
                "LiveLogSubscriber: following recording=%d from position=%d session=%d%n",
                recordingId, startPosition, sessionId);
        return true;
    }

    /**
     * Polls the replay subscription and applies up to {@code fragmentLimit}
     * fragments to the engine. Must be called on the agent thread.
     *
     * @return the number of fragments consumed, or 0 if not connected / idle.
     */
    int poll(final int fragmentLimit) {
        if (subscription == null) {
            return 0;
        }
        return subscription.poll(fragmentHandler, fragmentLimit);
    }

    @Override
    public void close() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    private void onFragment(
            final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header header) {
        if (length < CONSENSUS_FRAMING_LENGTH) {
            return; // fragment too short to carry a service message
        }

        consensusHeader.wrap(buffer, offset);
        if (consensusHeader.schemaId() != io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }

        if (consensusHeader.templateId() != SessionMessageHeaderDecoder.TEMPLATE_ID) {
            return; // consensus protocol message, not a service message
        }

        // Skip consensus framing to reach the service message.
        final int serviceOffset = offset + CONSENSUS_FRAMING_LENGTH;
        if (serviceOffset + com.adbe.protocol.MessageHeaderDecoder.ENCODED_LENGTH > offset + length) {
            return; // truncated service header; wait for a well-formed fragment
        }

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

    private long findConsensusRecording() {
        final long logStreamId = 100; // ConsensusModule.Configuration.LOG_STREAM_ID_DEFAULT
        final long[] latest = {-1L}; // highest recordingId on the log stream

        // The consensus log recording is active while the cluster runs, so its
        // stopPosition is NULL_POSITION; select by highest recordingId rather
        // than requiring a closed (stopped) recording.
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
                    if (streamId == logStreamId && recordingId > latest[0]) {
                        latest[0] = recordingId;
                    }
                };

        archive.listRecordings(0L, 200, consumer);
        return latest[0];
    }
}
