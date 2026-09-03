package io.justrade.ledgerd.read;

import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.justrade.ledgerd.core.BalanceEngine;
import io.justrade.ledgerd.core.BatchOutcome;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.protocol.CommandEnvelopeDecoder;
import io.justrade.ledgerd.protocol.TransferBatchDecoder;
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
 * <p>Single-writer: this class owns no thread. The read replica node's single agent
 * thread calls {@link #connect()} (reconnecting after each snapshot load) and
 * then drives {@link #poll(int)} from its event loop, so
 * {@link BalanceEngine#process} is only ever invoked from that one thread - the
 * same thread that serves queries. No concurrency control is required.
 */
final class LiveLogSubscriber implements AutoCloseable {

    private static final int CONSENSUS_FRAMING_LENGTH =
            io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;
    private static final System.Logger LOG = System.getLogger(LiveLogSubscriber.class.getName());

    private final AeronArchive archive;
    private final BalanceEngine engine;
    private final long startPosition;
    private final String localHost;
    private final io.aeron.cluster.codecs.MessageHeaderDecoder consensusHeader =
            new io.aeron.cluster.codecs.MessageHeaderDecoder();
    private final io.justrade.ledgerd.protocol.MessageHeaderDecoder ledgerdHeader =
            new io.justrade.ledgerd.protocol.MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final TransferBatchDecoder transferBatchDecoder = new TransferBatchDecoder();
    private final CommandOutcome outcome = new CommandOutcome();
    private final BatchOutcome batchOutcome;
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Subscription subscription;
    private long lastPosition;
    private boolean hadImage;

    /**
     * @param archive       connected AeronArchive client for the cluster
     * @param engine        the balance engine to apply messages to
     * @param startPosition log position to start replaying from: the position of
     *                      the last loaded snapshot, or {@code 0} to follow from
     *                      the start of the log when no snapshot has loaded yet
     * @param localHost     routable host the replay subscription binds on, so the
     *                      Archive can connect back (localhost for same-host runs)
     */
    LiveLogSubscriber(
            final AeronArchive archive, final BalanceEngine engine, final long startPosition, final String localHost) {
        this.archive = archive;
        this.engine = engine;
        this.startPosition = startPosition;
        this.localHost = localHost;
        this.batchOutcome = new BatchOutcome(engine.maxBatchSize());
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
            // No consensus recording yet (e.g. the cluster has not committed). The
            // read replica retries connect() on its agent loop, so this is a normal
            // transient state rather than an error; stay silent to avoid log spam.
            return false;
        }

        final Subscription sub = archive.context()
                .aeron()
                .addSubscription("aeron:udp?endpoint=" + localHost + ":0", ReadStreams.LIVE_LOG_REPLAY);
        final String endpoint = awaitResolvedEndpoint(sub);
        if (endpoint == null) {
            sub.close();
            LOG.log(System.Logger.Level.WARNING, "LiveLogSubscriber: timed out resolving replay endpoint");
            return false;
        }

        final String replayChannel = "aeron:udp?endpoint=" + endpoint;
        final long sessionId = archive.startReplay(
                recordingId, startPosition, AeronArchive.NULL_LENGTH, replayChannel, ReadStreams.LIVE_LOG_REPLAY);
        this.subscription = sub;
        this.lastPosition = startPosition;

        LOG.log(
                System.Logger.Level.INFO,
                "LiveLogSubscriber: following recording={0} from position={1} session={2}",
                recordingId,
                startPosition,
                sessionId);
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
        if (subscription.imageCount() > 0) {
            hadImage = true;
        }
        return subscription.poll(fragmentHandler, fragmentLimit);
    }

    /**
     * Whether the bounded replay has run to the end of the recording and its image
     * has closed. An Archive replay of an active recording follows the live tail
     * only while data keeps arriving; once it catches up to an idle recording the
     * replay session ends (the image closes). The read replica observes this and
     * re-points a fresh replay from the last consumed position, so commits that
     * land after the replay ended are still picked up. Returns {@code false} while
     * the replay image is still establishing (never yet seen).
     */
    boolean isReplayEnded() {
        return subscription != null && hadImage && subscription.imageCount() == 0;
    }

    /**
     * The consensus log position consumed up to (the position of the last polled
     * fragment), or the start position before any fragment arrives. Shares the
     * cluster-global log-position coordinate space with a snapshot's
     * {@code logPosition}, so the two are comparable across Archives.
     */
    long lastPosition() {
        return lastPosition;
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
        lastPosition = header.position();
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
        if (serviceOffset + io.justrade.ledgerd.protocol.MessageHeaderDecoder.ENCODED_LENGTH > offset + length) {
            return; // truncated service header; wait for a well-formed fragment
        }

        ledgerdHeader.wrap(buffer, serviceOffset);
        final int bodyOffset = serviceOffset + io.justrade.ledgerd.protocol.MessageHeaderDecoder.ENCODED_LENGTH;

        if (ledgerdHeader.templateId() == CommandEnvelopeDecoder.TEMPLATE_ID) {
            envelopeDecoder.wrap(buffer, bodyOffset, ledgerdHeader.blockLength(), ledgerdHeader.version());
            engine.process(envelopeDecoder, outcome);
            return;
        }

        if (ledgerdHeader.templateId() == TransferBatchDecoder.TEMPLATE_ID) {
            transferBatchDecoder.wrap(buffer, bodyOffset, ledgerdHeader.blockLength(), ledgerdHeader.version());
            engine.processBatch(transferBatchDecoder, batchOutcome);
            return;
        }

        // Not a command we process.
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
