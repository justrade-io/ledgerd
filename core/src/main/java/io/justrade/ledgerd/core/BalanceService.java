package io.justrade.ledgerd.core;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.pipeline.EventJournalRing;
import io.justrade.ledgerd.protocol.AllowanceChangedEventEncoder;
import io.justrade.ledgerd.protocol.BalanceChangedEventEncoder;
import io.justrade.ledgerd.protocol.CapturedEventEncoder;
import io.justrade.ledgerd.protocol.CommandEnvelopeDecoder;
import io.justrade.ledgerd.protocol.CommandRejectedEventEncoder;
import io.justrade.ledgerd.protocol.CommandResultEncoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.ReleasedEventEncoder;
import io.justrade.ledgerd.protocol.ReservedEventEncoder;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.protocol.TransferBatchDecoder;
import io.justrade.ledgerd.protocol.TransferBatchResultEncoder;
import io.justrade.ledgerd.protocol.TransferEventEncoder;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The LEDGERD clustered service: a deterministic, single-writer state machine that
 * applies balance and allowance commands, guarantees idempotency, replies with a
 * {@link CommandResultEncoder}, and supports controlled snapshotting.
 *
 * <p>All mutation happens on the single {@code ClusteredServiceAgent} thread, so
 * no locks or concurrency control are used. No system clock, randomness, or
 * external I/O is used; the only permitted time source is the leader-assigned
 * {@code timestamp} passed to {@link #onSessionMessage}.
 */
public final class BalanceService implements io.aeron.cluster.service.ClusteredService {

    private static final int EGRESS_BUFFER_LENGTH = 128;
    private static final int EVENT_BUFFER_LENGTH = 64;

    private final BalanceEngine engine;
    private final SnapshotManager snapshotManager;
    private final CoreMetrics metrics;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final TransferBatchDecoder transferBatchDecoder = new TransferBatchDecoder();
    private final MessageHeaderEncoder resultHeaderEncoder = new MessageHeaderEncoder();
    private final CommandResultEncoder resultEncoder = new CommandResultEncoder();
    private final TransferBatchResultEncoder batchResultEncoder = new TransferBatchResultEncoder();
    private final CommandOutcome outcome = new CommandOutcome();
    private final BatchOutcome batchOutcome;
    private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_LENGTH]);
    private final UnsafeBuffer batchEgressBuffer;

    // Domain event journal (ADR 0011): encoders and ring are only allocated when
    // journaling is enabled, so a non-journaling node pays nothing.
    private final boolean journalEnabled;
    private final EventJournalRing eventRing;
    private final MessageHeaderEncoder eventHeaderEncoder = new MessageHeaderEncoder();
    private final BalanceChangedEventEncoder balanceChangedEncoder = new BalanceChangedEventEncoder();
    private final ReservedEventEncoder reservedEncoder = new ReservedEventEncoder();
    private final CapturedEventEncoder capturedEncoder = new CapturedEventEncoder();
    private final ReleasedEventEncoder releasedEncoder = new ReleasedEventEncoder();
    private final TransferEventEncoder transferEncoder = new TransferEventEncoder();
    private final AllowanceChangedEventEncoder allowanceChangedEncoder = new AllowanceChangedEventEncoder();
    private final CommandRejectedEventEncoder commandRejectedEncoder = new CommandRejectedEventEncoder();
    private final UnsafeBuffer eventBuffer = new UnsafeBuffer(new byte[EVENT_BUFFER_LENGTH]);

    private Cluster cluster;
    private IdleStrategy idleStrategy;

    public BalanceService(final CoreConfig config, final CoreMetrics metrics) {
        this.metrics = metrics;
        this.engine = new BalanceEngine(config, metrics);
        this.snapshotManager = new SnapshotManager(config.maxBatchSize());
        this.journalEnabled = config.eventJournalEnabled();
        this.eventRing = journalEnabled ? new EventJournalRing(config.eventJournalCapacity()) : null;
        this.batchOutcome = new BatchOutcome(config.maxBatchSize());
        this.batchEgressBuffer = new UnsafeBuffer(
                new byte
                        [MessageHeaderEncoder.ENCODED_LENGTH
                                + TransferBatchResultEncoder.BLOCK_LENGTH
                                + TransferBatchResultEncoder.ResultsEncoder.HEADER_SIZE
                                + TransferBatchResultEncoder.ResultsEncoder.sbeBlockLength() * config.maxBatchSize()]);
    }

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        messageHeaderDecoder.wrap(buffer, offset);
        final int templateId = messageHeaderDecoder.templateId();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        if (templateId == CommandEnvelopeDecoder.TEMPLATE_ID) {
            envelopeDecoder.wrap(
                    buffer, bodyOffset, messageHeaderDecoder.blockLength(), messageHeaderDecoder.version());
            final boolean duplicate = engine.process(envelopeDecoder, outcome);
            sendResult(session);
            if (journalEnabled && !duplicate) {
                journalEvents(timestamp);
            }
            return;
        }

        if (templateId == TransferBatchDecoder.TEMPLATE_ID) {
            transferBatchDecoder.wrap(
                    buffer, bodyOffset, messageHeaderDecoder.blockLength(), messageHeaderDecoder.version());
            final boolean duplicate = engine.processBatch(transferBatchDecoder, batchOutcome);
            sendBatchResult(session);
            if (journalEnabled && !duplicate) {
                journalBatchEvents(timestamp);
            }
            return;
        }

        // Not a command we recognise; ignore rather than corrupt state.
    }

    private void sendResult(final ClientSession session) {
        resultEncoder
                .wrapAndApplyHeader(egressBuffer, 0, resultHeaderEncoder)
                .commandIdHi(outcome.commandIdHi())
                .commandIdLo(outcome.commandIdLo())
                .status(outcome.status())
                .resultBalance(
                        outcome.hasBalance() ? outcome.resultBalance() : CommandResultEncoder.resultBalanceNullValue())
                .resultAllowance(
                        outcome.hasAllowance()
                                ? outcome.resultAllowance()
                                : CommandResultEncoder.resultAllowanceNullValue())
                .resultReserved(
                        outcome.hasReserved()
                                ? outcome.resultReserved()
                                : CommandResultEncoder.resultReservedNullValue());

        final int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resultEncoder.encodedLength();
        offerToSession(session, msgLength);
    }

    private void sendBatchResult(final ClientSession session) {
        batchResultEncoder
                .wrapAndApplyHeader(batchEgressBuffer, 0, resultHeaderEncoder)
                .batchIdHi(transferBatchDecoder.batchIdHi())
                .batchIdLo(transferBatchDecoder.batchIdLo());
        final TransferBatchResultEncoder.ResultsEncoder results =
                batchResultEncoder.resultsCount(batchOutcome.legCount());
        for (int i = 0; i < batchOutcome.legCount(); i++) {
            results.next()
                    .status(batchOutcome.legStatus(i))
                    .hasBalance(batchOutcome.legHasBalance(i) ? (short) 1 : (short) 0)
                    .resultBalance(batchOutcome.legResultBalance(i));
        }
        final int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + batchResultEncoder.encodedLength();
        offerToSession(session, batchEgressBuffer, msgLength);
    }

    private void offerToSession(final ClientSession session, final int length) {
        offerToSession(session, egressBuffer, length);
    }

    private void offerToSession(final ClientSession session, final DirectBuffer buffer, final int length) {
        idleStrategy.reset();
        while (true) {
            final long result = session.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED
                    || result == Publication.MAX_POSITION_EXCEEDED
                    || result == Publication.NOT_CONNECTED) {
                // Session gone; nothing to deliver to. Do not spin forever.
                metrics.onBackpressure();
                return;
            }
            metrics.onBackpressure();
            idleStrategy.idle();
        }
    }

    /** The event-journal ring drained by the launcher's journaler, or null when disabled. */
    public EventJournalRing eventRing() {
        return eventRing;
    }

    // Encodes the domain events recorded for the command just applied (ADR 0011).
    // A rejected command emits one CommandRejectedEvent; a successful command
    // emits the events its handler recorded. Runs only on a fresh (non-duplicate)
    // apply, after the ACK, on the single-writer thread.
    private void journalEvents(final long timestamp) {
        final long logPosition = cluster.logPosition();
        if (outcome.status() != StatusCode.SUCCESS) {
            encodeRejected(logPosition, timestamp);
            return;
        }
        final int count = outcome.eventCount();
        for (int i = 0; i < count; i++) {
            encodeEvent(outcome.event(i), i, logPosition, timestamp);
        }
    }

    // Encodes the domain events for a transfer batch (ADR 0011). Committed legs
    // emit their staged transfer events; failed legs emit one rejection event
    // each. eventIndex is unique across the whole batch because the batch is one
    // log entry.
    private void journalBatchEvents(final long timestamp) {
        final long logPosition = cluster.logPosition();
        final TransferBatchDecoder.LegsDecoder legs = transferBatchDecoder.legs();
        int stagedIndex = 0;
        int eventIndex = 0;
        for (int i = 0; i < batchOutcome.legCount(); i++) {
            legs.next();
            if (batchOutcome.legStatus(i) != StatusCode.SUCCESS) {
                encodeBatchRejected(logPosition, timestamp, eventIndex++, legs, batchOutcome.legStatus(i));
            } else {
                for (int e = 0; e < BatchOutcome.EVENTS_PER_LEG; e++) {
                    encodeBatchEvent(stagedIndex++, eventIndex++, logPosition, timestamp);
                }
            }
        }
    }

    private void encodeRejected(final long logPosition, final long timestamp) {
        long asset = envelopeDecoder.assetId();
        if (asset == CommandEnvelopeDecoder.assetIdNullValue()) {
            asset = BalanceStore.DEFAULT_ASSET;
        }
        commandRejectedEncoder
                .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                .logPosition(logPosition)
                .timestamp(timestamp)
                .eventIndex(0)
                .assetId(asset)
                .accountId(envelopeDecoder.accountA())
                .amount(envelopeDecoder.amount())
                .commandType(envelopeDecoder.commandType())
                .reason(outcome.status());
        offerEvent(MessageHeaderEncoder.ENCODED_LENGTH + commandRejectedEncoder.encodedLength());
    }

    private void encodeEvent(
            final CommandOutcome.EventRecord e, final int index, final long logPosition, final long timestamp) {
        int length = 0;
        switch (e.kind()) {
            case BALANCE_CHANGED -> {
                balanceChangedEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(index)
                        .assetId(e.assetId())
                        .accountId(e.accountA())
                        .newBalance(e.valueA())
                        .delta(e.valueB())
                        .cause(e.cause());
                length = MessageHeaderEncoder.ENCODED_LENGTH + balanceChangedEncoder.encodedLength();
            }
            case RESERVED -> {
                reservedEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(index)
                        .assetId(e.assetId())
                        .accountId(e.accountA())
                        .newAvailable(e.valueA())
                        .newReserved(e.valueB());
                length = MessageHeaderEncoder.ENCODED_LENGTH + reservedEncoder.encodedLength();
            }
            case CAPTURED -> {
                capturedEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(index)
                        .assetId(e.assetId())
                        .accountId(e.accountA())
                        .newAvailable(e.valueA())
                        .newReserved(e.valueB());
                length = MessageHeaderEncoder.ENCODED_LENGTH + capturedEncoder.encodedLength();
            }
            case RELEASED -> {
                releasedEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(index)
                        .assetId(e.assetId())
                        .accountId(e.accountA())
                        .newAvailable(e.valueA())
                        .newReserved(e.valueB());
                length = MessageHeaderEncoder.ENCODED_LENGTH + releasedEncoder.encodedLength();
            }
            case TRANSFER -> {
                transferEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(index)
                        .assetId(e.assetId())
                        .fromAccount(e.accountA())
                        .toAccount(e.accountB())
                        .amount(e.valueA());
                length = MessageHeaderEncoder.ENCODED_LENGTH + transferEncoder.encodedLength();
            }
            case ALLOWANCE_CHANGED -> {
                allowanceChangedEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(index)
                        .assetId(e.assetId())
                        .ownerId(e.accountA())
                        .delegateId(e.accountB())
                        .newAllowance(e.valueA());
                length = MessageHeaderEncoder.ENCODED_LENGTH + allowanceChangedEncoder.encodedLength();
            }
            default -> length = 0;
        }
        if (length > 0) {
            offerEvent(length);
        }
    }

    private void encodeBatchRejected(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final TransferBatchDecoder.LegsDecoder leg,
            final StatusCode reason) {
        commandRejectedEncoder
                .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                .logPosition(logPosition)
                .timestamp(timestamp)
                .eventIndex(eventIndex)
                .assetId(leg.assetId())
                .accountId(leg.fromId())
                .amount(leg.amount())
                .commandType(CommandType.TRANSFER)
                .reason(reason);
        offerEvent(MessageHeaderEncoder.ENCODED_LENGTH + commandRejectedEncoder.encodedLength());
    }

    private void encodeBatchEvent(
            final int stagedIndex, final int eventIndex, final long logPosition, final long timestamp) {
        int length;
        switch (batchOutcome.eventKind(stagedIndex)) {
            case BALANCE_CHANGED -> {
                balanceChangedEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(eventIndex)
                        .assetId(batchOutcome.eventAsset(stagedIndex))
                        .accountId(batchOutcome.eventAccountA(stagedIndex))
                        .newBalance(batchOutcome.eventValueA(stagedIndex))
                        .delta(batchOutcome.eventValueB(stagedIndex))
                        .cause(batchOutcome.eventCause(stagedIndex));
                length = MessageHeaderEncoder.ENCODED_LENGTH + balanceChangedEncoder.encodedLength();
            }
            case TRANSFER -> {
                transferEncoder
                        .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                        .logPosition(logPosition)
                        .timestamp(timestamp)
                        .eventIndex(eventIndex)
                        .assetId(batchOutcome.eventAsset(stagedIndex))
                        .fromAccount(batchOutcome.eventAccountA(stagedIndex))
                        .toAccount(batchOutcome.eventAccountB(stagedIndex))
                        .amount(batchOutcome.eventValueA(stagedIndex));
                length = MessageHeaderEncoder.ENCODED_LENGTH + transferEncoder.encodedLength();
            }
            default -> length = 0;
        }
        if (length > 0) {
            offerEvent(length);
        }
    }

    private void offerEvent(final int length) {
        if (!eventRing.write(eventBuffer, 0, length)) {
            metrics.onEventJournalOverflow();
        }
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        final long start = cluster.time();
        engine.writeSnapshot(
                snapshotManager,
                (recordBuffer, recordOffset, recordLength) ->
                        offerToPublication(snapshotPublication, recordBuffer, recordOffset, recordLength),
                idleStrategy::idle,
                cluster.logPosition());
        metrics.snapshotWriteNanos(cluster.time() - start);
    }

    private void offerToPublication(
            final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length) {
        idleStrategy.reset();
        while (true) {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("Snapshot publication unavailable: " + result);
            }
            idleStrategy.idle();
        }
    }

    private void loadSnapshot(final Image snapshotImage) {
        final long start = cluster == null ? 0L : cluster.time();
        engine.beginSnapshotLoad(snapshotManager);
        while (!snapshotManager.loadComplete()) {
            final int fragments = snapshotImage.poll(this::onSnapshotFragment, 32);
            if (fragments == 0) {
                if (snapshotImage.isEndOfStream()) {
                    break;
                }
                idleStrategy.idle();
            }
        }
        if (cluster != null) {
            metrics.snapshotReadNanos(cluster.time() - start);
        }
        // A corrupt or truncated snapshot must never become committed state; fail
        // fast so the node aborts recovery rather than serving a broken ledger.
        if (!snapshotManager.verifyInvariant()) {
            throw new IllegalStateException(
                    "Snapshot integrity check failed: sum(balances) != totalSupply or footer missing");
        }
        // Reflect the restored map sizes immediately, before any command arrives.
        engine.publishSizeGauges();
    }

    private void onSnapshotFragment(
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
        snapshotManager.onRecord(buffer, offset);
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        // No per-session state is retained in the core.
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        // No per-session state to release.
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        // No timers scheduled in Phase 1.
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        if (newRole == Cluster.Role.LEADER) {
            metrics.onLeaderElection();
        }
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        // No external resources to close.
    }

    /** Exposes the engine for in-process tests and benchmarks. */
    public BalanceEngine engine() {
        return engine;
    }
}
