package com.adbe.core;

import com.adbe.config.CoreConfig;
import com.adbe.persistence.SnapshotManager;
import com.adbe.protocol.CommandEnvelopeDecoder;
import com.adbe.protocol.CommandResultEncoder;
import com.adbe.protocol.MessageHeaderDecoder;
import com.adbe.protocol.MessageHeaderEncoder;
import com.adbe.telemetry.CoreMetrics;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The ADBE clustered service: a deterministic, single-writer state machine that
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

    private final BalanceEngine engine;
    private final SnapshotManager snapshotManager;
    private final CoreMetrics metrics;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final MessageHeaderEncoder resultHeaderEncoder = new MessageHeaderEncoder();
    private final CommandResultEncoder resultEncoder = new CommandResultEncoder();
    private final CommandOutcome outcome = new CommandOutcome();
    private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_LENGTH]);

    private Cluster cluster;
    private IdleStrategy idleStrategy;

    public BalanceService(final CoreConfig config, final CoreMetrics metrics) {
        this.metrics = metrics;
        this.engine = new BalanceEngine(config, metrics);
        this.snapshotManager = new SnapshotManager();
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
        if (messageHeaderDecoder.templateId() != CommandEnvelopeDecoder.TEMPLATE_ID) {
            // Not a command we recognise; ignore rather than corrupt state.
            return;
        }

        envelopeDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

        engine.process(envelopeDecoder, outcome);
        sendResult(session);
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

    private void offerToSession(final ClientSession session, final int length) {
        idleStrategy.reset();
        while (true) {
            final long result = session.offer(egressBuffer, 0, length);
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
