package com.adbe.read.projection;

import com.adbe.collections.AllowanceStore;
import com.adbe.collections.BalanceStore;
import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceEngine;
import com.adbe.core.BalanceService;
import com.adbe.read.query.QueryCodec;
import com.adbe.read.query.QueryType;
import com.adbe.read.query.ReadQueryGateway;
import com.adbe.telemetry.CoreMetrics;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A read-side cluster service. It composes the deterministic core
 * {@link BalanceService}, delegating every cluster callback to it so the applied
 * state is byte-identical to the leader's, and additionally answers read queries
 * on the same single service thread via {@link #doBackgroundWork(long)}.
 *
 * <p>Because queries are drained and answered on the service thread, reads see a
 * consistent snapshot of the stores without any concurrent access, preserving the
 * single-writer discipline. Requests and responses cross thread boundaries only
 * through the lock-free ring buffers owned by {@link ReadQueryGateway}.
 */
public final class ReadModelService implements ClusteredService {

    private static final int REQUEST_DRAIN_LIMIT = 64;

    private final BalanceService delegate;
    private final BalanceEngine engine;
    private final ReadQueryGateway gateway;

    private final MessageHandler queryHandler = this::onQuery;
    private final UnsafeBuffer responseBuffer = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);

    public ReadModelService(final CoreConfig config, final CoreMetrics metrics, final ReadQueryGateway gateway) {
        this.delegate = new BalanceService(config, metrics);
        this.engine = delegate.engine();
        this.gateway = gateway;
    }

    // --- Read serving (service thread) -----------------------------------

    @Override
    public int doBackgroundWork(final long nowNs) {
        return gateway.readRequests(queryHandler, REQUEST_DRAIN_LIMIT);
    }

    private void onQuery(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
        final long correlationId = QueryCodec.correlationId(buffer, index);
        final QueryType type = QueryCodec.queryType(buffer, index);
        switch (type) {
            case BALANCE -> answerBalances(correlationId, buffer, index, 1);
            case BATCH_BALANCE -> answerBalances(correlationId, buffer, index, QueryCodec.count(buffer, index));
            case ALLOWANCE -> answerAllowance(correlationId, buffer, index);
            case TOTAL_SUPPLY -> answerTotalSupply(correlationId);
        }
    }

    private void answerBalances(
            final long correlationId, final DirectBuffer request, final int reqIndex, final int accountCount) {
        final BalanceStore balances = engine.balances();
        QueryCodec.beginResponse(responseBuffer, correlationId, QueryType.BATCH_BALANCE, accountCount);
        for (int i = 0; i < accountCount; i++) {
            final long accountId = QueryCodec.operand(request, reqIndex, i);
            final long balance = balances.rawGet(accountId);
            final boolean exists = balance != BalanceStore.MISSING;
            QueryCodec.putEntry(responseBuffer, i, exists ? balance : 0L, exists);
        }
        gateway.offerResponse(responseBuffer, 0, QueryCodec.responseLength(accountCount));
    }

    private void answerAllowance(final long correlationId, final DirectBuffer request, final int reqIndex) {
        final AllowanceStore allowances = engine.allowances();
        final long ownerId = QueryCodec.operand(request, reqIndex, 0);
        final long delegateId = QueryCodec.operand(request, reqIndex, 1);
        final long allowance = allowances.get(ownerId, delegateId);
        QueryCodec.beginResponse(responseBuffer, correlationId, QueryType.ALLOWANCE, 1);
        QueryCodec.putEntry(responseBuffer, 0, allowance, true);
        gateway.offerResponse(responseBuffer, 0, QueryCodec.responseLength(1));
    }

    private void answerTotalSupply(final long correlationId) {
        final long supply = engine.balances().totalSupply();
        QueryCodec.beginResponse(responseBuffer, correlationId, QueryType.TOTAL_SUPPLY, 1);
        QueryCodec.putEntry(responseBuffer, 0, supply, true);
        gateway.offerResponse(responseBuffer, 0, QueryCodec.responseLength(1));
    }

    // --- Delegated cluster callbacks -------------------------------------

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        delegate.onStart(cluster, snapshotImage);
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        delegate.onSessionOpen(session, timestamp);
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        delegate.onSessionClose(session, timestamp, closeReason);
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        delegate.onTimerEvent(correlationId, timestamp);
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        delegate.onTakeSnapshot(snapshotPublication);
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        delegate.onRoleChange(newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        delegate.onTerminate(cluster);
    }

    /** Exposes the underlying engine for in-process tests. */
    public BalanceEngine engine() {
        return engine;
    }
}
