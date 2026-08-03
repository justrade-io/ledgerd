package com.adbe.core;

import com.adbe.collections.AllowanceStore;
import com.adbe.collections.BalanceStore;
import com.adbe.collections.DedupTable;
import com.adbe.config.CoreConfig;
import com.adbe.core.handlers.ApproveHandler;
import com.adbe.core.handlers.CreditHandler;
import com.adbe.core.handlers.DebitHandler;
import com.adbe.core.handlers.DelegatedTransferHandler;
import com.adbe.core.handlers.TransferHandler;
import com.adbe.persistence.SnapshotManager;
import com.adbe.protocol.CommandEnvelopeDecoder;
import com.adbe.protocol.CommandType;
import com.adbe.telemetry.CoreMetrics;

/**
 * The deterministic state machine: idempotent dispatch of commands over balance
 * and allowance state. This class is intentionally free of any Aeron dependency
 * so it can be driven directly in unit and replay tests.
 *
 * <p>Single-writer: all methods must be invoked from one thread. No clock, no
 * randomness, no external I/O.
 */
public final class BalanceEngine {

    private final BalanceStore balances;
    private final AllowanceStore allowances;
    private final DedupTable dedup;
    private final CoreMetrics metrics;

    private final CreditHandler creditHandler;
    private final DebitHandler debitHandler;
    private final TransferHandler transferHandler;
    private final ApproveHandler approveHandler;
    private final DelegatedTransferHandler delegatedTransferHandler;

    private long lastBalanceCount = -1L;
    private long lastAllowanceOwnerCount = -1L;
    private long lastDedupClientCount = -1L;

    public BalanceEngine(final CoreConfig config, final CoreMetrics metrics) {
        this.balances = new BalanceStore(config.accountCapacity());
        this.allowances = new AllowanceStore(config.allowanceOwnerCapacity(), config.delegateCapacity());
        this.dedup = new DedupTable(config.dedupClientCapacity(), config.dedupWindow());
        this.metrics = metrics;
        this.creditHandler = new CreditHandler(balances);
        this.debitHandler = new DebitHandler(balances);
        this.transferHandler = new TransferHandler(balances);
        this.approveHandler = new ApproveHandler(allowances);
        this.delegatedTransferHandler = new DelegatedTransferHandler(balances, allowances);
    }

    /**
     * Processes one decoded command and populates {@code out}.
     *
     * @return {@code true} if this was a duplicate (cached result returned,
     *     command not re-applied), {@code false} if freshly applied.
     */
    public boolean process(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        final long clientId = cmd.clientId();
        final long clientSeq = cmd.clientSeq();

        final DedupRingHit hit = lookupDuplicate(clientId, clientSeq, out);
        if (hit == DedupRingHit.DUPLICATE) {
            metrics.onDuplicate();
            return true;
        }

        final long idHi = cmd.commandIdHi();
        final long idLo = cmd.commandIdLo();
        out.reset(idHi, idLo);
        dispatch(cmd, out);

        final boolean evicted = dedup.store(
                clientId,
                clientSeq,
                idHi,
                idLo,
                out.status().value(),
                out.resultBalance(),
                out.hasBalance(),
                out.resultAllowance(),
                out.hasAllowance());
        if (evicted) {
            metrics.onDedupEvicted();
        }

        metrics.onCommandProcessed();
        recordStatus(out);
        publishSizeGauges();
        return false;
    }

    private enum DedupRingHit {
        FRESH,
        DUPLICATE
    }

    private DedupRingHit lookupDuplicate(final long clientId, final long clientSeq, final CommandOutcome out) {
        final var ring = dedup.ringFor(clientId);
        if (ring != null && ring.contains(clientSeq)) {
            out.set(
                    ring.commandIdHi(clientSeq),
                    ring.commandIdLo(clientSeq),
                    com.adbe.protocol.StatusCode.get(ring.status(clientSeq)),
                    ring.resultBalance(clientSeq),
                    ring.hasBalance(clientSeq),
                    ring.resultAllowance(clientSeq),
                    ring.hasAllowance(clientSeq));
            return DedupRingHit.DUPLICATE;
        }
        return DedupRingHit.FRESH;
    }

    private void dispatch(final CommandEnvelopeDecoder cmd, final CommandOutcome out) {
        final CommandType type = cmd.commandType();
        final long a = cmd.accountA();
        final long b = cmd.accountB();
        final long c = cmd.accountC();
        final long amount = cmd.amount();
        switch (type) {
            case CREDIT -> creditHandler.handle(a, amount, out);
            case DEBIT -> debitHandler.handle(a, amount, out);
            case TRANSFER -> transferHandler.handle(a, b, amount, out);
            case APPROVE -> approveHandler.approve(a, b, amount, out);
            case INCREASE_ALLOWANCE -> approveHandler.increase(a, b, amount, out);
            case DECREASE_ALLOWANCE -> approveHandler.decrease(a, b, amount, out);
            case DELEGATED_TRANSFER -> delegatedTransferHandler.handle(a, b, c, amount, out);
            case NULL_VAL -> out.status(com.adbe.protocol.StatusCode.INVALID_AMOUNT);
        }
    }

    private void recordStatus(final CommandOutcome out) {
        switch (out.status()) {
            case INSUFFICIENT_BALANCE -> metrics.onInsufficientBalance();
            case INSUFFICIENT_ALLOWANCE -> metrics.onInsufficientAllowance();
            case INVALID_ACCOUNT -> metrics.onInvalidAccount();
            case OVERFLOW -> metrics.onOverflow();
            case INVALID_AMOUNT -> metrics.onInvalidAmount();
            default -> {
                // SUCCESS / DUPLICATE require no error counter.
            }
        }
    }

    /**
     * Publishes the current map sizes as gauges, but only when a size actually
     * changed, so steady-state processing performs no gauge writes. Sizes grow
     * only when a new account, owner, or client is first seen.
     */
    public void publishSizeGauges() {
        final long balanceSize = balances.size();
        if (balanceSize != lastBalanceCount) {
            lastBalanceCount = balanceSize;
            metrics.balanceCount(balanceSize);
        }
        final long ownerCount = allowances.ownerCount();
        if (ownerCount != lastAllowanceOwnerCount) {
            lastAllowanceOwnerCount = ownerCount;
            metrics.allowanceOwnerCount(ownerCount);
        }
        final long clientCount = dedup.clientCount();
        if (clientCount != lastDedupClientCount) {
            lastDedupClientCount = clientCount;
            metrics.dedupClientCount(clientCount);
        }
    }

    public BalanceStore balances() {
        return balances;
    }

    public AllowanceStore allowances() {
        return allowances;
    }

    public DedupTable dedup() {
        return dedup;
    }

    /** Writes engine state to a snapshot sink in deterministic order. */
    public void writeSnapshot(
            final SnapshotManager snapshotManager,
            final SnapshotManager.SnapshotSink sink,
            final Runnable idler,
            final long logPosition) {
        snapshotManager.write(sink, idler, balances, allowances, dedup, logPosition);
    }

    /** Prepares this engine's stores to receive a snapshot load. */
    public void beginSnapshotLoad(final SnapshotManager snapshotManager) {
        snapshotManager.beginLoad(balances, allowances, dedup);
    }

    /** Clears all engine state; used to discard a rejected snapshot load. */
    public void clearState() {
        balances.clear();
        allowances.clear();
        dedup.clear();
    }
}
