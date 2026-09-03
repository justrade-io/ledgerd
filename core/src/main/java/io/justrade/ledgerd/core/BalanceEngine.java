package io.justrade.ledgerd.core;

import io.justrade.ledgerd.collections.AllowanceStore;
import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.collections.BatchDedupRing;
import io.justrade.ledgerd.collections.DedupTable;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.core.handlers.ApproveHandler;
import io.justrade.ledgerd.core.handlers.CreditHandler;
import io.justrade.ledgerd.core.handlers.DebitHandler;
import io.justrade.ledgerd.core.handlers.DelegatedTransferHandler;
import io.justrade.ledgerd.core.handlers.ReserveHandler;
import io.justrade.ledgerd.core.handlers.TransferBatchHandler;
import io.justrade.ledgerd.core.handlers.TransferHandler;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.protocol.CommandEnvelopeDecoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.TransferBatchDecoder;
import io.justrade.ledgerd.telemetry.CoreMetrics;

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
    private final int maxBatchSize;

    private final CreditHandler creditHandler;
    private final DebitHandler debitHandler;
    private final TransferHandler transferHandler;
    private final TransferBatchHandler transferBatchHandler;
    private final ApproveHandler approveHandler;
    private final DelegatedTransferHandler delegatedTransferHandler;
    private final ReserveHandler reserveHandler;

    private long lastBalanceCount = -1L;
    private long lastAllowanceOwnerCount = -1L;
    private long lastDedupClientCount = -1L;

    public BalanceEngine(final CoreConfig config, final CoreMetrics metrics) {
        this.balances = new BalanceStore(config.accountCapacity());
        this.allowances = new AllowanceStore(config.allowanceOwnerCapacity(), config.delegateCapacity());
        this.dedup = new DedupTable(
                config.dedupClientCapacity(), config.dedupWindow(), config.batchDedupWindow(), config.maxBatchSize());
        this.metrics = metrics;
        this.maxBatchSize = config.maxBatchSize();
        this.creditHandler = new CreditHandler(balances);
        this.debitHandler = new DebitHandler(balances);
        this.transferHandler = new TransferHandler(balances);
        this.transferBatchHandler = new TransferBatchHandler(balances, config.maxBatchSize());
        this.approveHandler = new ApproveHandler(allowances);
        this.delegatedTransferHandler = new DelegatedTransferHandler(balances, allowances);
        this.reserveHandler = new ReserveHandler(balances);
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
                out.hasAllowance(),
                out.resultReserved(),
                out.hasReserved());
        if (evicted) {
            metrics.onDedupEvicted();
        }

        metrics.onCommandProcessed();
        recordStatus(out);
        publishSizeGauges();
        return false;
    }

    /**
     * Processes one decoded transfer batch and populates {@code out}. A batch is
     * one idempotency unit at {@code (clientId, clientSeq)}: resubmitting the same
     * sequence replays the cached per-leg results without re-applying.
     *
     * @return {@code true} if this was a duplicate (cached results returned),
     *     {@code false} if freshly applied.
     */
    public boolean processBatch(final TransferBatchDecoder batch, final BatchOutcome out) {
        final long clientId = batch.clientId();
        final long clientSeq = batch.clientSeq();

        final BatchDedupRing ring = dedup.batchRingFor(clientId);
        if (ring != null && ring.contains(clientSeq)) {
            replayBatch(ring, clientSeq, out);
            metrics.onDuplicate();
            return true;
        }

        transferBatchHandler.handle(batch, out);

        final boolean evicted = dedup.storeBatch(
                clientId,
                clientSeq,
                batch.batchIdHi(),
                batch.batchIdLo(),
                out.legCount(),
                out.statusValues(),
                out.hasBalanceFlags(),
                out.resultBalances());
        if (evicted) {
            metrics.onDedupEvicted();
        }

        metrics.onCommandProcessed();
        recordBatchStatus(out);
        publishSizeGauges();
        return false;
    }

    private void replayBatch(final BatchDedupRing ring, final long clientSeq, final BatchOutcome out) {
        final int legCount = ring.legCount(clientSeq);
        out.reset(legCount);
        for (int i = 0; i < legCount; i++) {
            out.setLeg(
                    i,
                    io.justrade.ledgerd.protocol.StatusCode.get(ring.status(clientSeq, i)),
                    ring.hasBalance(clientSeq, i),
                    ring.resultBalance(clientSeq, i));
        }
    }

    private void recordBatchStatus(final BatchOutcome out) {
        for (int i = 0; i < out.legCount(); i++) {
            switch (out.legStatus(i)) {
                case INSUFFICIENT_BALANCE -> metrics.onInsufficientBalance();
                case INVALID_ACCOUNT -> metrics.onInvalidAccount();
                case OVERFLOW -> metrics.onOverflow();
                case INVALID_AMOUNT, INVALID_CHAIN -> metrics.onInvalidAmount();
                default -> {
                    // SUCCESS and the allowance/reserved statuses carry no error counter.
                }
            }
        }
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
                    io.justrade.ledgerd.protocol.StatusCode.get(ring.status(clientSeq)),
                    ring.resultBalance(clientSeq),
                    ring.hasBalance(clientSeq),
                    ring.resultAllowance(clientSeq),
                    ring.hasAllowance(clientSeq),
                    ring.resultReserved(clientSeq),
                    ring.hasReserved(clientSeq));
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
        long asset = cmd.assetId();
        if (asset == CommandEnvelopeDecoder.assetIdNullValue()) {
            asset = BalanceStore.DEFAULT_ASSET;
        }
        switch (type) {
            case CREDIT -> creditHandler.handle(asset, a, amount, out);
            case DEBIT -> debitHandler.handle(asset, a, amount, out);
            case TRANSFER -> transferHandler.handle(asset, a, b, amount, out);
            case APPROVE -> approveHandler.approve(asset, a, b, amount, out);
            case INCREASE_ALLOWANCE -> approveHandler.increase(asset, a, b, amount, out);
            case DECREASE_ALLOWANCE -> approveHandler.decrease(asset, a, b, amount, out);
            case DELEGATED_TRANSFER -> delegatedTransferHandler.handle(asset, a, b, c, amount, out);
            case RESERVE -> reserveHandler.reserve(asset, a, amount, out);
            case CAPTURE -> reserveHandler.capture(asset, a, b, amount, out);
            case RELEASE -> reserveHandler.release(asset, a, amount, out);
            case NULL_VAL -> out.status(io.justrade.ledgerd.protocol.StatusCode.INVALID_AMOUNT);
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

    public int maxBatchSize() {
        return maxBatchSize;
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
