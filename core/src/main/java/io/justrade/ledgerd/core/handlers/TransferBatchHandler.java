package io.justrade.ledgerd.core.handlers;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.core.BatchOutcome;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.protocol.TransferBatchDecoder;
import io.justrade.ledgerd.util.Amounts;

/**
 * Applies a {@code TransferBatch}: a batch of transfer legs where contiguous runs
 * of {@code linked} legs form atomic all-or-nothing chains.
 *
 * <p>Each leg follows the single-command {@code TRANSFER} semantics (funds move
 * between two accounts, total supply is conserved, the recipient is created on
 * first receipt). A leg that fails rolls back every already-applied leg in its
 * chain; legs in other chains are unaffected.
 *
 * <p>Transfer legs only mutate an account's {@code available} balance, so the
 * undo mechanism is a narrow preallocated before-image stack restored through
 * {@link BalanceStore#restoreAvailable}. The single-command hot path is untouched.
 */
public final class TransferBatchHandler {

    private final BalanceStore balances;
    private final int maxBatchSize;

    private final long[] assetIds;
    private final long[] fromIds;
    private final long[] toIds;
    private final long[] amounts;
    private final byte[] linked;

    private final long[] undoAsset;
    private final long[] undoAccount;
    private final long[] undoPrior;
    private int undoTop;

    public TransferBatchHandler(final BalanceStore balances, final int maxBatchSize) {
        this.balances = balances;
        this.maxBatchSize = maxBatchSize;
        this.assetIds = new long[maxBatchSize];
        this.fromIds = new long[maxBatchSize];
        this.toIds = new long[maxBatchSize];
        this.amounts = new long[maxBatchSize];
        this.linked = new byte[maxBatchSize];
        this.undoAsset = new long[maxBatchSize * 2];
        this.undoAccount = new long[maxBatchSize * 2];
        this.undoPrior = new long[maxBatchSize * 2];
    }

    public void handle(final TransferBatchDecoder batch, final BatchOutcome out) {
        final TransferBatchDecoder.LegsDecoder legs = batch.legs();
        final int legCount = legs.count();

        if (legCount == 0) {
            out.reset(0);
            return;
        }
        if (legCount > maxBatchSize) {
            out.reset(maxBatchSize);
            for (int i = 0; i < maxBatchSize; i++) {
                out.setLeg(i, StatusCode.INVALID_CHAIN, false, 0L);
            }
            return;
        }

        // Copy legs into preallocated scratch so chains can be processed with
        // random access (the SBE group is a forward-only iterator).
        for (int i = 0; i < legCount; i++) {
            legs.next();
            assetIds[i] = legs.assetId();
            fromIds[i] = legs.fromId();
            toIds[i] = legs.toId();
            amounts[i] = legs.amount();
            linked[i] = (byte) legs.linked();
        }

        out.reset(legCount);

        // A trailing linked flag claims to continue a chain past the last leg.
        if (linked[legCount - 1] != 0) {
            for (int i = 0; i < legCount; i++) {
                out.setLeg(i, StatusCode.INVALID_CHAIN, false, 0L);
            }
            return;
        }

        int i = 0;
        while (i < legCount) {
            final int chainStart = i;
            final int chainEnd = chainEndOf(chainStart, legCount);
            applyChain(chainStart, chainEnd, out);
            i = chainEnd;
        }
    }

    /** A chain continues while its current leg is linked to the next. */
    private int chainEndOf(final int start, final int legCount) {
        int end = start + 1;
        while (end < legCount && linked[end - 1] != 0) {
            end++;
        }
        return end;
    }

    private void applyChain(final int start, final int end, final BatchOutcome out) {
        undoTop = 0;
        final int chainEventStart = out.eventCount();
        for (int k = start; k < end; k++) {
            final StatusCode status = applyLeg(k, out);
            if (status != StatusCode.SUCCESS) {
                rollback();
                out.truncateEvents(chainEventStart);
                for (int m = start; m < end; m++) {
                    out.setLeg(m, status, false, 0L);
                }
                return;
            }
        }
    }

    private StatusCode applyLeg(final int index, final BatchOutcome out) {
        final long assetId = assetIds[index];
        final long fromId = fromIds[index];
        final long toId = toIds[index];
        final long amount = amounts[index];

        if (Amounts.isNegative(amount)) {
            out.setLeg(index, StatusCode.INVALID_AMOUNT, false, 0L);
            return StatusCode.INVALID_AMOUNT;
        }
        final long fromBalance = balances.rawGet(assetId, fromId);
        if (fromBalance == BalanceStore.MISSING) {
            out.setLeg(index, StatusCode.INVALID_ACCOUNT, false, 0L);
            return StatusCode.INVALID_ACCOUNT;
        }
        if (fromBalance < amount) {
            out.setLeg(index, StatusCode.INSUFFICIENT_BALANCE, false, 0L);
            return StatusCode.INSUFFICIENT_BALANCE;
        }
        if (fromId == toId) {
            out.setLeg(index, StatusCode.SUCCESS, true, fromBalance);
            return StatusCode.SUCCESS;
        }
        final long toRaw = balances.rawGet(assetId, toId);
        final long toBase = toRaw == BalanceStore.MISSING ? 0L : toRaw;
        if (Amounts.addOverflows(toBase, amount)) {
            out.setLeg(index, StatusCode.OVERFLOW, false, 0L);
            return StatusCode.OVERFLOW;
        }

        // Commit: record before-images, then mutate. Only `available` changes.
        undoAsset[undoTop] = assetId;
        undoAccount[undoTop] = fromId;
        undoPrior[undoTop] = fromBalance;
        undoTop++;
        undoAsset[undoTop] = assetId;
        undoAccount[undoTop] = toId;
        undoPrior[undoTop] = toRaw;
        undoTop++;

        final long fromNew = fromBalance - amount;
        final long toNew = toBase + amount;
        balances.set(assetId, fromId, fromNew);
        balances.set(assetId, toId, toNew);
        out.setLeg(index, StatusCode.SUCCESS, true, fromNew);
        out.addTransferEvents(assetId, fromId, toId, fromNew, toNew, amount);
        return StatusCode.SUCCESS;
    }

    private void rollback() {
        for (int u = undoTop - 1; u >= 0; u--) {
            balances.restoreAvailable(undoAsset[u], undoAccount[u], undoPrior[u]);
        }
        undoTop = 0;
    }
}
