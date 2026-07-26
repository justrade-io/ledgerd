package com.adbe.collections;

import java.util.Arrays;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Per-client idempotency table. Each client has a {@link DedupRing} retaining the
 * most recent {@code window} command results; a repeated {@code (clientId, seq)}
 * yields the previously cached result rather than re-applying the command.
 *
 * <p>{@link Long2ObjectHashMap} is used only for O(1) key lookup, never for
 * order-dependent iteration at runtime. Deterministic ordering is imposed
 * explicitly in {@link #forEachSorted(DedupRecordConsumer)} for snapshots.
 */
public final class DedupTable {

    private static final float LOAD_FACTOR = 0.65f;

    private final Long2ObjectHashMap<DedupRing> perClient;
    private final int window;

    public DedupTable(final int clientCapacity, final int window) {
        this.perClient = new Long2ObjectHashMap<>(clientCapacity, LOAD_FACTOR);
        this.window = window;
    }

    /** Returns the ring for a client, or {@code null} if the client is unseen. */
    public DedupRing ringFor(final long clientId) {
        return perClient.get(clientId);
    }

    /** Caches a command result, creating the client ring on first use. */
    public void store(
            final long clientId,
            final long seq,
            final long idHi,
            final long idLo,
            final short status,
            final long balance,
            final boolean hasBalance,
            final long allowance,
            final boolean hasAllowance) {
        DedupRing ring = perClient.get(clientId);
        if (ring == null) {
            ring = new DedupRing(window);
            perClient.put(clientId, ring);
        }
        ring.put(seq, idHi, idLo, status, balance, hasBalance, allowance, hasAllowance);
    }

    public int clientCount() {
        return perClient.size();
    }

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        perClient.clear();
    }

    /**
     * Emits every cached record in ascending {@code (clientId, seq)} order.
     *
     * <p>Cold snapshot path: key extraction and sorting are acceptable here.
     */
    public void forEachSorted(final DedupRecordConsumer consumer) {
        final long[] clientIds = new long[perClient.size()];
        final int[] cursor = {0};
        perClient.forEachLong((clientId, ring) -> clientIds[cursor[0]++] = clientId);
        Arrays.sort(clientIds);

        for (final long clientId : clientIds) {
            final DedupRing ring = perClient.get(clientId);
            final long[] seqs = new long[ring.capacity()];
            final int count = ring.occupiedSeqs(seqs);
            Arrays.sort(seqs, 0, count);
            for (int i = 0; i < count; i++) {
                final long seq = seqs[i];
                consumer.accept(
                        clientId,
                        seq,
                        ring.commandIdHi(seq),
                        ring.commandIdLo(seq),
                        ring.status(seq),
                        ring.resultBalance(seq),
                        ring.hasBalance(seq),
                        ring.resultAllowance(seq),
                        ring.hasAllowance(seq));
            }
        }
    }

    /** Primitive callback for deterministic dedup iteration. */
    @FunctionalInterface
    public interface DedupRecordConsumer {
        void accept(
                long clientId,
                long seq,
                long commandIdHi,
                long commandIdLo,
                short status,
                long resultBalance,
                boolean hasBalance,
                long resultAllowance,
                boolean hasAllowance);
    }
}
