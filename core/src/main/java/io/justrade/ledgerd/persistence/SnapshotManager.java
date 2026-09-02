package io.justrade.ledgerd.persistence;

import io.justrade.ledgerd.collections.AllowanceStore;
import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.collections.DedupTable;
import io.justrade.ledgerd.protocol.AllowanceEntryDecoder;
import io.justrade.ledgerd.protocol.AllowanceEntryEncoder;
import io.justrade.ledgerd.protocol.AssetSupplyEntryDecoder;
import io.justrade.ledgerd.protocol.AssetSupplyEntryEncoder;
import io.justrade.ledgerd.protocol.BalanceEntryDecoder;
import io.justrade.ledgerd.protocol.BalanceEntryEncoder;
import io.justrade.ledgerd.protocol.DedupEntryDecoder;
import io.justrade.ledgerd.protocol.DedupEntryEncoder;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.SnapshotFooterDecoder;
import io.justrade.ledgerd.protocol.SnapshotFooterEncoder;
import io.justrade.ledgerd.protocol.SnapshotHeaderDecoder;
import io.justrade.ledgerd.protocol.SnapshotHeaderEncoder;
import io.justrade.ledgerd.protocol.StatusCode;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Serialises and restores engine state as a sequence of self-describing SBE
 * records: a header, then balances, allowances, and dedup records in
 * deterministic key order, terminated by a footer carrying a checksum.
 *
 * <p>Records are emitted one at a time into a small reusable buffer and handed
 * to a {@link SnapshotSink}, so the writer never allocates a dataset-sized
 * buffer. On load, records are fed one at a time to {@link #onRecord} in the
 * same order they were written.
 *
 * <p>Deterministic ordering is guaranteed by the stores' {@code forEachSorted}
 * methods, which is mandatory for byte-identical snapshots across nodes.
 */
public final class SnapshotManager {

    private static final int MAX_RECORD_LENGTH = 128;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SnapshotHeaderEncoder snapshotHeaderEncoder = new SnapshotHeaderEncoder();
    private final BalanceEntryEncoder balanceEncoder = new BalanceEntryEncoder();
    private final AllowanceEntryEncoder allowanceEncoder = new AllowanceEntryEncoder();
    private final AssetSupplyEntryEncoder assetSupplyEncoder = new AssetSupplyEntryEncoder();
    private final DedupEntryEncoder dedupEncoder = new DedupEntryEncoder();
    private final SnapshotFooterEncoder footerEncoder = new SnapshotFooterEncoder();
    private final UnsafeBuffer recordBuffer = new UnsafeBuffer(new byte[MAX_RECORD_LENGTH]);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SnapshotHeaderDecoder snapshotHeaderDecoder = new SnapshotHeaderDecoder();
    private final BalanceEntryDecoder balanceDecoder = new BalanceEntryDecoder();
    private final AllowanceEntryDecoder allowanceDecoder = new AllowanceEntryDecoder();
    private final AssetSupplyEntryDecoder assetSupplyDecoder = new AssetSupplyEntryDecoder();
    private final DedupEntryDecoder dedupDecoder = new DedupEntryDecoder();
    private final SnapshotFooterDecoder footerDecoder = new SnapshotFooterDecoder();

    // Load-side targets, set once before load begins.
    private BalanceStore loadBalances;
    private AllowanceStore loadAllowances;
    private DedupTable loadDedup;
    private long computedChecksum;
    private long expectedAggregateSupply;
    private boolean footerSeen;
    private long loadedLogPosition;

    /** Receives one encoded snapshot record. */
    @FunctionalInterface
    public interface SnapshotSink {
        void accept(DirectBuffer buffer, int offset, int length);
    }

    /**
     * Writes a full snapshot to the sink in deterministic order.
     *
     * @param idler invoked once per record so a live publication can honour
     *     back-pressure via {@code Cluster.idle()}; may be a no-op in tests.
     */
    public void write(
            final SnapshotSink sink,
            final Runnable idler,
            final BalanceStore balances,
            final AllowanceStore allowances,
            final DedupTable dedup,
            final long logPosition) {
        final long checksum = checksumOf(balances);

        int len = snapshotHeaderEncoder
                .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                .logPosition(logPosition)
                .snapshotSchemaVer(SnapshotHeaderEncoder.SCHEMA_VERSION)
                .balanceCount(balances.size())
                .allowanceCount(allowances.ownerCount())
                .dedupCount(dedup.clientCount())
                .totalSupply(balances.aggregateSupply())
                .assetCount(balances.assetCount())
                .encodedLength();
        emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + len);

        balances.forEachSupplySorted((assetId, supply) -> {
            final int l = assetSupplyEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .assetId(assetId)
                    .totalSupply(supply)
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        balances.forEachSorted((assetId, accountId, balance, reserved) -> {
            final int l = balanceEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .accountId(accountId)
                    .balance(balance)
                    .assetId(assetId)
                    .reserved(reserved)
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        allowances.forEachSorted((assetId, ownerId, delegateId, allowance) -> {
            final int l = allowanceEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .ownerId(ownerId)
                    .delegateId(delegateId)
                    .allowance(allowance)
                    .assetId(assetId)
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        dedup.forEachSorted(
                (clientId, seq, cmdHi, cmdLo, status, resBal, hasBal, resAllow, hasAllow, resReserved, hasReserved) -> {
                    dedupEncoder
                            .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                            .clientId(clientId)
                            .clientSeq(seq)
                            .commandIdHi(cmdHi)
                            .commandIdLo(cmdLo)
                            .status(StatusCode.get(status));
                    if (hasBal) {
                        dedupEncoder.resultBalance(resBal);
                    }
                    if (hasAllow) {
                        dedupEncoder.resultAllowance(resAllow);
                    }
                    if (hasReserved) {
                        dedupEncoder.resultReserved(resReserved);
                    }
                    emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + dedupEncoder.encodedLength());
                });

        len = footerEncoder
                .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                .checksum(checksum)
                .encodedLength();
        emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + len);
    }

    private void emit(final SnapshotSink sink, final Runnable idler, final int length) {
        sink.accept(recordBuffer, 0, length);
        idler.run();
    }

    /** Prepares the manager to receive records for the given (cleared) stores. */
    public void beginLoad(final BalanceStore balances, final AllowanceStore allowances, final DedupTable dedup) {
        balances.clear();
        allowances.clear();
        dedup.clear();
        this.loadBalances = balances;
        this.loadAllowances = allowances;
        this.loadDedup = dedup;
        this.computedChecksum = 0L;
        this.expectedAggregateSupply = 0L;
        this.footerSeen = false;
    }

    /** Decodes and applies a single snapshot record. */
    public void onRecord(final DirectBuffer buffer, final int offset) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();
        final int blockLength = headerDecoder.blockLength();
        final int version = headerDecoder.version();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId) {
            case SnapshotHeaderDecoder.TEMPLATE_ID -> {
                snapshotHeaderDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadedLogPosition = snapshotHeaderDecoder.logPosition();
                expectedAggregateSupply = snapshotHeaderDecoder.totalSupply();
                // Pre-2 snapshots carry a single aggregate supply and no per-asset
                // records; seed the default asset so they load unchanged. Version-2
                // snapshots overwrite this via AssetSupplyEntry records that follow.
                loadBalances.setSupply(BalanceStore.DEFAULT_ASSET, snapshotHeaderDecoder.totalSupply());
            }
            case AssetSupplyEntryDecoder.TEMPLATE_ID -> {
                assetSupplyDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadBalances.setSupply(assetSupplyDecoder.assetId(), assetSupplyDecoder.totalSupply());
            }
            case BalanceEntryDecoder.TEMPLATE_ID -> {
                balanceDecoder.wrap(buffer, bodyOffset, blockLength, version);
                final long balance = balanceDecoder.balance();
                final long accountId = balanceDecoder.accountId();
                long assetId = balanceDecoder.assetId();
                if (assetId == BalanceEntryDecoder.assetIdNullValue()) {
                    assetId = BalanceStore.DEFAULT_ASSET;
                }
                long reserved = balanceDecoder.reserved();
                if (reserved == BalanceEntryDecoder.reservedNullValue()) {
                    reserved = 0L;
                }
                loadBalances.set(assetId, accountId, balance);
                if (reserved != 0L) {
                    loadBalances.setReserved(assetId, accountId, reserved);
                }
                computedChecksum += balance + reserved;
            }
            case AllowanceEntryDecoder.TEMPLATE_ID -> {
                allowanceDecoder.wrap(buffer, bodyOffset, blockLength, version);
                long assetId = allowanceDecoder.assetId();
                if (assetId == AllowanceEntryDecoder.assetIdNullValue()) {
                    assetId = BalanceStore.DEFAULT_ASSET;
                }
                loadAllowances.set(
                        assetId,
                        allowanceDecoder.ownerId(),
                        allowanceDecoder.delegateId(),
                        allowanceDecoder.allowance());
            }
            case DedupEntryDecoder.TEMPLATE_ID -> {
                dedupDecoder.wrap(buffer, bodyOffset, blockLength, version);
                final long resBal = dedupDecoder.resultBalance();
                final long resAllow = dedupDecoder.resultAllowance();
                final long resReserved = dedupDecoder.resultReserved();
                final boolean hasBal = resBal != DedupEntryDecoder.resultBalanceNullValue();
                final boolean hasAllow = resAllow != DedupEntryDecoder.resultAllowanceNullValue();
                final boolean hasReserved = resReserved != DedupEntryDecoder.resultReservedNullValue();
                loadDedup.store(
                        dedupDecoder.clientId(),
                        dedupDecoder.clientSeq(),
                        dedupDecoder.commandIdHi(),
                        dedupDecoder.commandIdLo(),
                        dedupDecoder.status().value(),
                        hasBal ? resBal : 0L,
                        hasBal,
                        hasAllow ? resAllow : 0L,
                        hasAllow,
                        hasReserved ? resReserved : 0L,
                        hasReserved);
            }
            case SnapshotFooterDecoder.TEMPLATE_ID -> {
                footerDecoder.wrap(buffer, bodyOffset, blockLength, version);
                footerSeen = true;
            }
            default -> throw new IllegalStateException("Unknown snapshot template id: " + templateId);
        }
    }

    /** Returns {@code true} once the terminating footer has been applied. */
    public boolean loadComplete() {
        return footerSeen;
    }

    /** The log position decoded from the snapshot header, or 0 if not yet loaded. */
    public long loadedLogPosition() {
        return loadedLogPosition;
    }

    /** Verifies that the restored balances reproduce the aggregate total supply. */
    public boolean verifyInvariant() {
        return footerSeen && computedChecksum == expectedAggregateSupply;
    }

    private static long checksumOf(final BalanceStore balances) {
        final long[] sum = {0L};
        balances.forEachSorted((assetId, accountId, balance, reserved) -> sum[0] += balance + reserved);
        return sum[0];
    }

    /** Length of the small reusable per-record buffer. */
    public static int maxRecordLength() {
        return MAX_RECORD_LENGTH;
    }

    /** Exposes the reusable record buffer for callers that copy before offering. */
    public MutableDirectBuffer recordBuffer() {
        return recordBuffer;
    }
}
