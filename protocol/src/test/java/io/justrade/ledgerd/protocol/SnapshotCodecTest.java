package io.justrade.ledgerd.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trip encode then decode for the records that make up a snapshot stream:
 * header, per-asset supply, balances, allowances, single-command and batch dedup
 * entries, and the integrity footer. These are the records
 * {@code SnapshotManager} writes and reads, so a codec regression here would
 * corrupt recovery silently.
 */
final class SnapshotCodecTest {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    @Test
    void snapshotHeaderRoundTrip() {
        final SnapshotHeaderEncoder encoder = new SnapshotHeaderEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(987654L)
                .snapshotSchemaVer(5L)
                .balanceCount(3L)
                .allowanceCount(2L)
                .dedupCount(4L)
                .totalSupply(1000L)
                .assetCount(2L)
                .batchDedupCount(1L);

        headerDecoder.wrap(buffer, 0);
        assertEquals(SnapshotHeaderDecoder.TEMPLATE_ID, headerDecoder.templateId());
        final SnapshotHeaderDecoder decoder = new SnapshotHeaderDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(987654L, decoder.logPosition());
        assertEquals(5L, decoder.snapshotSchemaVer());
        assertEquals(3L, decoder.balanceCount());
        assertEquals(2L, decoder.allowanceCount());
        assertEquals(4L, decoder.dedupCount());
        assertEquals(1000L, decoder.totalSupply());
        assertEquals(2L, decoder.assetCount());
        assertEquals(1L, decoder.batchDedupCount());
    }

    @Test
    void balanceEntryRoundTrip() {
        final BalanceEntryEncoder encoder = new BalanceEntryEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .accountId(1001L)
                .balance(750L)
                .assetId(2L)
                .reserved(250L);

        headerDecoder.wrap(buffer, 0);
        final BalanceEntryDecoder decoder = new BalanceEntryDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(1001L, decoder.accountId());
        assertEquals(750L, decoder.balance());
        assertEquals(2L, decoder.assetId());
        assertEquals(250L, decoder.reserved());
    }

    @Test
    void allowanceEntryRoundTrip() {
        final AllowanceEntryEncoder encoder = new AllowanceEntryEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .ownerId(5L)
                .delegateId(6L)
                .allowance(400L)
                .assetId(2L);

        headerDecoder.wrap(buffer, 0);
        final AllowanceEntryDecoder decoder = new AllowanceEntryDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(5L, decoder.ownerId());
        assertEquals(6L, decoder.delegateId());
        assertEquals(400L, decoder.allowance());
        assertEquals(2L, decoder.assetId());
    }

    @Test
    void dedupEntryRoundTrip() {
        final DedupEntryEncoder encoder = new DedupEntryEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(7L)
                .clientSeq(42L)
                .commandIdHi(7L)
                .commandIdLo(99L)
                .status(StatusCode.SUCCESS)
                .resultBalance(601L)
                .resultAllowance(DedupEntryEncoder.resultAllowanceNullValue())
                .resultReserved(50L);

        headerDecoder.wrap(buffer, 0);
        final DedupEntryDecoder decoder = new DedupEntryDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(7L, decoder.clientId());
        assertEquals(42L, decoder.clientSeq());
        assertEquals(7L, decoder.commandIdHi());
        assertEquals(99L, decoder.commandIdLo());
        assertEquals(StatusCode.SUCCESS, decoder.status());
        assertEquals(601L, decoder.resultBalance());
        assertEquals(DedupEntryDecoder.resultAllowanceNullValue(), decoder.resultAllowance());
        assertEquals(50L, decoder.resultReserved());
    }

    @Test
    void assetSupplyEntryRoundTrip() {
        final AssetSupplyEntryEncoder encoder = new AssetSupplyEntryEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder).assetId(2L).totalSupply(1000L);

        headerDecoder.wrap(buffer, 0);
        final AssetSupplyEntryDecoder decoder = new AssetSupplyEntryDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(2L, decoder.assetId());
        assertEquals(1000L, decoder.totalSupply());
    }

    @Test
    void batchDedupEntryRoundTripWithLegsGroup() {
        final BatchDedupEntryEncoder encoder = new BatchDedupEntryEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(7L)
                .clientSeq(43L)
                .batchIdHi(7L)
                .batchIdLo(100L);
        final BatchDedupEntryEncoder.LegsEncoder legs = encoder.legsCount(2);
        legs.next().status(StatusCode.SUCCESS).hasBalance((short) 1).resultBalance(90L);
        legs.next().status(StatusCode.INVALID_CHAIN).hasBalance((short) 0).resultBalance(0L);

        headerDecoder.wrap(buffer, 0);
        final BatchDedupEntryDecoder decoder = new BatchDedupEntryDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(7L, decoder.clientId());
        assertEquals(43L, decoder.clientSeq());
        assertEquals(7L, decoder.batchIdHi());
        assertEquals(100L, decoder.batchIdLo());

        final BatchDedupEntryDecoder.LegsDecoder legsDecoder = decoder.legs();
        assertEquals(2, legsDecoder.count());

        legsDecoder.next();
        assertEquals(StatusCode.SUCCESS, legsDecoder.status());
        assertEquals((short) 1, legsDecoder.hasBalance());
        assertEquals(90L, legsDecoder.resultBalance());

        legsDecoder.next();
        assertEquals(StatusCode.INVALID_CHAIN, legsDecoder.status());
        assertEquals((short) 0, legsDecoder.hasBalance());
    }

    @Test
    void snapshotFooterRoundTrip() {
        final SnapshotFooterEncoder encoder = new SnapshotFooterEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder).checksum(0x0123456789ABCDEFL);

        headerDecoder.wrap(buffer, 0);
        assertEquals(SnapshotFooterDecoder.TEMPLATE_ID, headerDecoder.templateId());
        final SnapshotFooterDecoder decoder = new SnapshotFooterDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(0x0123456789ABCDEFL, decoder.checksum());
    }
}
