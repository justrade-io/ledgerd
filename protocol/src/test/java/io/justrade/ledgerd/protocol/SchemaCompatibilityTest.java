package io.justrade.ledgerd.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Pins the wire contract and verifies backward-compatible schema evolution. A
 * reader acting at an older schema version must decode the fields it knows and
 * resolve fields added in a later version to their null value, rather than
 * reading garbage. These tests encode at the current version and decode at an
 * older acting version to prove that contract, and assert the template and
 * schema identifiers that define the wire format do not drift.
 */
final class SchemaCompatibilityTest {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    @Test
    void commandEnvelopeAssetIdDefaultsBelowVersionTwo() {
        new CommandEnvelopeEncoder()
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(7L)
                .clientSeq(42L)
                .commandIdHi(7L)
                .commandIdLo(99L)
                .commandType(CommandType.CREDIT)
                .accountA(1001L)
                .accountB(CommandEnvelopeEncoder.accountBNullValue())
                .amount(500L)
                .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
                .accountC(CommandEnvelopeEncoder.accountCNullValue())
                .assetId(9L);

        final CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, CommandEnvelopeEncoder.BLOCK_LENGTH, 1);

        // Base fields present since version 0 decode normally.
        assertEquals(7L, decoder.clientId());
        assertEquals(1001L, decoder.accountA());
        assertEquals(500L, decoder.amount());
        // assetId was added in version 2; a version 1 reader sees its null value.
        assertEquals(CommandEnvelopeDecoder.assetIdNullValue(), decoder.assetId());
    }

    @Test
    void commandResultReservedDefaultsBelowVersionTwo() {
        new CommandResultEncoder()
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .commandIdHi(7L)
                .commandIdLo(99L)
                .status(StatusCode.SUCCESS)
                .resultBalance(601L)
                .resultAllowance(CommandResultEncoder.resultAllowanceNullValue())
                .resultReserved(50L);

        final CommandResultDecoder decoder = new CommandResultDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, CommandResultEncoder.BLOCK_LENGTH, 1);

        assertEquals(StatusCode.SUCCESS, decoder.status());
        assertEquals(601L, decoder.resultBalance());
        // resultReserved was added in version 2.
        assertEquals(CommandResultDecoder.resultReservedNullValue(), decoder.resultReserved());
    }

    @Test
    void snapshotHeaderCountsDefaultAcrossVersions() {
        new SnapshotHeaderEncoder()
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(987654L)
                .snapshotSchemaVer(5L)
                .balanceCount(3L)
                .allowanceCount(2L)
                .dedupCount(4L)
                .totalSupply(1000L)
                .assetCount(2L)
                .batchDedupCount(1L);

        // A version 1 reader knows neither assetCount (v2) nor batchDedupCount (v5).
        final SnapshotHeaderDecoder v1 = new SnapshotHeaderDecoder();
        v1.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, SnapshotHeaderEncoder.BLOCK_LENGTH, 1);
        assertEquals(3L, v1.balanceCount());
        assertEquals(1000L, v1.totalSupply());
        assertEquals(SnapshotHeaderDecoder.assetCountNullValue(), v1.assetCount());
        assertEquals(SnapshotHeaderDecoder.batchDedupCountNullValue(), v1.batchDedupCount());

        // A version 2 reader knows assetCount but still not batchDedupCount (v5).
        final SnapshotHeaderDecoder v2 = new SnapshotHeaderDecoder();
        v2.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, SnapshotHeaderEncoder.BLOCK_LENGTH, 2);
        assertEquals(2L, v2.assetCount());
        assertEquals(SnapshotHeaderDecoder.batchDedupCountNullValue(), v2.batchDedupCount());
    }

    @Test
    void balanceEntryAssetAndReservedDefaultBelowVersionTwo() {
        new BalanceEntryEncoder()
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .accountId(1001L)
                .balance(750L)
                .assetId(2L)
                .reserved(250L);

        final BalanceEntryDecoder decoder = new BalanceEntryDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, BalanceEntryEncoder.BLOCK_LENGTH, 1);

        assertEquals(1001L, decoder.accountId());
        assertEquals(750L, decoder.balance());
        assertEquals(BalanceEntryDecoder.assetIdNullValue(), decoder.assetId());
        assertEquals(BalanceEntryDecoder.reservedNullValue(), decoder.reserved());
    }

    @Test
    void templateAndSchemaIdentifiersArePinned() {
        assertEquals(1, CommandEnvelopeEncoder.TEMPLATE_ID);
        assertEquals(2, CommandResultEncoder.TEMPLATE_ID);
        assertEquals(3, TransferBatchEncoder.TEMPLATE_ID);
        assertEquals(4, TransferBatchResultEncoder.TEMPLATE_ID);
        assertEquals(10, SnapshotHeaderEncoder.TEMPLATE_ID);
        assertEquals(11, BalanceEntryEncoder.TEMPLATE_ID);
        assertEquals(12, AllowanceEntryEncoder.TEMPLATE_ID);
        assertEquals(13, DedupEntryEncoder.TEMPLATE_ID);
        assertEquals(14, SnapshotFooterEncoder.TEMPLATE_ID);
        assertEquals(15, AssetSupplyEntryEncoder.TEMPLATE_ID);
        assertEquals(16, BatchDedupEntryEncoder.TEMPLATE_ID);
        assertEquals(20, BalanceChangedEventEncoder.TEMPLATE_ID);
        assertEquals(26, CommandRejectedEventEncoder.TEMPLATE_ID);
        assertEquals(60, QueryRequestEncoder.TEMPLATE_ID);
        assertEquals(61, QueryResponseEncoder.TEMPLATE_ID);

        assertEquals(1, CommandEnvelopeEncoder.SCHEMA_ID);
        assertEquals(5, CommandEnvelopeEncoder.SCHEMA_VERSION);
    }
}
