package io.justrade.ledgerd.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trip encode then decode for the wire messages exchanged between the Edge
 * client and the cluster: commands, their results, and transfer batches. Each
 * message is framed with a {@link MessageHeaderEncoder} exactly as it is on the
 * wire, then decoded through the header, asserting every field survives.
 */
final class WireMessageCodecTest {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    @Test
    void commandEnvelopeRoundTrip() {
        final CommandEnvelopeEncoder encoder = new CommandEnvelopeEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(7L)
                .clientSeq(42L)
                .commandIdHi(7L)
                .commandIdLo(99L)
                .commandType(CommandType.TRANSFER)
                .accountA(1001L)
                .accountB(2002L)
                .amount(12345L)
                .correlationId(555L)
                .accountC(3003L)
                .assetId(9L);

        final CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();
        wrapBody(decoder, CommandEnvelopeDecoder.TEMPLATE_ID);

        assertEquals(7L, decoder.clientId());
        assertEquals(42L, decoder.clientSeq());
        assertEquals(7L, decoder.commandIdHi());
        assertEquals(99L, decoder.commandIdLo());
        assertEquals(CommandType.TRANSFER, decoder.commandType());
        assertEquals(1001L, decoder.accountA());
        assertEquals(2002L, decoder.accountB());
        assertEquals(12345L, decoder.amount());
        assertEquals(555L, decoder.correlationId());
        assertEquals(3003L, decoder.accountC());
        assertEquals(9L, decoder.assetId());
    }

    @Test
    void commandResultRoundTrip() {
        final CommandResultEncoder encoder = new CommandResultEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .commandIdHi(7L)
                .commandIdLo(99L)
                .status(StatusCode.SUCCESS)
                .resultBalance(601L)
                .resultAllowance(CommandResultEncoder.resultAllowanceNullValue())
                .resultReserved(50L);

        final CommandResultDecoder decoder = new CommandResultDecoder();
        wrapBody(decoder, CommandResultDecoder.TEMPLATE_ID);

        assertEquals(7L, decoder.commandIdHi());
        assertEquals(99L, decoder.commandIdLo());
        assertEquals(StatusCode.SUCCESS, decoder.status());
        assertEquals(601L, decoder.resultBalance());
        assertEquals(CommandResultDecoder.resultAllowanceNullValue(), decoder.resultAllowance());
        assertEquals(50L, decoder.resultReserved());
    }

    @Test
    void transferBatchRoundTripWithLinkedLegs() {
        final TransferBatchEncoder encoder = new TransferBatchEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(7L)
                .clientSeq(43L)
                .batchIdHi(7L)
                .batchIdLo(100L);
        final TransferBatchEncoder.LegsEncoder legs = encoder.legsCount(3);
        legs.next().fromId(1L).toId(2L).amount(10L).assetId(0L).linked((short) 1);
        legs.next().fromId(2L).toId(3L).amount(20L).assetId(0L).linked((short) 1);
        legs.next().fromId(3L).toId(4L).amount(30L).assetId(1L).linked((short) 0);

        final TransferBatchDecoder decoder = new TransferBatchDecoder();
        wrapBody(decoder, TransferBatchDecoder.TEMPLATE_ID);

        assertEquals(7L, decoder.clientId());
        assertEquals(43L, decoder.clientSeq());
        assertEquals(7L, decoder.batchIdHi());
        assertEquals(100L, decoder.batchIdLo());

        final TransferBatchDecoder.LegsDecoder legsDecoder = decoder.legs();
        assertEquals(3, legsDecoder.count());

        legsDecoder.next();
        assertEquals(1L, legsDecoder.fromId());
        assertEquals(2L, legsDecoder.toId());
        assertEquals(10L, legsDecoder.amount());
        assertEquals(0L, legsDecoder.assetId());
        assertEquals((short) 1, legsDecoder.linked());

        legsDecoder.next();
        assertEquals(2L, legsDecoder.fromId());
        assertEquals(3L, legsDecoder.toId());
        assertEquals(20L, legsDecoder.amount());
        assertEquals((short) 1, legsDecoder.linked());

        legsDecoder.next();
        assertEquals(3L, legsDecoder.fromId());
        assertEquals(4L, legsDecoder.toId());
        assertEquals(30L, legsDecoder.amount());
        assertEquals(1L, legsDecoder.assetId());
        assertEquals((short) 0, legsDecoder.linked());
    }

    @Test
    void transferBatchResultRoundTrip() {
        final TransferBatchResultEncoder encoder = new TransferBatchResultEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder).batchIdHi(7L).batchIdLo(100L);
        final TransferBatchResultEncoder.ResultsEncoder results = encoder.resultsCount(2);
        results.next().status(StatusCode.SUCCESS).hasBalance((short) 1).resultBalance(90L);
        results.next()
                .status(StatusCode.INSUFFICIENT_BALANCE)
                .hasBalance((short) 0)
                .resultBalance(0L);

        final TransferBatchResultDecoder decoder = new TransferBatchResultDecoder();
        wrapBody(decoder, TransferBatchResultDecoder.TEMPLATE_ID);

        assertEquals(7L, decoder.batchIdHi());
        assertEquals(100L, decoder.batchIdLo());

        final TransferBatchResultDecoder.ResultsDecoder resultsDecoder = decoder.results();
        assertEquals(2, resultsDecoder.count());

        resultsDecoder.next();
        assertEquals(StatusCode.SUCCESS, resultsDecoder.status());
        assertEquals((short) 1, resultsDecoder.hasBalance());
        assertEquals(90L, resultsDecoder.resultBalance());

        resultsDecoder.next();
        assertEquals(StatusCode.INSUFFICIENT_BALANCE, resultsDecoder.status());
        assertEquals((short) 0, resultsDecoder.hasBalance());
    }

    private void wrapBody(final CommandEnvelopeDecoder decoder, final int expectedTemplateId) {
        headerDecoder.wrap(buffer, 0);
        assertEquals(expectedTemplateId, headerDecoder.templateId());
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
    }

    private void wrapBody(final CommandResultDecoder decoder, final int expectedTemplateId) {
        headerDecoder.wrap(buffer, 0);
        assertEquals(expectedTemplateId, headerDecoder.templateId());
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
    }

    private void wrapBody(final TransferBatchDecoder decoder, final int expectedTemplateId) {
        headerDecoder.wrap(buffer, 0);
        assertEquals(expectedTemplateId, headerDecoder.templateId());
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
    }

    private void wrapBody(final TransferBatchResultDecoder decoder, final int expectedTemplateId) {
        headerDecoder.wrap(buffer, 0);
        assertEquals(expectedTemplateId, headerDecoder.templateId());
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
    }
}
