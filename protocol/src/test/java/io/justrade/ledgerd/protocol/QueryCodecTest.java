package io.justrade.ledgerd.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trip encode then decode for the read-side query protocol. These messages
 * exercise the two variable-shape SBE features the wire messages above do not: a
 * repeating group ({@code accountIds} / {@code balances}) and a variable-length
 * data field ({@code responseChannel}). SBE requires the group to be fully
 * consumed before the trailing var-data, so the decode order below is the
 * contract a consumer must follow.
 */
final class QueryCodecTest {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    @Test
    void queryRequestRoundTripWithGroupAndVarData() {
        final String responseChannel = "aeron:udp?endpoint=client:44001";
        final QueryRequestEncoder encoder = new QueryRequestEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .requestId(77L)
                .queryType(QueryType.BATCH_BALANCE)
                .assetId(3L)
                .accountId(QueryRequestEncoder.accountIdNullValue())
                .ownerId(QueryRequestEncoder.ownerIdNullValue())
                .delegateId(QueryRequestEncoder.delegateIdNullValue())
                .responseStreamId(101L);
        final QueryRequestEncoder.AccountIdsEncoder ids = encoder.accountIdsCount(3);
        ids.next().accountId(10L);
        ids.next().accountId(20L);
        ids.next().accountId(30L);
        encoder.responseChannel(responseChannel);

        headerDecoder.wrap(buffer, 0);
        assertEquals(QueryRequestDecoder.TEMPLATE_ID, headerDecoder.templateId());
        final QueryRequestDecoder decoder = new QueryRequestDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(77L, decoder.requestId());
        assertEquals(QueryType.BATCH_BALANCE, decoder.queryType());
        assertEquals(3L, decoder.assetId());
        assertEquals(101L, decoder.responseStreamId());

        final QueryRequestDecoder.AccountIdsDecoder idsDecoder = decoder.accountIds();
        assertEquals(3, idsDecoder.count());
        assertEquals(10L, idsDecoder.next().accountId());
        assertEquals(20L, idsDecoder.next().accountId());
        assertEquals(30L, idsDecoder.next().accountId());

        assertEquals(responseChannel, decoder.responseChannel());
    }

    @Test
    void queryResponseRoundTripWithBalancesGroup() {
        final QueryResponseEncoder encoder = new QueryResponseEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .requestId(77L)
                .queryType(QueryType.BATCH_BALANCE)
                .status(QueryStatusCode.SUCCESS)
                .appliedPosition(123456L)
                .assetId(3L)
                .accountId(QueryResponseEncoder.accountIdNullValue())
                .balance(QueryResponseEncoder.balanceNullValue())
                .exists(QueryResponseEncoder.existsNullValue())
                .ownerId(QueryResponseEncoder.ownerIdNullValue())
                .delegateId(QueryResponseEncoder.delegateIdNullValue())
                .allowance(QueryResponseEncoder.allowanceNullValue())
                .totalSupply(QueryResponseEncoder.totalSupplyNullValue());
        final QueryResponseEncoder.BalancesEncoder balances = encoder.balancesCount(2);
        balances.next().accountId(10L).balance(500L).exists((short) 1);
        balances.next().accountId(20L).balance(0L).exists((short) 0);

        headerDecoder.wrap(buffer, 0);
        assertEquals(QueryResponseDecoder.TEMPLATE_ID, headerDecoder.templateId());
        final QueryResponseDecoder decoder = new QueryResponseDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(77L, decoder.requestId());
        assertEquals(QueryType.BATCH_BALANCE, decoder.queryType());
        assertEquals(QueryStatusCode.SUCCESS, decoder.status());
        assertEquals(123456L, decoder.appliedPosition());
        assertEquals(3L, decoder.assetId());

        final QueryResponseDecoder.BalancesDecoder balancesDecoder = decoder.balances();
        assertEquals(2, balancesDecoder.count());

        balancesDecoder.next();
        assertEquals(10L, balancesDecoder.accountId());
        assertEquals(500L, balancesDecoder.balance());
        assertEquals((short) 1, balancesDecoder.exists());

        balancesDecoder.next();
        assertEquals(20L, balancesDecoder.accountId());
        assertEquals(0L, balancesDecoder.balance());
        assertEquals((short) 0, balancesDecoder.exists());
    }
}
