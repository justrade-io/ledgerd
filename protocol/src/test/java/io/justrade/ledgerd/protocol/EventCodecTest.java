package io.justrade.ledgerd.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trip encode then decode for the opt-in domain event journal messages
 * (ADR 0011). Every event shares the {@code (logPosition, timestamp, eventIndex)}
 * prefix so a consumer can peek the dedup key uniformly; the tests assert that
 * prefix plus each event's payload.
 */
final class EventCodecTest {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    @Test
    void balanceChangedEventRoundTrip() {
        final BalanceChangedEventEncoder encoder = new BalanceChangedEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(500L)
                .timestamp(1234L)
                .eventIndex(2)
                .assetId(1L)
                .accountId(100L)
                .newBalance(601L)
                .delta(-400L)
                .cause(EventCause.DEBIT);

        headerDecoder.wrap(buffer, 0);
        assertEquals(BalanceChangedEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
        final BalanceChangedEventDecoder decoder = new BalanceChangedEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(500L, decoder.logPosition());
        assertEquals(1234L, decoder.timestamp());
        assertEquals(2, decoder.eventIndex());
        assertEquals(1L, decoder.assetId());
        assertEquals(100L, decoder.accountId());
        assertEquals(601L, decoder.newBalance());
        assertEquals(-400L, decoder.delta());
        assertEquals(EventCause.DEBIT, decoder.cause());
    }

    @Test
    void reservedEventRoundTrip() {
        final ReservedEventEncoder encoder = new ReservedEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(501L)
                .timestamp(1235L)
                .eventIndex(0)
                .assetId(1L)
                .accountId(100L)
                .newAvailable(700L)
                .newReserved(300L);

        headerDecoder.wrap(buffer, 0);
        final ReservedEventDecoder decoder = new ReservedEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(501L, decoder.logPosition());
        assertEquals(1235L, decoder.timestamp());
        assertEquals(0, decoder.eventIndex());
        assertEquals(1L, decoder.assetId());
        assertEquals(100L, decoder.accountId());
        assertEquals(700L, decoder.newAvailable());
        assertEquals(300L, decoder.newReserved());
    }

    @Test
    void capturedEventRoundTrip() {
        final CapturedEventEncoder encoder = new CapturedEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(502L)
                .timestamp(1236L)
                .eventIndex(1)
                .assetId(1L)
                .accountId(100L)
                .newAvailable(700L)
                .newReserved(0L);

        headerDecoder.wrap(buffer, 0);
        final CapturedEventDecoder decoder = new CapturedEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(502L, decoder.logPosition());
        assertEquals(100L, decoder.accountId());
        assertEquals(700L, decoder.newAvailable());
        assertEquals(0L, decoder.newReserved());
    }

    @Test
    void releasedEventRoundTrip() {
        final ReleasedEventEncoder encoder = new ReleasedEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(503L)
                .timestamp(1237L)
                .eventIndex(0)
                .assetId(1L)
                .accountId(100L)
                .newAvailable(1000L)
                .newReserved(0L);

        headerDecoder.wrap(buffer, 0);
        final ReleasedEventDecoder decoder = new ReleasedEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(503L, decoder.logPosition());
        assertEquals(1000L, decoder.newAvailable());
        assertEquals(0L, decoder.newReserved());
    }

    @Test
    void transferEventRoundTrip() {
        final TransferEventEncoder encoder = new TransferEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(504L)
                .timestamp(1238L)
                .eventIndex(3)
                .assetId(1L)
                .fromAccount(100L)
                .toAccount(200L)
                .amount(400L);

        headerDecoder.wrap(buffer, 0);
        final TransferEventDecoder decoder = new TransferEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(504L, decoder.logPosition());
        assertEquals(3, decoder.eventIndex());
        assertEquals(1L, decoder.assetId());
        assertEquals(100L, decoder.fromAccount());
        assertEquals(200L, decoder.toAccount());
        assertEquals(400L, decoder.amount());
    }

    @Test
    void allowanceChangedEventRoundTrip() {
        final AllowanceChangedEventEncoder encoder = new AllowanceChangedEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(505L)
                .timestamp(1239L)
                .eventIndex(0)
                .assetId(1L)
                .ownerId(5L)
                .delegateId(6L)
                .newAllowance(400L);

        headerDecoder.wrap(buffer, 0);
        final AllowanceChangedEventDecoder decoder = new AllowanceChangedEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(505L, decoder.logPosition());
        assertEquals(5L, decoder.ownerId());
        assertEquals(6L, decoder.delegateId());
        assertEquals(400L, decoder.newAllowance());
    }

    @Test
    void commandRejectedEventRoundTrip() {
        final CommandRejectedEventEncoder encoder = new CommandRejectedEventEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(506L)
                .timestamp(1240L)
                .eventIndex(0)
                .assetId(1L)
                .accountId(100L)
                .amount(400L)
                .commandType(CommandType.DEBIT)
                .reason(StatusCode.INSUFFICIENT_BALANCE);

        headerDecoder.wrap(buffer, 0);
        assertEquals(CommandRejectedEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
        final CommandRejectedEventDecoder decoder = new CommandRejectedEventDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(506L, decoder.logPosition());
        assertEquals(100L, decoder.accountId());
        assertEquals(400L, decoder.amount());
        assertEquals(CommandType.DEBIT, decoder.commandType());
        assertEquals(StatusCode.INSUFFICIENT_BALANCE, decoder.reason());
    }
}
