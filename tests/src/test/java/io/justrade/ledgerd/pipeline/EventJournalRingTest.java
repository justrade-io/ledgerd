package io.justrade.ledgerd.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.protocol.BalanceChangedEventDecoder;
import io.justrade.ledgerd.protocol.BalanceChangedEventEncoder;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trips SBE event records through the off-heap SPSC ring (ADR 0011):
 * encode, write, drain, decode, and assert the fields survive intact.
 */
class EventJournalRingTest {

    @Test
    void writeThenReadDecodesTheEventRecord() {
        final EventJournalRing ring = new EventJournalRing(4096);

        final UnsafeBuffer src = new UnsafeBuffer(new byte[64]);
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final BalanceChangedEventEncoder encoder = new BalanceChangedEventEncoder();
        encoder.wrapAndApplyHeader(src, 0, headerEncoder)
                .logPosition(1024L)
                .timestamp(999L)
                .eventIndex(2)
                .assetId(7L)
                .accountId(42L)
                .newBalance(350L)
                .delta(-150L)
                .cause(EventCause.TRANSFER_DEBIT);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

        assertTrue(ring.write(src, 0, length), "ring should accept the record");

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final BalanceChangedEventDecoder decoder = new BalanceChangedEventDecoder();
        final AtomicInteger read = new AtomicInteger();

        final int count = ring.read(
                (msgTypeId, buffer, index, len) -> {
                    read.incrementAndGet();
                    assertEquals(EventJournalRing.MSG_TYPE_ID, msgTypeId);
                    headerDecoder.wrap(buffer, index);
                    assertEquals(BalanceChangedEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
                    decoder.wrap(
                            buffer,
                            index + MessageHeaderDecoder.ENCODED_LENGTH,
                            headerDecoder.blockLength(),
                            headerDecoder.version());
                    assertEquals(1024L, decoder.logPosition());
                    assertEquals(999L, decoder.timestamp());
                    assertEquals(2, decoder.eventIndex());
                    assertEquals(7L, decoder.assetId());
                    assertEquals(42L, decoder.accountId());
                    assertEquals(350L, decoder.newBalance());
                    assertEquals(-150L, decoder.delta());
                    assertEquals(EventCause.TRANSFER_DEBIT, decoder.cause());
                },
                16);

        assertEquals(1, count);
        assertEquals(1, read.get());
    }

    @Test
    void readOnEmptyRingReturnsZero() {
        final EventJournalRing ring = new EventJournalRing(1024);
        assertEquals(0, ring.read((msgTypeId, buffer, index, len) -> {}, 16));
    }

    @Test
    void writeFailsWhenRingIsFull() {
        // Fill a small (but valid) ring with fixed-size records; once full, writes
        // are rejected rather than blocking, so the producer never stalls the
        // consensus thread.
        final EventJournalRing ring = new EventJournalRing(512);
        final UnsafeBuffer src = new UnsafeBuffer(new byte[64]);
        assertTrue(ring.write(src, 0, 60), "first write into an empty ring");
        boolean rejected = false;
        for (int i = 0; i < 100 && !rejected; i++) {
            rejected = !ring.write(src, 0, 60);
        }
        assertTrue(rejected, "a full ring must reject further writes");
    }
}
