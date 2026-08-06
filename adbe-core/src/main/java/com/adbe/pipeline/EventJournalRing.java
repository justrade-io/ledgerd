package com.adbe.pipeline;

import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * Off-heap single-producer / single-consumer ring for the domain event journal
 * (ADR 0011). The single-writer service thread encodes SBE event records and
 * {@link #write writes} them; a dedicated journaler thread {@link #read drains}
 * them to an Aeron publication, off the consensus hot path.
 *
 * <p>Backed by an Agrona {@link OneToOneRingBuffer} over a direct buffer, so a
 * write is a bounded memcpy plus a release-ordered sequence advance: no lock, no
 * allocation. A full ring returns {@code false} from {@link #write} rather than
 * blocking, so the producer never stalls the consensus thread.
 */
public final class EventJournalRing {

    /** Ring message type id shared by every event record; the SBE header carries the real type. */
    public static final int MSG_TYPE_ID = 1;

    // OneToOneRingBuffer caps a single message at capacity / 8; a ring too small to
    // hold the largest event (64 bytes framed) would throw on the hot path instead
    // of signalling back-pressure, so require headroom for at least one record.
    private static final int MIN_CAPACITY = 8 * 64;

    private final OneToOneRingBuffer ring;

    /**
     * @param capacity ring capacity in bytes; must be a power of two (the direct
     *     buffer is sized {@code capacity + TRAILER_LENGTH}).
     */
    public EventJournalRing(final int capacity) {
        if (capacity < MIN_CAPACITY) {
            throw new IllegalArgumentException(
                    "event journal capacity must be >= " + MIN_CAPACITY + ", was: " + capacity);
        }
        this.ring = new OneToOneRingBuffer(
                new UnsafeBuffer(ByteBuffer.allocateDirect(capacity + RingBufferDescriptor.TRAILER_LENGTH)));
    }

    /** Writes one encoded event record; returns {@code false} if the ring is full. */
    public boolean write(final DirectBuffer buffer, final int offset, final int length) {
        return ring.write(MSG_TYPE_ID, buffer, offset, length);
    }

    /** Drains up to {@code limit} records to {@code handler}; returns the count read. */
    public int read(final MessageHandler handler, final int limit) {
        return ring.read(handler, limit);
    }
}
