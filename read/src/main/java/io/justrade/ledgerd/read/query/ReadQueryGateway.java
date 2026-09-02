package io.justrade.ledgerd.read.query;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * Lock-free bridge between the many HTTP boundary threads and the single cluster
 * service thread.
 *
 * <ul>
 *   <li>Producers (Netty threads) call {@link #submit} to enqueue a request onto
 *       a {@link ManyToOneRingBuffer}.
 *   <li>The service thread drains requests via {@link #readRequests} inside
 *       {@code ClusteredService.doBackgroundWork}, looks up its own stores, and
 *       publishes answers via {@link #offerResponse} onto a
 *       {@link OneToOneRingBuffer}.
 *   <li>A dedicated dispatcher thread drains responses and completes the pending
 *       {@link ReadCallback} correlated by id.
 * </ul>
 *
 * <p>The service thread never touches shared mutable read state from another
 * thread: it only reads and writes ring buffers, so the single-writer discipline
 * of the deterministic state machine is preserved.
 */
public final class ReadQueryGateway implements AutoCloseable {

    /** Returned by {@link #submit} when the request ring is full. */
    public static final long NO_CAPACITY = -1L;

    private final ManyToOneRingBuffer requestRing;
    private final OneToOneRingBuffer responseRing;

    private final ConcurrentHashMap<Long, ReadCallback> pending = new ConcurrentHashMap<>();
    private final AtomicLong correlationSeq = new AtomicLong(1L);
    private final ThreadLocal<UnsafeBuffer> encodeBuffer =
            ThreadLocal.withInitial(() -> new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]));

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong overloads = new AtomicLong();
    private final AtomicLong orphanResponses = new AtomicLong();

    private final Thread dispatcher;
    private volatile boolean running = true;

    public ReadQueryGateway(final int requestCapacity, final int responseCapacity) {
        this.requestRing = new ManyToOneRingBuffer(
                new UnsafeBuffer(ByteBuffer.allocateDirect(requestCapacity + RingBufferDescriptor.TRAILER_LENGTH)));
        this.responseRing = new OneToOneRingBuffer(
                new UnsafeBuffer(ByteBuffer.allocateDirect(responseCapacity + RingBufferDescriptor.TRAILER_LENGTH)));

        this.dispatcher = new Thread(this::runDispatcher, "ledgerd-read-dispatcher");
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    /**
     * Enqueues a query. Returns the assigned correlation id, or {@link #NO_CAPACITY}
     * if the request ring is full (the caller should reject with backpressure).
     */
    public long submit(
            final QueryType type,
            final long assetId,
            final long[] operands,
            final int operandCount,
            final ReadCallback callback) {
        final long correlationId = correlationSeq.getAndIncrement();
        pending.put(correlationId, callback);

        final UnsafeBuffer buffer = encodeBuffer.get();
        final int length = QueryCodec.encodeRequest(buffer, correlationId, type, assetId, operands, operandCount);
        if (!requestRing.write(QueryCodec.MSG_TYPE_ID, buffer, 0, length)) {
            pending.remove(correlationId);
            overloads.incrementAndGet();
            return NO_CAPACITY;
        }
        submitted.incrementAndGet();
        return correlationId;
    }

    /** Removes a pending callback without completing it (used on client timeout). */
    public boolean cancel(final long correlationId) {
        return pending.remove(correlationId) != null;
    }

    /** Drains up to {@code limit} pending requests on the service thread. */
    public int readRequests(final MessageHandler handler, final int limit) {
        return requestRing.read(handler, limit);
    }

    /** Publishes an encoded response from the service thread. */
    public boolean offerResponse(final UnsafeBuffer buffer, final int offset, final int length) {
        return responseRing.write(QueryCodec.MSG_TYPE_ID, buffer, offset, length);
    }

    private void runDispatcher() {
        final IdleStrategy idleStrategy = new BackoffIdleStrategy();
        final MessageHandler handler = this::onResponse;
        while (running) {
            final int work = responseRing.read(handler, 64);
            idleStrategy.idle(work);
        }
    }

    private void onResponse(
            final int msgTypeId, final org.agrona.MutableDirectBuffer buffer, final int index, final int length) {
        final long correlationId = QueryCodec.correlationId(buffer, index);
        final ReadCallback callback = pending.remove(correlationId);
        if (callback == null) {
            orphanResponses.incrementAndGet();
            return;
        }
        completed.incrementAndGet();
        callback.onResponse(buffer, index, length);
    }

    public long submitted() {
        return submitted.get();
    }

    public long completed() {
        return completed.get();
    }

    public long overloads() {
        return overloads.get();
    }

    public long orphanResponses() {
        return orphanResponses.get();
    }

    public int pendingCount() {
        return pending.size();
    }

    @Override
    public void close() {
        running = false;
        dispatcher.interrupt();
        try {
            dispatcher.join(1_000L);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
