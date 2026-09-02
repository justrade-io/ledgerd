package io.justrade.ledgerd.read.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Exercises the lock-free query gateway end to end without any Aeron: a producer
 * submits a request, a stand-in "service thread" drains it and publishes a
 * response, and the dispatcher completes the correlated callback.
 */
class ReadQueryGatewayTest {

    @Test
    @Timeout(10)
    void correlatesRequestToResponse() throws InterruptedException {
        try (ReadQueryGateway gateway = new ReadQueryGateway(1 << 16, 1 << 16)) {
            final long[] observed = {Long.MIN_VALUE};
            final boolean[] present = {false};
            final CountDownLatch done = new CountDownLatch(1);

            final long correlationId =
                    gateway.submit(QueryType.BALANCE, 0L, new long[] {42L}, 1, (buffer, offset, len) -> {
                        present[0] = QueryCodec.entryPresent(buffer, offset, 0);
                        observed[0] = QueryCodec.entryValue(buffer, offset, 0);
                        done.countDown();
                    });
            assertTrue(correlationId > 0, "correlation id assigned");

            // Stand in for the single service thread: decode the request, answer 777.
            final UnsafeBuffer response = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
            final MessageHandler serviceHandler = (msgTypeId, buffer, index, length) -> {
                final long id = QueryCodec.correlationId(buffer, index);
                assertEquals(42L, QueryCodec.operand(buffer, index, 0));
                QueryCodec.beginResponse(response, id, QueryType.BATCH_BALANCE, 1);
                QueryCodec.putEntry(response, 0, 777L, true);
                gateway.offerResponse(response, 0, QueryCodec.responseLength(1));
            };
            drainUntil(gateway, serviceHandler, done);

            assertTrue(done.await(5, TimeUnit.SECONDS), "callback completed");
            assertTrue(present[0], "balance present");
            assertEquals(777L, observed[0]);
            assertEquals(1L, gateway.completed());
        }
    }

    @Test
    @Timeout(10)
    void cancelPreventsCompletion() throws InterruptedException {
        try (ReadQueryGateway gateway = new ReadQueryGateway(1 << 16, 1 << 16)) {
            final AtomicBoolean called = new AtomicBoolean(false);
            final long correlationId = gateway.submit(
                    QueryType.TOTAL_SUPPLY, 0L, new long[0], 0, (buffer, offset, len) -> called.set(true));

            assertTrue(gateway.cancel(correlationId), "pending cancelled");

            // A late response for a cancelled request must be dropped as an orphan.
            final UnsafeBuffer response = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
            QueryCodec.beginResponse(response, correlationId, QueryType.TOTAL_SUPPLY, 1);
            QueryCodec.putEntry(response, 0, 5L, true);
            gateway.offerResponse(response, 0, QueryCodec.responseLength(1));

            Thread.sleep(200L);
            assertTrue(!called.get(), "cancelled callback not invoked");
            assertEquals(1L, gateway.orphanResponses());
        }
    }

    private static void drainUntil(
            final ReadQueryGateway gateway, final MessageHandler handler, final CountDownLatch done)
            throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (done.getCount() > 0 && System.nanoTime() < deadline) {
            if (gateway.readRequests(handler, 16) == 0) {
                Thread.sleep(1L);
            }
        }
    }

    @Test
    @Timeout(10)
    void rejectsWhenRequestRingFull() {
        // A small request ring with no service thread draining it fills quickly.
        try (ReadQueryGateway gateway = new ReadQueryGateway(1024, 1 << 16)) {
            boolean rejected = false;
            int accepted = 0;
            for (int i = 0; i < 100_000; i++) {
                final long id = gateway.submit(QueryType.BALANCE, 0L, new long[] {1L}, 1, (buffer, offset, len) -> {});
                if (id == ReadQueryGateway.NO_CAPACITY) {
                    rejected = true;
                    break;
                }
                accepted++;
            }
            assertTrue(rejected, "request ring must reject once full");
            assertTrue(accepted > 0, "some requests accepted before the ring filled");
            assertTrue(gateway.overloads() >= 1, "overload counter incremented on rejection");
        }
    }

    @Test
    void roundTripsQueryCodec() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        final long[] operands = {1L, 2L, 3L};
        final int len = QueryCodec.encodeRequest(buffer, 99L, QueryType.BATCH_BALANCE, 0L, operands, 3);
        assertTrue(len > 0);
        assertEquals(99L, QueryCodec.correlationId(buffer, 0));
        assertEquals(QueryType.BATCH_BALANCE, QueryCodec.queryType(buffer, 0));
        assertEquals(3, QueryCodec.count(buffer, 0));
        assertEquals(2L, QueryCodec.operand(buffer, 0, 1));

        final MutableDirectBuffer resp = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        QueryCodec.beginResponse(resp, 99L, QueryType.BATCH_BALANCE, 2);
        QueryCodec.putEntry(resp, 0, 500L, true);
        QueryCodec.putEntry(resp, 1, 0L, false);
        assertEquals(500L, QueryCodec.entryValue(resp, 0, 0));
        assertTrue(QueryCodec.entryPresent(resp, 0, 0));
        assertTrue(!QueryCodec.entryPresent(resp, 0, 1));
    }
}
