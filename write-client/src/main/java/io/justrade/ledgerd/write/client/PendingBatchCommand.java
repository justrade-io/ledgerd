package io.justrade.ledgerd.write.client;

import org.agrona.concurrent.UnsafeBuffer;

/**
 * A pooled, reusable holder for an in-flight transfer batch: its pre-encoded
 * bytes (for idempotent resend), identity, and retry bookkeeping. Instances are
 * recycled by {@link WriteClient} so a batch submission does not allocate a
 * fresh buffer.
 */
final class PendingBatchCommand {

    /** Encoded batch bytes (header + legs), resent verbatim on retry. */
    final UnsafeBuffer buffer;

    final int poolIndex;

    int length;
    long batchIdHi;
    long batchIdLo;
    long submitNanos;
    long deadlineNanos;
    int retries;
    boolean inUse;

    PendingBatchCommand(final int poolIndex, final int bufferLength) {
        this.poolIndex = poolIndex;
        this.buffer = new UnsafeBuffer(new byte[bufferLength]);
    }

    void reset() {
        length = 0;
        batchIdHi = 0L;
        batchIdLo = 0L;
        submitNanos = 0L;
        deadlineNanos = 0L;
        retries = 0;
        inUse = false;
    }
}
