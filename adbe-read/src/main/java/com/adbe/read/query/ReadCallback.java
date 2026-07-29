package com.adbe.read.query;

import org.agrona.DirectBuffer;

/**
 * Invoked, on the dispatcher thread, when a query response is correlated back to
 * its originating request. The buffer is only valid for the duration of the call;
 * decode it with {@link QueryCodec} and hand the result off before returning.
 */
@FunctionalInterface
public interface ReadCallback {

    /**
     * @param buffer response buffer (valid only during this call)
     * @param offset offset of the response message within {@code buffer}
     * @param length length of the response message
     */
    void onResponse(DirectBuffer buffer, int offset, int length);
}
