package com.adbe.read.query;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Fixed, little-endian binary layout for query request and response messages
 * carried on the in-process ring buffers between the HTTP boundary threads and
 * the single service thread. All methods operate on caller-supplied buffers and
 * allocate nothing.
 *
 * <p>Request layout:
 *
 * <pre>
 *   offset 0  : long correlationId
 *   offset 8  : int  queryType code
 *   offset 12 : int  operandCount
 *   offset 16 : long operand[0..operandCount-1]
 * </pre>
 *
 * <p>Response layout:
 *
 * <pre>
 *   offset 0  : long correlationId
 *   offset 8  : int  queryType code
 *   offset 12 : int  entryCount
 *   offset 16 : { long value ; int present } * entryCount
 * </pre>
 */
public final class QueryCodec {

    /** Ring buffer message type id shared by request and response messages. */
    public static final int MSG_TYPE_ID = 1;

    /** Largest number of accounts a single batch-balance query may carry. */
    public static final int MAX_OPERANDS = 512;

    private static final int CORRELATION_OFFSET = 0;
    private static final int TYPE_OFFSET = 8;
    private static final int COUNT_OFFSET = 12;
    private static final int BODY_OFFSET = 16;

    private static final int OPERAND_SIZE = Long.BYTES;
    private static final int ENTRY_SIZE = Long.BYTES + Integer.BYTES;

    private QueryCodec() {}

    /** Maximum encoded length of any request or response for buffer sizing. */
    public static int maxMessageLength() {
        return BODY_OFFSET + MAX_OPERANDS * ENTRY_SIZE;
    }

    // --- Request ---------------------------------------------------------

    /** Encodes a request into {@code dst}; returns the encoded length in bytes. */
    public static int encodeRequest(
            final MutableDirectBuffer dst,
            final long correlationId,
            final QueryType type,
            final long[] operands,
            final int operandCount) {
        dst.putLong(CORRELATION_OFFSET, correlationId);
        dst.putInt(TYPE_OFFSET, type.code());
        dst.putInt(COUNT_OFFSET, operandCount);
        for (int i = 0; i < operandCount; i++) {
            dst.putLong(BODY_OFFSET + i * OPERAND_SIZE, operands[i]);
        }
        return BODY_OFFSET + operandCount * OPERAND_SIZE;
    }

    public static long correlationId(final DirectBuffer buffer, final int offset) {
        return buffer.getLong(offset + CORRELATION_OFFSET);
    }

    public static QueryType queryType(final DirectBuffer buffer, final int offset) {
        return QueryType.fromCode(buffer.getInt(offset + TYPE_OFFSET));
    }

    public static int count(final DirectBuffer buffer, final int offset) {
        return buffer.getInt(offset + COUNT_OFFSET);
    }

    public static long operand(final DirectBuffer buffer, final int offset, final int index) {
        return buffer.getLong(offset + BODY_OFFSET + index * OPERAND_SIZE);
    }

    // --- Response --------------------------------------------------------

    /** Writes the fixed response header and returns the offset of the first entry. */
    public static int beginResponse(
            final MutableDirectBuffer dst, final long correlationId, final QueryType type, final int entryCount) {
        dst.putLong(CORRELATION_OFFSET, correlationId);
        dst.putInt(TYPE_OFFSET, type.code());
        dst.putInt(COUNT_OFFSET, entryCount);
        return BODY_OFFSET;
    }

    /** Writes one response entry (value plus a presence flag) at {@code entryIndex}. */
    public static void putEntry(
            final MutableDirectBuffer dst, final int entryIndex, final long value, final boolean present) {
        final int at = BODY_OFFSET + entryIndex * ENTRY_SIZE;
        dst.putLong(at, value);
        dst.putInt(at + Long.BYTES, present ? 1 : 0);
    }

    /** Total encoded length of a response carrying {@code entryCount} entries. */
    public static int responseLength(final int entryCount) {
        return BODY_OFFSET + entryCount * ENTRY_SIZE;
    }

    public static long entryValue(final DirectBuffer buffer, final int offset, final int entryIndex) {
        return buffer.getLong(offset + BODY_OFFSET + entryIndex * ENTRY_SIZE);
    }

    public static boolean entryPresent(final DirectBuffer buffer, final int offset, final int entryIndex) {
        return buffer.getInt(offset + BODY_OFFSET + entryIndex * ENTRY_SIZE + Long.BYTES) != 0;
    }
}
