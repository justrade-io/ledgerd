package com.adbe.testkit;

import com.adbe.core.BalanceEngine;
import com.adbe.persistence.SnapshotManager;
import java.util.Arrays;
import org.agrona.ExpandableArrayBuffer;

/**
 * Serialises engine state into an in-memory, length-prefixed record stream and
 * restores it, standing in for an Aeron snapshot publication/image in unit tests.
 */
public final class InMemorySnapshot {

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private int length;

    /** Writes a full snapshot of {@code engine} into the in-memory buffer. */
    public void writeFrom(final SnapshotManager manager, final BalanceEngine engine, final long logPosition) {
        length = 0;
        engine.writeSnapshot(
                manager,
                (recordBuffer, recordOffset, recordLength) -> {
                    buffer.putInt(length, recordLength);
                    length += Integer.BYTES;
                    buffer.putBytes(length, recordBuffer, recordOffset, recordLength);
                    length += recordLength;
                },
                () -> {},
                logPosition);
    }

    /** Restores a previously written snapshot into {@code engine}. */
    public void readInto(final SnapshotManager manager, final BalanceEngine engine) {
        engine.beginSnapshotLoad(manager);
        int offset = 0;
        while (offset < length && !manager.loadComplete()) {
            final int recordLength = buffer.getInt(offset);
            offset += Integer.BYTES;
            manager.onRecord(buffer, offset);
            offset += recordLength;
        }
    }

    /** Returns a copy of the raw serialized bytes for byte-identical comparisons. */
    public byte[] toByteArray() {
        final byte[] out = new byte[length];
        buffer.getBytes(0, out, 0, length);
        return out;
    }

    public int length() {
        return length;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InMemorySnapshot other)) {
            return false;
        }
        return length == other.length && Arrays.equals(toByteArray(), other.toByteArray());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(toByteArray());
    }
}
