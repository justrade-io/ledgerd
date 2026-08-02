package com.adbe.read;

import com.adbe.read.config.ReadReplicaConfig;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Owns the {@link AeronArchive} client for the read replica's ACTIVE Archive
 * endpoint and reconnects across an ordered list of endpoints (one per cluster
 * member). By ADR 0008 every member records the committed consensus log to its
 * own Archive, so failover needs no leader discovery: the replica uses the first
 * reachable endpoint and, on failure, advances to the next, round-robin.
 *
 * <p>Single-writer: this class owns no thread. The read replica node's single
 * agent thread calls {@link #connect()}, {@link #advance()}, and {@link #close()}
 * and reads {@link #archive()} from that one thread, so no concurrency control is
 * required.
 */
final class ArchiveSource implements AutoCloseable {

    private final Aeron aeron;
    private final String localHost;
    private final int controlStreamId;
    private final long messageTimeoutNs;
    private final String[] channels;

    private int index;
    private AeronArchive archive;

    ArchiveSource(final Aeron aeron, final ReadReplicaConfig config) {
        this.aeron = aeron;
        this.localHost = config.localHost();
        this.controlStreamId = config.archiveControlStreamId();
        this.messageTimeoutNs = TimeUnit.MILLISECONDS.toNanos(config.archiveMessageTimeoutMs());
        final List<String> configured = config.archiveControlChannels();
        this.channels = configured.toArray(new String[0]);
        this.index = 0;
    }

    /** The Archive control channel currently targeted (round-robin position). */
    String activeChannel() {
        return channels[index];
    }

    /** The connected Archive client, or {@code null} when not connected. */
    AeronArchive archive() {
        return archive;
    }

    boolean isConnected() {
        return archive != null;
    }

    /**
     * Attempts to connect to the current candidate endpoint. Idempotent when
     * already connected.
     *
     * @return {@code true} if a connection is now held; {@code false} if the
     *     endpoint was unreachable (the caller should {@link #advance()} and
     *     retry after a backoff).
     */
    boolean connect() {
        if (archive != null) {
            return true;
        }
        try {
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .messageTimeoutNs(messageTimeoutNs)
                    .controlRequestChannel(channels[index])
                    .controlResponseChannel("aeron:udp?endpoint=" + localHost + ":0")
                    .controlRequestStreamId(controlStreamId));
            return true;
        } catch (final RuntimeException e) {
            // Unreachable endpoint or a control-session handshake failure; treat as
            // a signal to advance to the next endpoint rather than a fatal error.
            archive = null;
            return false;
        }
    }

    /** Drops the current connection and moves to the next endpoint, round-robin. */
    void advance() {
        close();
        index = (index + 1) % channels.length;
    }

    @Override
    public void close() {
        if (archive != null) {
            try {
                archive.close();
            } catch (final RuntimeException ignored) {
                // Best-effort teardown: the source may already be dead.
            }
            archive = null;
        }
    }
}
