package com.adbe.read;

/**
 * Aeron stream ids the read replica uses for ephemeral Archive replay delivery.
 *
 * <p>The two ids MUST be distinct so a snapshot replay and a live-log replay can
 * run concurrently on the replica's single embedded media driver without their
 * subscriptions colliding. They are delivery streams for the replica's own
 * subscriptions and are unrelated to the source recording streams (snapshot 106,
 * consensus log 100).
 */
final class ReadStreams {

    /** Delivery stream for a service snapshot (source stream 106) replay. */
    static final int SNAPSHOT_REPLAY = 42;

    /** Delivery stream for a consensus log (source stream 100) live replay. */
    static final int LIVE_LOG_REPLAY = 43;

    private ReadStreams() {}
}
