package com.adbe.read.config;

/**
 * Immutable configuration for a standby read node that replicates cluster state
 * via Aeron Archive snapshot replay, independent of the Raft consensus protocol.
 *
 * <p>The standby node connects to a cluster member's Archive, periodically
 * downloads the latest service snapshot, loads it into the balance engine, and
 * serves eventually-consistent reads over HTTP. It does NOT appear in the
 * cluster's {@code clusterMembers} string and does not affect quorum.
 */
public final class StandbyConfig {

    private final String archiveControlChannel;
    private final int archiveControlStreamId;
    private final int snapshotStreamId;
    private final int logStreamId;
    private final long pollIntervalMs;
    private final long replayTimeoutMs;
    private final boolean liveLogEnabled;

    private StandbyConfig(final Builder builder) {
        this.archiveControlChannel = builder.archiveControlChannel;
        this.archiveControlStreamId = builder.archiveControlStreamId;
        this.snapshotStreamId = builder.snapshotStreamId;
        this.logStreamId = builder.logStreamId;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.replayTimeoutMs = builder.replayTimeoutMs;
        this.liveLogEnabled = builder.liveLogEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static StandbyConfig defaults() {
        return builder().build();
    }

    /** Aeron Archive control channel endpoint (e.g. {@code aeron:udp?endpoint=host:20104}). */
    public String archiveControlChannel() {
        return archiveControlChannel;
    }

    /** Stream id for the Archive control protocol (default 10). */
    public int archiveControlStreamId() {
        return archiveControlStreamId;
    }

    /** Stream id for the clustered service snapshot recordings (default 106). */
    public int snapshotStreamId() {
        return snapshotStreamId;
    }

    /** Stream id for the consensus module log recordings (default 100). */
    public int logStreamId() {
        return logStreamId;
    }

    /** Interval in milliseconds between snapshot polls. */
    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    /** Maximum time to wait for a snapshot replay to complete. */
    public long replayTimeoutMs() {
        return replayTimeoutMs;
    }

    /** Whether to subscribe to the live consensus log between snapshots. */
    public boolean liveLogEnabled() {
        return liveLogEnabled;
    }

    /** Fluent builder for {@link StandbyConfig}. */
    public static final class Builder {
        private String archiveControlChannel = "aeron:udp?endpoint=localhost:20104";
        private int archiveControlStreamId = 10;
        private int snapshotStreamId = 106;
        private int logStreamId = 100;
        private long pollIntervalMs = 5_000L;
        private long replayTimeoutMs = 30_000L;
        private boolean liveLogEnabled = true;

        private Builder() {}

        public Builder archiveControlChannel(final String value) {
            this.archiveControlChannel = value;
            return this;
        }

        public Builder archiveControlStreamId(final int value) {
            this.archiveControlStreamId = value;
            return this;
        }

        public Builder snapshotStreamId(final int value) {
            this.snapshotStreamId = value;
            return this;
        }

        public Builder logStreamId(final int value) {
            this.logStreamId = value;
            return this;
        }

        public Builder pollIntervalMs(final long value) {
            this.pollIntervalMs = value;
            return this;
        }

        public Builder replayTimeoutMs(final long value) {
            this.replayTimeoutMs = value;
            return this;
        }

        public Builder liveLogEnabled(final boolean value) {
            this.liveLogEnabled = value;
            return this;
        }

        public StandbyConfig build() {
            if (archiveControlChannel == null || archiveControlChannel.isBlank()) {
                throw new IllegalArgumentException("archiveControlChannel is required");
            }
            return new StandbyConfig(this);
        }
    }
}
