package io.justrade.ledgerd.read.config;

import io.justrade.ledgerd.protocol.QueryStreams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable configuration for a read replica node that replicates cluster state
 * via Aeron Archive, independent of the Raft consensus protocol.
 *
 * <p>The read replica node connects to a cluster member's Archive, follows the
 * committed consensus log, loads service snapshots as they appear, and serves
 * eventually-consistent reads over HTTP. It does NOT appear in the cluster's
 * {@code clusterMembers} string and does not affect quorum.
 *
 * <p>The replica accepts an ORDERED list of Archive control channels (one per
 * cluster member) and fails over between them: by ADR 0008 every member records
 * the committed consensus log to its own Archive, so the replica uses the first
 * reachable endpoint and, on failure, moves to the next (round-robin with
 * backoff). A single channel remains supported for backward compatibility.
 */
public final class ReadReplicaConfig {

    private final List<String> archiveControlChannels;
    private final String localHost;
    private final String aeronDir;
    private final int archiveControlStreamId;
    private final int snapshotStreamId;
    private final int logStreamId;
    private final long pollIntervalMs;
    private final long replayTimeoutMs;
    private final boolean liveLogEnabled;
    private final long failoverBackoffMs;
    private final long archiveMessageTimeoutMs;
    private final long liveLogLivenessTimeoutMs;
    private final String queryRequestChannel;
    private final int queryRequestStreamId;

    private ReadReplicaConfig(final Builder builder) {
        this.archiveControlChannels = Collections.unmodifiableList(new ArrayList<>(builder.archiveControlChannels));
        this.localHost = builder.localHost;
        this.aeronDir = builder.aeronDir;
        this.archiveControlStreamId = builder.archiveControlStreamId;
        this.snapshotStreamId = builder.snapshotStreamId;
        this.logStreamId = builder.logStreamId;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.replayTimeoutMs = builder.replayTimeoutMs;
        this.liveLogEnabled = builder.liveLogEnabled;
        this.failoverBackoffMs = builder.failoverBackoffMs;
        this.archiveMessageTimeoutMs = builder.archiveMessageTimeoutMs;
        this.liveLogLivenessTimeoutMs = builder.liveLogLivenessTimeoutMs;
        this.queryRequestChannel = builder.queryRequestChannel;
        this.queryRequestStreamId = builder.queryRequestStreamId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ReadReplicaConfig defaults() {
        return builder().build();
    }

    /**
     * The ordered Aeron Archive control channels (e.g.
     * {@code aeron:udp?endpoint=host:20104}), one per cluster member. The replica
     * connects to the first reachable channel and fails over to the next on
     * failure, round-robin.
     */
    public List<String> archiveControlChannels() {
        return archiveControlChannels;
    }

    /** The first configured Archive control channel (convenience for logging). */
    public String archiveControlChannel() {
        return archiveControlChannels.get(0);
    }

    /**
     * The routable host the read replica binds its archive-facing subscriptions on
     * (the Archive control response channel and snapshot / log replays). The
     * Archive connects back to this host, so it must be reachable from the
     * Archive: {@code localhost} for same-host runs, or the container's network
     * address when the read replica and the cluster run on different hosts (e.g.
     * Docker). Default {@code localhost}.
     */
    public String localHost() {
        return localHost;
    }

    /**
     * Directory for this replica's embedded Aeron media driver. Must be unique per
     * replica process on a host so that co-located replicas do not share a driver
     * directory. Default {@code build/read-replica/driver}.
     */
    public String aeronDir() {
        return aeronDir;
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

    /** Delay before retrying after a failed Archive connect or a failover. */
    public long failoverBackoffMs() {
        return failoverBackoffMs;
    }

    /**
     * Aeron Archive control-message timeout. Bounds how long a dead Archive's
     * control session can block a {@code listRecordings} / {@code startReplay}
     * (and thus how long failover detection takes) and how long a connect to an
     * unreachable endpoint waits before giving up.
     */
    public long archiveMessageTimeoutMs() {
        return archiveMessageTimeoutMs;
    }

    /**
     * Backstop liveness timeout for the live log: if no fragments arrive AND no
     * snapshot poll succeeds within this window the source is presumed dead and a
     * failover is triggered. Covers a silently dead Archive that raises no
     * exception; a successful snapshot poll counts as activity, so an idle but
     * healthy cluster does not false-positive.
     */
    public long liveLogLivenessTimeoutMs() {
        return liveLogLivenessTimeoutMs;
    }

    /** The channel the read service subscribes to for {@code QueryRequest} frames. */
    public String queryRequestChannel() {
        return queryRequestChannel;
    }

    /** The stream id the read service subscribes to for {@code QueryRequest} frames. */
    public int queryRequestStreamId() {
        return queryRequestStreamId;
    }

    /** Fluent builder for {@link ReadReplicaConfig}. */
    public static final class Builder {
        private final List<String> archiveControlChannels = new ArrayList<>();
        private String localHost = "localhost";
        private String aeronDir = "build/read-replica/driver";
        private int archiveControlStreamId = 10;
        private int snapshotStreamId = 106;
        private int logStreamId = 100;
        private long pollIntervalMs = 5_000L;
        private long replayTimeoutMs = 30_000L;
        private boolean liveLogEnabled = true;
        private long failoverBackoffMs = 1_000L;
        private long archiveMessageTimeoutMs = 2_000L;
        private long liveLogLivenessTimeoutMs = 10_000L;
        private String queryRequestChannel = QueryStreams.QUERY_REQUEST_CHANNEL;
        private int queryRequestStreamId = QueryStreams.QUERY_REQUEST_STREAM_ID;

        private Builder() {
            archiveControlChannels.add("aeron:udp?endpoint=localhost:20104");
        }

        /** Sets a single Archive control channel (backward-compatible form). */
        public Builder archiveControlChannel(final String value) {
            this.archiveControlChannels.clear();
            this.archiveControlChannels.add(value);
            return this;
        }

        /** Sets the ordered Archive control channels, one per cluster member. */
        public Builder archiveControlChannels(final String... values) {
            this.archiveControlChannels.clear();
            Collections.addAll(this.archiveControlChannels, values);
            return this;
        }

        /** Sets the ordered Archive control channels, one per cluster member. */
        public Builder archiveControlChannels(final List<String> values) {
            this.archiveControlChannels.clear();
            this.archiveControlChannels.addAll(values);
            return this;
        }

        public Builder localHost(final String value) {
            this.localHost = value;
            return this;
        }

        public Builder aeronDir(final String value) {
            this.aeronDir = value;
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

        public Builder failoverBackoffMs(final long value) {
            this.failoverBackoffMs = value;
            return this;
        }

        public Builder archiveMessageTimeoutMs(final long value) {
            this.archiveMessageTimeoutMs = value;
            return this;
        }

        public Builder liveLogLivenessTimeoutMs(final long value) {
            this.liveLogLivenessTimeoutMs = value;
            return this;
        }

        public Builder queryRequestChannel(final String value) {
            this.queryRequestChannel = value;
            return this;
        }

        public Builder queryRequestStreamId(final int value) {
            this.queryRequestStreamId = value;
            return this;
        }

        public ReadReplicaConfig build() {
            if (archiveControlChannels.isEmpty()) {
                throw new IllegalArgumentException("at least one archiveControlChannel is required");
            }
            for (final String channel : archiveControlChannels) {
                if (channel == null || channel.isBlank()) {
                    throw new IllegalArgumentException("archiveControlChannel entries must be non-blank");
                }
            }
            return new ReadReplicaConfig(this);
        }
    }
}
