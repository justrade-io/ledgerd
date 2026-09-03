package io.justrade.ledgerd.read.journal;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable configuration for an {@link EventJournalFollower}: the ordered
 * Archive endpoints to follow the event journal from, plus liveness and backoff
 * tuning. Mirrors the read replica's multi-archive model (ADR 0008): every
 * cluster member records its own event stream, so the follower uses the first
 * reachable endpoint and fails over to the next on failure.
 */
public final class EventJournalConfig {

    private final List<String> archiveControlChannels;
    private final String localHost;
    private final String aeronDir;
    private final int archiveControlStreamId;
    private final long replayTimeoutMs;
    private final long failoverBackoffMs;
    private final long archiveMessageTimeoutMs;
    private final long livenessTimeoutMs;

    private EventJournalConfig(final Builder builder) {
        this.archiveControlChannels = List.copyOf(builder.archiveControlChannels);
        this.localHost = builder.localHost;
        this.aeronDir = builder.aeronDir;
        this.archiveControlStreamId = builder.archiveControlStreamId;
        this.replayTimeoutMs = builder.replayTimeoutMs;
        this.failoverBackoffMs = builder.failoverBackoffMs;
        this.archiveMessageTimeoutMs = builder.archiveMessageTimeoutMs;
        this.livenessTimeoutMs = builder.livenessTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> archiveControlChannels() {
        return archiveControlChannels;
    }

    public String localHost() {
        return localHost;
    }

    public String aeronDir() {
        return aeronDir;
    }

    public int archiveControlStreamId() {
        return archiveControlStreamId;
    }

    public long replayTimeoutMs() {
        return replayTimeoutMs;
    }

    public long failoverBackoffMs() {
        return failoverBackoffMs;
    }

    public long archiveMessageTimeoutMs() {
        return archiveMessageTimeoutMs;
    }

    public long livenessTimeoutMs() {
        return livenessTimeoutMs;
    }

    /** Builder with localhost-friendly defaults. */
    public static final class Builder {
        private final List<String> archiveControlChannels = new ArrayList<>();
        private String localHost = "localhost";
        private String aeronDir = "build/event-follower/driver";
        private int archiveControlStreamId = 10;
        private long replayTimeoutMs = 10_000L;
        private long failoverBackoffMs = 250L;
        private long archiveMessageTimeoutMs = 10_000L;
        private long livenessTimeoutMs = 10_000L;

        /** Adds one Archive control channel; call once per cluster member. */
        public Builder archiveControlChannel(final String channel) {
            this.archiveControlChannels.add(channel);
            return this;
        }

        /** Sets the ordered Archive control channels, replacing any already added. */
        public Builder archiveControlChannels(final List<String> channels) {
            this.archiveControlChannels.clear();
            this.archiveControlChannels.addAll(channels);
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

        public Builder replayTimeoutMs(final long value) {
            this.replayTimeoutMs = value;
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

        public Builder livenessTimeoutMs(final long value) {
            this.livenessTimeoutMs = value;
            return this;
        }

        public EventJournalConfig build() {
            if (archiveControlChannels.isEmpty()) {
                throw new IllegalArgumentException("at least one archive control channel is required");
            }
            return new EventJournalConfig(this);
        }
    }
}
