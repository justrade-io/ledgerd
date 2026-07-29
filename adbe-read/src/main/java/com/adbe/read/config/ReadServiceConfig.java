package com.adbe.read.config;

import com.adbe.read.query.QueryCodec;

/**
 * Immutable configuration for the read service: the HTTP boundary port, the
 * lock-free ring capacities between the boundary and the service thread, the
 * per-request timeout, and the maximum batch size.
 */
public final class ReadServiceConfig {

    private final int httpPort;
    private final int requestRingCapacity;
    private final int responseRingCapacity;
    private final long requestTimeoutMs;
    private final int maxBatchSize;

    private ReadServiceConfig(final Builder builder) {
        this.httpPort = builder.httpPort;
        this.requestRingCapacity = builder.requestRingCapacity;
        this.responseRingCapacity = builder.responseRingCapacity;
        this.requestTimeoutMs = builder.requestTimeoutMs;
        this.maxBatchSize = builder.maxBatchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ReadServiceConfig defaults() {
        return builder().build();
    }

    public int httpPort() {
        return httpPort;
    }

    public int requestRingCapacity() {
        return requestRingCapacity;
    }

    public int responseRingCapacity() {
        return responseRingCapacity;
    }

    public long requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }

    /** Fluent builder with defaults suitable for local runs and tests. */
    public static final class Builder {
        private int httpPort = 8080;
        private int requestRingCapacity = 1 << 20;
        private int responseRingCapacity = 1 << 20;
        private long requestTimeoutMs = 5_000L;
        private int maxBatchSize = QueryCodec.MAX_OPERANDS;

        private Builder() {}

        public Builder httpPort(final int value) {
            this.httpPort = value;
            return this;
        }

        public Builder requestRingCapacity(final int value) {
            this.requestRingCapacity = value;
            return this;
        }

        public Builder responseRingCapacity(final int value) {
            this.responseRingCapacity = value;
            return this;
        }

        public Builder requestTimeoutMs(final long value) {
            this.requestTimeoutMs = value;
            return this;
        }

        public Builder maxBatchSize(final int value) {
            if (value < 1 || value > QueryCodec.MAX_OPERANDS) {
                throw new IllegalArgumentException("maxBatchSize must be in [1, " + QueryCodec.MAX_OPERANDS + "]");
            }
            this.maxBatchSize = value;
            return this;
        }

        public ReadServiceConfig build() {
            return new ReadServiceConfig(this);
        }
    }
}
