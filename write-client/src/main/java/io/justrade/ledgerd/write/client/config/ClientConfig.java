package io.justrade.ledgerd.write.client.config;

import java.util.concurrent.TimeUnit;

/**
 * Immutable configuration for an {@link io.justrade.ledgerd.write.client.WriteClient}.
 *
 * <p>When {@code aeronDirectoryName} is {@code null}, the client launches its own
 * embedded media driver, so it survives the shutdown of any individual cluster
 * node.
 */
public final class ClientConfig {

    private final long clientId;
    private final String ingressEndpoints;
    private final String aeronDirectoryName;
    private final String egressChannel;
    private final long messageTimeoutNs;
    private final long retryBackoffNs;
    private final int maxRetries;
    private final int maxInFlight;
    private final int maxBatchSize;
    private final int maxBatchInFlight;

    private ClientConfig(final Builder builder) {
        this.clientId = builder.clientId;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.aeronDirectoryName = builder.aeronDirectoryName;
        this.egressChannel = builder.egressChannel;
        this.messageTimeoutNs = builder.messageTimeoutNs;
        this.retryBackoffNs = builder.retryBackoffNs;
        this.maxRetries = builder.maxRetries;
        this.maxInFlight = builder.maxInFlight;
        this.maxBatchSize = builder.maxBatchSize;
        this.maxBatchInFlight = builder.maxBatchInFlight;
    }

    public static Builder builder(final long clientId, final String ingressEndpoints) {
        return new Builder(clientId, ingressEndpoints);
    }

    public long clientId() {
        return clientId;
    }

    public String ingressEndpoints() {
        return ingressEndpoints;
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    public String egressChannel() {
        return egressChannel;
    }

    public long messageTimeoutNs() {
        return messageTimeoutNs;
    }

    public long retryBackoffNs() {
        return retryBackoffNs;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public int maxInFlight() {
        return maxInFlight;
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }

    public int maxBatchInFlight() {
        return maxBatchInFlight;
    }

    /** Fluent builder with sensible defaults for local and production use. */
    public static final class Builder {
        private final long clientId;
        private final String ingressEndpoints;
        private String aeronDirectoryName;
        private String egressChannel = "aeron:udp?endpoint=localhost:0";
        private long messageTimeoutNs = TimeUnit.SECONDS.toNanos(30);
        private long retryBackoffNs = TimeUnit.MILLISECONDS.toNanos(250);
        private int maxRetries;
        private int maxInFlight = 1024;
        private int maxBatchSize = 1024;
        private int maxBatchInFlight = 16;

        private Builder(final long clientId, final String ingressEndpoints) {
            this.clientId = clientId;
            this.ingressEndpoints = ingressEndpoints;
        }

        /** Attach to an existing media driver rather than launching an embedded one. */
        public Builder aeronDirectoryName(final String value) {
            this.aeronDirectoryName = value;
            return this;
        }

        /**
         * Egress (result) channel the client binds and advertises to the cluster.
         * Defaults to {@code aeron:udp?endpoint=localhost:0}, which is correct for a
         * client co-located with the cluster. When the client and cluster are on
         * different hosts (for example separate containers), set an endpoint the
         * cluster nodes can route back to, such as this host's address with an
         * ephemeral port.
         */
        public Builder egressChannel(final String value) {
            this.egressChannel = value;
            return this;
        }

        public Builder messageTimeoutNs(final long value) {
            this.messageTimeoutNs = value;
            return this;
        }

        public Builder retryBackoffNs(final long value) {
            this.retryBackoffNs = value;
            return this;
        }

        /** Maximum resend attempts per command; {@code 0} means retry indefinitely. */
        public Builder maxRetries(final int value) {
            this.maxRetries = value;
            return this;
        }

        public Builder maxInFlight(final int value) {
            this.maxInFlight = value;
            return this;
        }

        /** Maximum legs accepted per transfer batch. */
        public Builder maxBatchSize(final int value) {
            this.maxBatchSize = value;
            return this;
        }

        /** Maximum transfer batches in flight at once. */
        public Builder maxBatchInFlight(final int value) {
            this.maxBatchInFlight = value;
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(this);
        }
    }
}
