package io.justrade.ledgerd.risk;

/**
 * Immutable configuration for the risk service (ADR 0012): the HTTP dashboard
 * port and the number of top-scoring rows the dashboard endpoints expose.
 */
public final class RiskServiceConfig {

    public static final int DEFAULT_HTTP_PORT = 8090;
    public static final int DEFAULT_MAX_SCORE_ROWS = 100;
    public static final int DEFAULT_MAX_GRAPH_EDGES = 500;

    private final int httpPort;
    private final int maxScoreRows;
    private final int maxGraphEdges;

    private RiskServiceConfig(final Builder builder) {
        this.httpPort = builder.httpPort;
        this.maxScoreRows = builder.maxScoreRows;
        this.maxGraphEdges = builder.maxGraphEdges;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RiskServiceConfig defaults() {
        return builder().build();
    }

    public int httpPort() {
        return httpPort;
    }

    public int maxScoreRows() {
        return maxScoreRows;
    }

    public int maxGraphEdges() {
        return maxGraphEdges;
    }

    /** Fluent builder with defaults suitable for local runs and tests. */
    public static final class Builder {
        private int httpPort = DEFAULT_HTTP_PORT;
        private int maxScoreRows = DEFAULT_MAX_SCORE_ROWS;
        private int maxGraphEdges = DEFAULT_MAX_GRAPH_EDGES;

        private Builder() {}

        public Builder httpPort(final int value) {
            this.httpPort = value;
            return this;
        }

        public Builder maxScoreRows(final int value) {
            if (value < 1) {
                throw new IllegalArgumentException("maxScoreRows must be >= 1");
            }
            this.maxScoreRows = value;
            return this;
        }

        public Builder maxGraphEdges(final int value) {
            if (value < 1) {
                throw new IllegalArgumentException("maxGraphEdges must be >= 1");
            }
            this.maxGraphEdges = value;
            return this;
        }

        public RiskServiceConfig build() {
            return new RiskServiceConfig(this);
        }
    }
}
