package com.adbe.risk.model;

/**
 * Baseline risk model (ADR 0012). It combines a per-account transaction-velocity
 * z-score and a money-flow graph centrality into a single bounded risk score in
 * {@code [0, 1]} using configurable weights and normalising reference scales, and
 * flags an account when the score meets a threshold.
 *
 * <p>This is a deliberately simple heuristic: the point of the PoC is the
 * substrate, not model accuracy. A trained gradient-boosting model is the natural
 * follow-up and slots in behind this same interface.
 */
public final class RiskModel {

    public static final double DEFAULT_VELOCITY_WEIGHT = 0.6;
    public static final double DEFAULT_GRAPH_WEIGHT = 0.4;
    public static final double DEFAULT_ZSCORE_REF = 4.0;
    public static final double DEFAULT_CENTRALITY_REF = 20.0;
    public static final double DEFAULT_FLAG_THRESHOLD = 0.6;

    private final double velocityWeight;
    private final double graphWeight;
    private final double zScoreRef;
    private final double centralityRef;
    private final double flagThreshold;

    public RiskModel() {
        this(
                DEFAULT_VELOCITY_WEIGHT,
                DEFAULT_GRAPH_WEIGHT,
                DEFAULT_ZSCORE_REF,
                DEFAULT_CENTRALITY_REF,
                DEFAULT_FLAG_THRESHOLD);
    }

    public RiskModel(
            final double velocityWeight,
            final double graphWeight,
            final double zScoreRef,
            final double centralityRef,
            final double flagThreshold) {
        if (velocityWeight < 0.0 || graphWeight < 0.0 || velocityWeight + graphWeight <= 0.0) {
            throw new IllegalArgumentException("weights must be non-negative and sum to a positive value");
        }
        if (zScoreRef <= 0.0 || centralityRef <= 0.0) {
            throw new IllegalArgumentException("reference scales must be positive");
        }
        final double sum = velocityWeight + graphWeight;
        this.velocityWeight = velocityWeight / sum;
        this.graphWeight = graphWeight / sum;
        this.zScoreRef = zScoreRef;
        this.centralityRef = centralityRef;
        this.flagThreshold = flagThreshold;
    }

    /**
     * Scores an account. Only a positive z-score (transacting faster than baseline)
     * contributes; a slowdown is not risk.
     */
    public double score(final double zScore, final double centrality) {
        final double velocityComponent = clamp01(Math.max(0.0, zScore) / zScoreRef);
        final double graphComponent = clamp01(centrality / centralityRef);
        return clamp01(velocityWeight * velocityComponent + graphWeight * graphComponent);
    }

    /** Whether {@code score} meets the flag threshold. */
    public boolean isFlagged(final double score) {
        return score >= flagThreshold;
    }

    public double flagThreshold() {
        return flagThreshold;
    }

    private static double clamp01(final double value) {
        if (value < 0.0) {
            return 0.0;
        }
        return Math.min(value, 1.0);
    }
}
