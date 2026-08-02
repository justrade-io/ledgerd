package com.adbe.read;

/**
 * Replication health published by the read replica's single agent thread and
 * read by the HTTP (Netty) threads serving {@code /healthz} and {@code /metrics}.
 * Volatile fields give the cross-thread visibility this boundary needs without
 * locking; the agent thread is the sole writer.
 *
 * <p>Health is {@code ok} while the replica is connected to an Archive and
 * following, and {@code stale} while it is reconnecting after a source failure.
 * ADR 0008 exposes this so an orchestrator or load balancer can detect a
 * degraded replica instead of the endpoint always reporting ok.
 */
public final class ReplicationHealth {

    private volatile boolean healthy;
    private volatile long appliedPosition;
    private volatile String activeEndpoint = "";
    private volatile long failovers;

    /** Records a successful connect / steady following cycle. */
    public void markHealthy(final String endpoint, final long appliedLogPosition) {
        this.activeEndpoint = endpoint;
        this.appliedPosition = appliedLogPosition;
        this.healthy = true;
    }

    /** Records a lost source; the replica keeps serving its last state. */
    public void markStale(final String endpoint, final long appliedLogPosition) {
        this.activeEndpoint = endpoint;
        this.appliedPosition = appliedLogPosition;
        this.healthy = false;
    }

    /** Notes that the replica switched to a new Archive endpoint. */
    public void recordFailover() {
        this.failovers++;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public long appliedPosition() {
        return appliedPosition;
    }

    public String activeEndpoint() {
        return activeEndpoint;
    }

    public long failovers() {
        return failovers;
    }
}
