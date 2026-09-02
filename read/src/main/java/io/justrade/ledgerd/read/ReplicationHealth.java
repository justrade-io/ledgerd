package io.justrade.ledgerd.read;

/**
 * Replication health tracked by the read replica's single agent thread and read
 * by {@link ReadReplicaNode#isHealthy()} (and any future external probe).
 * Volatile fields give the cross-thread visibility that boundary needs without
 * locking; the agent thread is the sole writer.
 *
 * <p>Health is {@code ok} while the replica is connected to an Archive and
 * following, and {@code stale} while it is reconnecting after a source failure.
 * ADR 0008 exposes this so an orchestrator or load balancer can detect a
 * degraded replica instead of the node always reporting ok.
 */
public final class ReplicationHealth {

    private volatile boolean healthy;
    private volatile long appliedPosition;
    private volatile String activeEndpoint = "";
    private volatile long failovers;
    private volatile long integrityFailures;

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

    /** Notes that a replayed snapshot failed its integrity check and was discarded. */
    public void recordIntegrityFailure() {
        this.integrityFailures++;
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

    public long integrityFailures() {
        return integrityFailures;
    }
}
