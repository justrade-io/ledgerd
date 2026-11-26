package io.justrade.ledgerd.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.cluster.ConsensusModule;
import org.junit.jupiter.api.Test;

/**
 * Verifies the tunable consensus timeout overrides (M6). Each override maps to a
 * system property; an unset property must leave the Aeron default untouched so
 * that production behaviour is unchanged unless an operator opts in. Lowering
 * these timeouts is the prerequisite for driving failover faster than the
 * default election window, both in production tuning and in fault testing.
 */
final class ClusterNodeTimeoutTest {

    private static final String LEADER_HEARTBEAT_TIMEOUT_PROP = "ledgerd.leaderHeartbeatTimeoutNs";
    private static final String LEADER_HEARTBEAT_INTERVAL_PROP = "ledgerd.leaderHeartbeatIntervalNs";
    private static final String ELECTION_TIMEOUT_PROP = "ledgerd.electionTimeoutNs";
    private static final String STARTUP_CANVASS_TIMEOUT_PROP = "ledgerd.startupCanvassTimeoutNs";

    @Test
    void overridesApplyWhenPropertiesAreSet() {
        clearProps();
        System.setProperty(LEADER_HEARTBEAT_TIMEOUT_PROP, "2000000000");
        System.setProperty(LEADER_HEARTBEAT_INTERVAL_PROP, "200000000");
        System.setProperty(ELECTION_TIMEOUT_PROP, "1500000000");
        System.setProperty(STARTUP_CANVASS_TIMEOUT_PROP, "3000000000");
        try {
            final ConsensusModule.Context context = new ConsensusModule.Context();
            ClusterNode.applyConsensusTimeouts(context);

            assertEquals(2_000_000_000L, context.leaderHeartbeatTimeoutNs());
            assertEquals(200_000_000L, context.leaderHeartbeatIntervalNs());
            assertEquals(1_500_000_000L, context.electionTimeoutNs());
            assertEquals(3_000_000_000L, context.startupCanvassTimeoutNs());
        } finally {
            clearProps();
        }
    }

    @Test
    void unsetPropertiesLeaveAeronDefaults() {
        clearProps();
        try {
            final ConsensusModule.Context reference = new ConsensusModule.Context();
            final ConsensusModule.Context applied = new ConsensusModule.Context();
            ClusterNode.applyConsensusTimeouts(applied);

            assertEquals(reference.leaderHeartbeatTimeoutNs(), applied.leaderHeartbeatTimeoutNs());
            assertEquals(reference.leaderHeartbeatIntervalNs(), applied.leaderHeartbeatIntervalNs());
            assertEquals(reference.electionTimeoutNs(), applied.electionTimeoutNs());
            assertEquals(reference.startupCanvassTimeoutNs(), applied.startupCanvassTimeoutNs());
        } finally {
            clearProps();
        }
    }

    private static void clearProps() {
        System.clearProperty(LEADER_HEARTBEAT_TIMEOUT_PROP);
        System.clearProperty(LEADER_HEARTBEAT_INTERVAL_PROP);
        System.clearProperty(ELECTION_TIMEOUT_PROP);
        System.clearProperty(STARTUP_CANVASS_TIMEOUT_PROP);
    }
}
