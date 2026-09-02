package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.InMemorySnapshot;
import io.justrade.ledgerd.testkit.WorkloadGenerator;
import org.junit.jupiter.api.Test;

/**
 * Guards the snapshot integrity check that {@code BalanceService.loadSnapshot}
 * and {@code ReadReplicaNode} now enforce: a truncated or byte-corrupted snapshot
 * must fail {@link SnapshotManager#verifyInvariant()} so it is never served.
 */
class SnapshotIntegrityTest {

    private static CoreConfig config() {
        return CoreConfig.of(1024, 64, 8, 64, 16);
    }

    @Test
    void truncatedSnapshotMissingFooterFailsIntegrityCheck() {
        final BalanceEngine source = new BalanceEngine(config(), new CoreMetrics());
        WorkloadGenerator.apply(source, 4242L, 500);

        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), source, 1L);

        final BalanceEngine restored = new BalanceEngine(config(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readIntoDroppingFooter(readManager, restored);

        assertFalse(readManager.loadComplete(), "footer must be absent");
        assertFalse(readManager.verifyInvariant(), "truncated snapshot must be rejected");
    }

    @Test
    void corruptedBalanceValueFailsIntegrityCheck() {
        final BalanceEngine source = new BalanceEngine(config(), new CoreMetrics());
        WorkloadGenerator.apply(source, 777L, 500);

        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), source, 1L);
        snapshot.corruptFirstBalanceValue();

        final BalanceEngine restored = new BalanceEngine(config(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);

        assertTrue(readManager.loadComplete(), "footer still applied");
        assertFalse(readManager.verifyInvariant(), "sum(balances) != totalSupply must be detected");
    }
}
