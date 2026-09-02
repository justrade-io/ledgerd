package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.InMemorySnapshot;
import io.justrade.ledgerd.testkit.WorkloadGenerator;
import org.junit.jupiter.api.Test;

class SnapshotRoundTripTest {

    private static CoreConfig config() {
        return CoreConfig.of(1024, 64, 8, 64, 16);
    }

    @Test
    void writeThenLoadReproducesByteIdenticalStateAndInvariant() {
        final BalanceEngine source = new BalanceEngine(config(), new CoreMetrics());
        WorkloadGenerator.apply(source, 12345L, 2000);

        final SnapshotManager writeManager = new SnapshotManager();
        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(writeManager, source, 987L);

        final BalanceEngine restored = new BalanceEngine(config(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);

        assertTrue(readManager.loadComplete(), "footer must be applied");
        assertTrue(readManager.verifyInvariant(), "sum(balances) must equal totalSupply");

        // Re-serialising the restored engine must produce identical bytes.
        final InMemorySnapshot reSerialized = new InMemorySnapshot();
        reSerialized.writeFrom(new SnapshotManager(), restored, 987L);
        assertArrayEquals(snapshot.toByteArray(), reSerialized.toByteArray());
    }
}
