package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import io.justrade.ledgerd.testkit.InMemorySnapshot;
import io.justrade.ledgerd.testkit.WorkloadGenerator;
import org.junit.jupiter.api.Test;

class ReplayDeterminismTest {

    private static CoreConfig config() {
        return CoreConfig.of(1024, 64, 8, 64, 16);
    }

    @Test
    void independentEnginesReplayingSameLogProduceIdenticalState() {
        final BalanceEngine first = new BalanceEngine(config(), new CoreMetrics());
        final BalanceEngine second = new BalanceEngine(config(), new CoreMetrics());

        WorkloadGenerator.apply(first, 0xC0FFEEL, 5000);
        WorkloadGenerator.apply(second, 0xC0FFEEL, 5000);

        final InMemorySnapshot snapshotFirst = new InMemorySnapshot();
        final InMemorySnapshot snapshotSecond = new InMemorySnapshot();
        snapshotFirst.writeFrom(new SnapshotManager(), first, 1L);
        snapshotSecond.writeFrom(new SnapshotManager(), second, 1L);

        assertArrayEquals(snapshotFirst.toByteArray(), snapshotSecond.toByteArray());
    }
}
