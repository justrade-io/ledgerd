package io.justrade.ledgerd.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Verifies the batch dedup sizing guard (H3). The per-client batch dedup ring
 * costs {@code batchDedupWindow * maxBatchSize * BATCH_DEDUP_BYTES_PER_LEG}, so a
 * window sized to {@code maxBatchSize} rather than the in-flight batch count can
 * silently exhaust a small heap. The guard flags that footprint without failing
 * construction, because sizing is guidance rather than a hard limit.
 */
final class CoreConfigSizingTest {

    @Test
    void defaultsAreWithinBudgetAndEmitNoWarning() {
        final CoreConfig config = CoreConfig.defaults();
        assertEquals(
                (long) CoreConfig.DEFAULT_BATCH_DEDUP_WINDOW
                        * CoreConfig.DEFAULT_MAX_BATCH_SIZE
                        * CoreConfig.BATCH_DEDUP_BYTES_PER_LEG,
                config.batchDedupBytesPerClient());
        assertNull(config.batchDedupSizingWarning());
    }

    @Test
    void largeWindowAndBatchSizeTogetherWarn() {
        // maxBatchSize=4096, batchDedupWindow=4096 -> ~160 MiB per client.
        final CoreConfig config = CoreConfig.defaults().withBatch(4096, 4096);
        final String warning = config.batchDedupSizingWarning();
        assertNotNull(warning);
        assertTrue(warning.contains("batchDedupWindow=4096"), warning);
        assertTrue(warning.contains("maxBatchSize=4096"), warning);
    }

    @Test
    void largeBatchSizeWithSmallWindowStaysWithinBudget() {
        // maxBatchSize=4096 but window kept small -> ~40 MiB per client, no warning.
        final CoreConfig config = CoreConfig.defaults().withBatch(4096, 1024);
        assertNull(config.batchDedupSizingWarning());
    }

    @Test
    void footprintCrossingThresholdWarns() {
        final CoreConfig config = CoreConfig.defaults().withBatch(4096, 2048);
        assertTrue(config.batchDedupBytesPerClient() > CoreConfig.BATCH_DEDUP_WARN_BYTES_PER_CLIENT);
        assertNotNull(config.batchDedupSizingWarning());
    }
}
