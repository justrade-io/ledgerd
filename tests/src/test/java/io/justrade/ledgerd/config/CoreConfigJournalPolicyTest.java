package io.justrade.ledgerd.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.justrade.ledgerd.config.CoreConfig.JournalOverflowPolicy;
import org.junit.jupiter.api.Test;

/**
 * Verifies the event-journal overflow policy plumbing (H1, ADR 0014). The default
 * must stay {@code DROP} so upgrading a deployment does not change journal
 * semantics, and the policy must survive the other copy-with methods unchanged.
 */
final class CoreConfigJournalPolicyTest {

    @Test
    void defaultPolicyIsDrop() {
        assertEquals(JournalOverflowPolicy.DROP, CoreConfig.defaults().journalOverflowPolicy());
    }

    @Test
    void withJournalOverflowPolicySelectsBlock() {
        final CoreConfig config = CoreConfig.defaults().withJournalOverflowPolicy(JournalOverflowPolicy.BLOCK);
        assertEquals(JournalOverflowPolicy.BLOCK, config.journalOverflowPolicy());
    }

    @Test
    void policyIsPreservedAcrossOtherCopyMethods() {
        final CoreConfig config = CoreConfig.defaults()
                .withJournalOverflowPolicy(JournalOverflowPolicy.BLOCK)
                .withEventJournal(CoreConfig.DEFAULT_EVENT_JOURNAL_CAPACITY)
                .withBatch(1024, 1024)
                .withCapacities(1 << 16, 1 << 12, 1 << 12);
        assertEquals(JournalOverflowPolicy.BLOCK, config.journalOverflowPolicy());
    }
}
