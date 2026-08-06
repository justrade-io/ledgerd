package com.adbe.risk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adbe.protocol.EventCause;
import com.adbe.risk.RiskScoringService.AccountRisk;
import java.util.List;
import org.junit.jupiter.api.Test;

class RiskScoringServiceTest {

    @Test
    void balanceAndTransferEventsDriveScoresAndGraph() {
        final RiskScoringService service = new RiskScoringService();

        // A credit to account 1, then a transfer 1 -> 2.
        service.onBalanceChanged(10L, 1_000L, 0, 0L, 1L, 100L, 100L, EventCause.CREDIT);
        service.onBalanceChanged(11L, 1_000L, 0, 0L, 1L, -50L, -50L, EventCause.TRANSFER_DEBIT);
        service.onBalanceChanged(11L, 1_000L, 1, 0L, 2L, 50L, 50L, EventCause.TRANSFER_CREDIT);
        service.onTransfer(11L, 1_000L, 2, 0L, 1L, 2L, 50L);

        assertEquals(4L, service.eventsProcessed());
        assertEquals(1L, service.transfers());
        assertEquals(3L, service.balanceChanges());
        assertEquals(1L, service.graph().edgeCount());
        assertEquals(2, service.scoredAccounts());

        final AccountRisk risk1 = service.risk(1L);
        assertNotNull(risk1);
        assertTrue(risk1.centrality() >= 1.0, "account 1 has a graph edge");
    }

    @Test
    void topScoresIsSortedDescendingAndBounded() {
        final RiskScoringService service = new RiskScoringService();
        for (long account = 1L; account <= 5L; account++) {
            service.onBalanceChanged(account, account * 100L, 0, 0L, account, 10L, 10L, EventCause.CREDIT);
        }
        final List<AccountRisk> top = service.topScores(3);
        assertEquals(3, top.size(), "limit is honoured");
        for (int i = 1; i < top.size(); i++) {
            assertTrue(top.get(i - 1).score() >= top.get(i).score(), "descending by score");
        }
    }
}
