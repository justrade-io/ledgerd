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

    // Guards the RemoteClientExample "risk" demo scenario (ADR 0012): a slow
    // baseline followed by one anomalously fast transaction must flag the account,
    // otherwise the dashboard demo no longer lights up.
    @Test
    void velocityBaselineThenFastEventFlagsAccountLikeTheRiskDemo() {
        final RiskScoringService service = new RiskScoringService();
        final long account = 800L;
        long ts = 1_000L;
        for (int i = 0; i < 8; i++) {
            service.onBalanceChanged(i, ts, 0, 0L, account, 10L, 10L, EventCause.CREDIT);
            if (i < 7) {
                ts += 300L;
            }
        }
        service.onBalanceChanged(100L, ts + 1L, 0, 0L, account, 1L, 1L, EventCause.CREDIT);

        final AccountRisk risk = service.risk(account);
        assertNotNull(risk);
        assertTrue(risk.zScore() >= 4.0, "burst peak yields a high velocity z-score: " + risk.zScore());
        assertTrue(risk.flagged(), "the spiking account is flagged: " + risk.score());
    }

    // Guards the decaying-peak behaviour: a spiking, well-connected account stays
    // flagged on the immediately following (quiet) event even though the
    // instantaneous z-score has decayed, so the dashboard still shows the alert.
    @Test
    void velocityFlagPersistsAfterSpikeDecays() {
        final RiskScoringService service = new RiskScoringService();
        final long account = 800L;
        long ts = 1_000L;
        for (int e = 0; e < 20; e++) {
            service.onTransfer(e, ts, 0, 0L, account, 1_000L + e, 100L);
            ts += 1L;
        }
        for (int i = 0; i < 8; i++) {
            service.onBalanceChanged(100 + i, ts, 0, 0L, account, 10L, 10L, EventCause.CREDIT);
            if (i < 7) {
                ts += 300L;
            }
        }
        service.onBalanceChanged(200L, ts + 1L, 0, 0L, account, 1L, 1L, EventCause.CREDIT);
        service.onBalanceChanged(201L, ts + 2L, 0, 0L, account, 1L, 1L, EventCause.CREDIT);

        final AccountRisk risk = service.risk(account);
        assertNotNull(risk);
        assertTrue(risk.zScore() < 4.0, "the instantaneous z-score has decayed: " + risk.zScore());
        assertTrue(risk.flagged(), "the peak keeps the account flagged: " + risk.score());
    }

    // Guards the demo's money-flow hub: fanning out to many counterparties keeps a
    // persistent high graph centrality that dominates the hub's risk score.
    @Test
    void moneyFlowHubHasHighCentralityLikeTheRiskDemo() {
        final RiskScoringService service = new RiskScoringService();
        final long hub = 900L;
        long ts = 1_000L;
        for (int s = 0; s < 24; s++) {
            service.onTransfer(s, ts, 0, 0L, hub, hub + 1L + s, 100L);
            ts += 1L;
        }
        final AccountRisk risk = service.risk(hub);
        assertNotNull(risk);
        assertTrue(risk.centrality() >= 20.0, "hub centrality exceeds the model reference: " + risk.centrality());
        assertTrue(risk.score() >= 0.4, "graph centrality dominates the hub score: " + risk.score());
    }
}
