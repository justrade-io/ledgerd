package io.justrade.ledgerd.risk;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.read.journal.DomainEventListener;
import io.justrade.ledgerd.risk.feature.TransferGraph;
import io.justrade.ledgerd.risk.feature.VelocityTracker;
import io.justrade.ledgerd.risk.model.RiskModel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns the domain event stream (ADR 0011) into live per-account risk scores
 * (ADR 0012). It is a {@link DomainEventListener}, so the event follower delivers
 * every event on one agent thread and this service owns all feature state
 * single-threaded with no locks. Each processed event updates the velocity and
 * graph features for the touched accounts and republishes an immutable
 * {@link AccountRisk} snapshot that the HTTP dashboard reads concurrently.
 */
public final class RiskScoringService implements DomainEventListener {

    // Half-life for the published peak score decay (see peakScore).
    private static final double PEAK_HALF_LIFE_MS = 30_000.0;

    private final VelocityTracker velocity;
    private final TransferGraph graph;
    private final RiskModel model;
    private final ConcurrentHashMap<Long, AccountRisk> risks = new ConcurrentHashMap<>();

    private volatile long eventsProcessed;
    private volatile long balanceChanges;
    private volatile long transfers;
    private volatile long holds;
    private volatile long allowanceChanges;
    private volatile long rejects;

    public RiskScoringService() {
        this(new VelocityTracker(), new TransferGraph(), new RiskModel());
    }

    public RiskScoringService(final VelocityTracker velocity, final TransferGraph graph, final RiskModel model) {
        this.velocity = velocity;
        this.graph = graph;
        this.model = model;
    }

    @Override
    public void onBalanceChanged(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long accountId,
            final long newBalance,
            final long delta,
            final EventCause cause) {
        balanceChanges++;
        eventsProcessed++;
        velocity.record(accountId, timestamp);
        rescore(accountId, timestamp);
    }

    @Override
    public void onTransfer(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long fromAccount,
            final long toAccount,
            final long amount) {
        transfers++;
        eventsProcessed++;
        graph.addEdge(fromAccount, toAccount, amount);
        velocity.record(fromAccount, timestamp);
        velocity.record(toAccount, timestamp);
        rescore(fromAccount, timestamp);
        rescore(toAccount, timestamp);
    }

    @Override
    public void onReserved(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long accountId,
            final long newAvailable,
            final long newReserved) {
        recordHold(accountId, timestamp);
    }

    @Override
    public void onCaptured(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long accountId,
            final long newAvailable,
            final long newReserved) {
        recordHold(accountId, timestamp);
    }

    @Override
    public void onReleased(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long accountId,
            final long newAvailable,
            final long newReserved) {
        recordHold(accountId, timestamp);
    }

    @Override
    public void onAllowanceChanged(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long ownerId,
            final long delegateId,
            final long newAllowance) {
        allowanceChanges++;
        eventsProcessed++;
        velocity.record(ownerId, timestamp);
        rescore(ownerId, timestamp);
    }

    @Override
    public void onCommandRejected(
            final long logPosition,
            final long timestamp,
            final int eventIndex,
            final long assetId,
            final long accountId,
            final long amount,
            final CommandType commandType,
            final StatusCode reason) {
        rejects++;
        eventsProcessed++;
    }

    private void recordHold(final long accountId, final long timestamp) {
        holds++;
        eventsProcessed++;
        velocity.record(accountId, timestamp);
        rescore(accountId, timestamp);
    }

    private void rescore(final long accountId, final long timestamp) {
        final double zScore = velocity.zScore(accountId);
        final double centrality = graph.degreeCentrality(accountId);
        final double instant = model.score(zScore, centrality);
        final AccountRisk previous = risks.get(accountId);
        final long txCount = (previous == null ? 0L : previous.txCount()) + 1L;
        final double score = peakScore(instant, previous, timestamp);
        risks.put(
                accountId,
                new AccountRisk(accountId, score, zScore, centrality, txCount, timestamp, model.isFlagged(score)));
    }

    // A velocity anomaly is instantaneous - the EWMA absorbs it within a couple of
    // events - so the published score is a decaying peak: it fades toward the
    // instantaneous score over a half-life instead of vanishing on the next event,
    // keeping a spike visible on the dashboard (ADR 0012, PoC).
    private static double peakScore(final double instant, final AccountRisk previous, final long timestamp) {
        if (previous == null) {
            return instant;
        }
        final long dtMs = Math.max(0L, timestamp - previous.lastTimestamp());
        final double decayed = previous.score() * Math.pow(2.0, -dtMs / PEAK_HALF_LIFE_MS);
        return Math.max(instant, decayed);
    }

    /** The current risk snapshot for {@code accountId}, or {@code null} if unseen. */
    public AccountRisk risk(final long accountId) {
        return risks.get(accountId);
    }

    /** The {@code limit} highest-scoring accounts, descending by score. */
    public List<AccountRisk> topScores(final int limit) {
        final List<AccountRisk> all = new ArrayList<>(risks.values());
        all.sort(Comparator.comparingDouble(AccountRisk::score).reversed().thenComparingLong(AccountRisk::account));
        if (all.size() > limit) {
            return all.subList(0, limit);
        }
        return all;
    }

    /** The underlying money-flow graph (for snapshot / PageRank on the HTTP thread). */
    public TransferGraph graph() {
        return graph;
    }

    public long eventsProcessed() {
        return eventsProcessed;
    }

    public long balanceChanges() {
        return balanceChanges;
    }

    public long transfers() {
        return transfers;
    }

    public long holds() {
        return holds;
    }

    public long allowanceChanges() {
        return allowanceChanges;
    }

    public long rejects() {
        return rejects;
    }

    public int scoredAccounts() {
        return risks.size();
    }

    /** Immutable published risk snapshot for one account. */
    public record AccountRisk(
            long account,
            double score,
            double zScore,
            double centrality,
            long txCount,
            long lastTimestamp,
            boolean flagged) {}
}
