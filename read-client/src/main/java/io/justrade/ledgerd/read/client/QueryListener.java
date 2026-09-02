package io.justrade.ledgerd.read.client;

import io.justrade.ledgerd.protocol.QueryStatusCode;
import io.justrade.ledgerd.protocol.QueryType;
import java.util.List;

/**
 * Callback sink for asynchronous query delivery. Register with
 * {@link ReadClient#setListener(QueryListener)}; every callback runs on the
 * thread that calls {@link ReadClient#poll()}, matching how {@code WriteClient}
 * delivers egress events.
 *
 * <p>Each callback carries the {@code requestId} returned by the matching
 * {@code submit...} call for correlation. Only the callback relevant to the
 * submitted query type fires; {@code onTimeout} and {@code onError} fire for
 * every query type.
 */
public interface QueryListener {

    /** No-op listener; the default until one is registered. */
    QueryListener NONE = new QueryListener() {};

    default void onBalance(final long requestId, final BalanceResult result) {}

    default void onBatchBalances(final long requestId, final List<BalanceResult> results) {}

    default void onAllowance(final long requestId, final AllowanceResult result) {}

    default void onTotalSupply(final long requestId, final TotalSupplyResult result) {}

    /** The query exhausted its retry budget without a response. */
    default void onTimeout(final long requestId, final QueryType type) {}

    /** The read service rejected the query (e.g. unsupported query type). */
    default void onError(final long requestId, final QueryType type, final QueryStatusCode status) {}
}
