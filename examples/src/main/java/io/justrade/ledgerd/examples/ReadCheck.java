package io.justrade.ledgerd.examples;

import io.justrade.ledgerd.read.client.BalanceResult;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.client.TotalSupplyResult;
import io.justrade.ledgerd.read.client.config.ReadClientConfig;

/**
 * Verifies a remote read replica converges to an expected final state after a
 * {@link LoadGenerator} run: balance(100) = expectedA, balance(200) = expectedB,
 * and totalSupply(asset 0) = expectedSupply. Reads are eventually consistent, so
 * each assertion polls until the value matches or a timeout elapses.
 *
 * <p>Configuration (environment):
 *
 * <pre>
 *   LEDGERD_EXPECT_SUPPLY     expected total supply for asset 0 (required)
 *   LEDGERD_EXPECT_BALANCE_A  expected balance of account 100 (default 0)
 *   LEDGERD_EXPECT_BALANCE_B  expected balance of account 200 (default expectedSupply)
 *   LEDGERD_QUERY_HOST        read replica host (default localhost:44000)
 *   LEDGERD_RESPONSE_HOST     routable host for this client's response channel
 * </pre>
 *
 * <p>Exits non-zero if any assertion times out. Deliberately not part of the
 * deterministic hot path: it uses the system clock for its await loops.
 */
public final class ReadCheck {

    private static final long ACCOUNT_A = 100L;
    private static final long ACCOUNT_B = 200L;
    private static final long READ_TIMEOUT_MS = 60_000L;

    private ReadCheck() {}

    public static void main(final String[] args) {
        final long expectedSupply = requireLong("LEDGERD_EXPECT_SUPPLY");
        final long expectedA = envLong("LEDGERD_EXPECT_BALANCE_A", 0L);
        final long expectedB = envLong("LEDGERD_EXPECT_BALANCE_B", expectedSupply);
        final String queryHost = System.getenv("LEDGERD_QUERY_HOST");
        final String responseHost = System.getenv("LEDGERD_RESPONSE_HOST");

        final ReadClientConfig.Builder builder = ReadClientConfig.builder();
        if (queryHost != null && !queryHost.isBlank()) {
            builder.requestChannel("aeron:udp?endpoint=" + queryHost + ":44000");
        }
        if (responseHost != null && !responseHost.isBlank()) {
            builder.responseChannel("aeron:udp?endpoint=" + responseHost + ":0");
        }

        try (ReadClient client = new ReadClient(builder.build())) {
            final BalanceResult a = awaitBalance(client, 0L, ACCOUNT_A, expectedA);
            final BalanceResult b = awaitBalance(client, 0L, ACCOUNT_B, expectedB);
            final TotalSupplyResult supply = awaitSupply(client, 0L, expectedSupply);

            System.out.printf(
                    "readcheck: balance(%d)=%d balance(%d)=%d supply=%d appliedPosition=%d submitted=%d completed=%d%n",
                    ACCOUNT_A,
                    a.balance(),
                    ACCOUNT_B,
                    b.balance(),
                    supply.totalSupply(),
                    supply.appliedPosition(),
                    client.submitted(),
                    client.completed());
        }
    }

    private static BalanceResult awaitBalance(
            final ReadClient client, final long assetId, final long accountId, final long expected) {
        final long deadline = System.currentTimeMillis() + READ_TIMEOUT_MS;
        BalanceResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.balance(assetId, accountId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && last.found() && last.balance() == expected) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("balance(asset=" + assetId + ", account=" + accountId + ") never reached "
                + expected + ", last=" + last);
    }

    private static TotalSupplyResult awaitSupply(final ReadClient client, final long assetId, final long expected) {
        final long deadline = System.currentTimeMillis() + READ_TIMEOUT_MS;
        TotalSupplyResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.totalSupply(assetId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && last.totalSupply() == expected) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("supply(asset=" + assetId + ") never reached " + expected + ", last=" + last);
    }

    private static long requireLong(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return Long.parseLong(value.trim());
    }

    private static long envLong(final String name, final long fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value.trim());
    }
}
