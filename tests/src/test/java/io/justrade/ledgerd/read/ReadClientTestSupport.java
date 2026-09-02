package io.justrade.ledgerd.read;

import io.justrade.ledgerd.read.client.AllowanceResult;
import io.justrade.ledgerd.read.client.BalanceResult;
import io.justrade.ledgerd.read.client.ReadClient;
import io.justrade.ledgerd.read.client.TotalSupplyResult;
import io.justrade.ledgerd.read.client.config.ReadClientConfig;
import java.io.IOException;
import java.net.DatagramSocket;

/** Shared helpers for tests that drive a read replica through the read-client SDK. */
final class ReadClientTestSupport {

    private ReadClientTestSupport() {}

    /** Allocates a currently-free UDP port for a read replica's query channel. */
    static int freeUdpPort() {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        } catch (final IOException e) {
            throw new IllegalStateException("failed to allocate a free UDP port", e);
        }
    }

    /** A read replica query channel bound to {@code port}. */
    static String queryChannel(final int port) {
        return "aeron:udp?endpoint=localhost:" + port;
    }

    /** A read-client config pointed at the replica's query channel on {@code port}. */
    static ReadClientConfig clientConfig(final int port) {
        return ReadClientConfig.builder().requestChannel(queryChannel(port)).build();
    }

    /** Polls until the replica's total supply for {@code assetId} reaches {@code expected}. */
    static TotalSupplyResult awaitSupply(
            final ReadClient client, final long assetId, final long expected, final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
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
        throw new AssertionError("supply never reached " + expected + ", last=" + last);
    }

    /** Polls until the replica's balance for {@code (assetId, accountId)} reaches {@code expected}. */
    static BalanceResult awaitBalance(
            final ReadClient client,
            final long assetId,
            final long accountId,
            final long expected,
            final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
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
        throw new AssertionError("balance never reached " + expected + ", last=" + last);
    }

    /** Polls until the replica's allowance for {@code (assetId, ownerId, delegateId)} reaches {@code expected}. */
    static AllowanceResult awaitAllowance(
            final ReadClient client,
            final long assetId,
            final long ownerId,
            final long delegateId,
            final long expected,
            final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        AllowanceResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.allowance(assetId, ownerId, delegateId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && last.allowance() == expected) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("allowance never reached " + expected + ", last=" + last);
    }

    /** Polls until the replica reports {@code (assetId, accountId)} as unknown. */
    static BalanceResult awaitMissing(
            final ReadClient client, final long assetId, final long accountId, final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        BalanceResult last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = client.balance(assetId, accountId);
            } catch (final RuntimeException e) {
                // The replica may still be converging; retry on the next poll.
            }
            if (last != null && !last.found()) {
                return last;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("account " + accountId + " never reported missing, last=" + last);
    }
}
