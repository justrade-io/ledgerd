package io.justrade.ledgerd.read.client;

/** Result of a {@code BALANCE} query: the balance and whether the account exists. */
public record BalanceResult(long accountId, long balance, boolean found, long appliedPosition) {}
