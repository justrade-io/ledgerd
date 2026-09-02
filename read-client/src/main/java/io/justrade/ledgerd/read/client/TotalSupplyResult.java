package io.justrade.ledgerd.read.client;

/** Result of a {@code TOTAL_SUPPLY} query: the engine-wide total supply for an asset. */
public record TotalSupplyResult(long assetId, long totalSupply, long appliedPosition) {}
