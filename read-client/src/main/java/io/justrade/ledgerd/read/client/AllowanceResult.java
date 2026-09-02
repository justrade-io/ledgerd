package io.justrade.ledgerd.read.client;

/** Result of an {@code ALLOWANCE} query for an (owner, delegate) pair. */
public record AllowanceResult(long ownerId, long delegateId, long allowance, long appliedPosition) {}
