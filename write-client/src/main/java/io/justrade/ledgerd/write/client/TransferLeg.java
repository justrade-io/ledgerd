package io.justrade.ledgerd.write.client;

/** One leg of a {@code TransferBatch}: a single transfer with a linked flag. */
public record TransferLeg(long fromId, long toId, long amount, long assetId, boolean linked) {}
