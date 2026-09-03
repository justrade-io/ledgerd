package io.justrade.ledgerd.write.client;

import io.justrade.ledgerd.protocol.StatusCode;

/** Result of one leg of a {@code TransferBatch}. */
public record TransferLegResult(StatusCode status, boolean hasBalance, long resultBalance) {}
