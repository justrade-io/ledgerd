# write-client

The write-side SDK: submit commands to the cluster and receive deterministic
results. It depends only on `protocol` (no `core`), so it is a thin, embeddable
client.

## Responsibility

- Submit `CommandEnvelope` messages to the cluster ingress.
- Submit `TransferBatch` messages (many transfer legs, with linked atomic
  chains) and deliver `TransferBatchResult` messages (ADR 0012).
- Handle leader changes and reconnects transparently.
- Provide idempotent retry via `(clientId, commandId)` correlation, so a retry
  after a timeout or failover cannot double-apply.
- Deliver `CommandResult` messages with explicit backpressure signalling.

## Key classes

- [WriteClient.java](src/main/java/io/justrade/ledgerd/write/client/WriteClient.java) -
  the client: connect, submit, poll.
- [ResultHandler.java](src/main/java/io/justrade/ledgerd/write/client/ResultHandler.java) -
  callback for command results.
- [BatchResultHandler.java](src/main/java/io/justrade/ledgerd/write/client/BatchResultHandler.java) -
  callback for transfer-batch results.
- [TransferLeg.java](src/main/java/io/justrade/ledgerd/write/client/TransferLeg.java) -
  one leg of a transfer batch (from, to, amount, asset, linked).
- [TransferLegResult.java](src/main/java/io/justrade/ledgerd/write/client/TransferLegResult.java) -
  result of one transfer leg.
- [PendingCommand.java](src/main/java/io/justrade/ledgerd/write/client/PendingCommand.java) -
  in-flight command tracking for correlation and retry.
- [PendingBatchCommand.java](src/main/java/io/justrade/ledgerd/write/client/PendingBatchCommand.java) -
  in-flight batch tracking for correlation and retry.
- [config/ClientConfig.java](src/main/java/io/justrade/ledgerd/write/client/config/ClientConfig.java) -
  immutable client configuration.

## Usage shape

Submit a command and poll for results in a closed loop:

```java
try (WriteClient client = new WriteClient(config, handler)) {
    long commandId = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
    while (running) {
        client.poll();   // drives result callbacks, correlated by commandId
    }
}
```

Submit a transfer batch with an atomic linked chain (ADR 0012):

```java
client.setBatchResultHandler((batchIdHi, batchIdLo, results) -> { /* per-leg results */ });
TransferLeg[] legs = {
    new TransferLeg(100L, 200L, 50L, 0L, true),   // linked to the next leg
    new TransferLeg(300L, 400L, 25L, 0L, false),  // last leg of the chain
};
long batchIdLo = client.submitTransferBatch(legs);
```

See [../examples/](../examples/) for a runnable end-to-end use and
[../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for the retry and correlation
model.

## Related

- Exactly-once semantics: [../docs/decisions/](../docs/decisions/).
- Wire format: [../protocol/README.md](../protocol/README.md).
